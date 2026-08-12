package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/dominicnunez/agentos-a2a-go/executionkind"
	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestOfficialA2AClientUsesDurableAgentOSState(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	actor := testExternalActor("official-client", "org-1", testExternalToken, ExternalRoleOperator, intake.WorkScopeOwn)
	client, closeServer := officialA2AClient(t, operator, actor)

	deterministic := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("echo official"))
	deterministic.ID = "official-message-1"
	if err := executionkind.Set(deterministic, executionkind.KindDeterministic); err != nil {
		t.Fatal(err)
	}
	result, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: deterministic})
	if err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok || task.ID == "" || task.ContextID == "" || task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("SendMessage() = %#v", result)
	}
	task = confirmOfficialIntent(t, client, task, "official-confirmation-1")
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("confirmed task = %#v", task)
	}
	if deterministic.ContextID != "" {
		t.Fatalf("client message was unexpectedly mutated: contextId=%q", deterministic.ContextID)
	}

	retry := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("echo official"))
	retry.ID = deterministic.ID
	if err := executionkind.Set(retry, executionkind.KindDeterministic); err != nil {
		t.Fatal(err)
	}
	retryResult, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: retry})
	if err != nil {
		t.Fatalf("initial retry error = %v", err)
	}
	retryTask, ok := retryResult.(*a2a.Task)
	if !ok || retryTask.ID != task.ID || retryTask.ContextID != task.ContextID {
		t.Fatalf("retry = %#v, want task=%s context=%s", retryResult, task.ID, task.ContextID)
	}

	loaded, err := client.GetTask(context.Background(), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || loaded.ID != task.ID || loaded.ContextID != task.ContextID || len(loaded.Artifacts) != 1 || loaded.Artifacts[0].Parts[0].Text() != "official" {
		t.Fatalf("GetTask() = %#v, %v", loaded, err)
	}

	eventsBefore := len(gatewayExternalStream(t, store, task.ContextID))
	conflict := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("different work"))
	conflict.ID = "official-message-conflict"
	conflict.ContextID = task.ContextID
	if _, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: conflict}); err == nil {
		t.Fatal("different message in an existing context without taskId was accepted")
	}
	if got := len(gatewayExternalStream(t, store, task.ContextID)); got != eventsBefore {
		t.Fatalf("rejected context reuse appended events: got=%d want=%d", got, eventsBefore)
	}

	blockedMessage := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("needs human input"))
	blockedMessage.ID = "official-message-blocked"
	if err := executionkind.Set(blockedMessage, executionkind.KindHuman); err != nil {
		t.Fatal(err)
	}
	blockedResult, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: blockedMessage})
	if err != nil {
		t.Fatal(err)
	}
	blocked, ok := blockedResult.(*a2a.Task)
	if !ok || blocked.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("blocked result = %#v", blockedResult)
	}
	blocked = confirmOfficialIntent(t, client, blocked, "official-confirmation-blocked")
	if blocked.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("confirmed blocked result = %#v", blocked)
	}
	mismatch := a2a.NewMessageForTask(a2a.MessageRoleUser, blocked, a2a.NewTextPart("detail"))
	mismatch.ID = "official-message-mismatch"
	mismatch.ContextID = "wrong-context"
	if _, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: mismatch}); err == nil {
		t.Fatal("continuation with a mismatched contextId was accepted")
	}
	continuation := a2a.NewMessageForTask(a2a.MessageRoleUser, blocked, a2a.NewTextPart("detail"))
	continuation.ID = "official-message-continuation"
	continuedResult, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: continuation})
	if err != nil {
		t.Fatal(err)
	}
	continued, ok := continuedResult.(*a2a.Task)
	if !ok || continued.ID != blocked.ID || continued.ContextID != blocked.ContextID || continued.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("continued result = %#v", continuedResult)
	}

	agentMessage := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("draft an update"))
	agentMessage.ID = "official-message-agent"
	if err := executionkind.Set(agentMessage, executionkind.KindAgent); err != nil {
		t.Fatal(err)
	}
	agentResult, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: agentMessage})
	if err != nil {
		t.Fatal(err)
	}
	agentTask, ok := agentResult.(*a2a.Task)
	if !ok || agentTask.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("agent result = %#v", agentResult)
	}
	agentTask = confirmOfficialIntent(t, client, agentTask, "official-confirmation-agent")
	if agentTask.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("confirmed agent result = %#v", agentTask)
	}

	allBefore, err := store.Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTasks(context.Background(), &a2a.ListTasksRequest{}); err == nil {
		t.Fatal("unsupported ListTasks succeeded")
	}
	allAfter, err := store.Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(allAfter) != len(allBefore) {
		t.Fatalf("unsupported method created ledger events: got=%d want=%d", len(allAfter), len(allBefore))
	}

	closeServer()
	restartedClient, closeRestarted := officialA2AClient(t, intake.New(app.New(events.NewGateway(store))), actor)
	defer closeRestarted()
	restarted, err := restartedClient.GetTask(context.Background(), &a2a.GetTaskRequest{ID: task.ID})
	if err != nil || restarted.ID != task.ID || restarted.ContextID != task.ContextID || restarted.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("GetTask after gateway restart = %#v, %v", restarted, err)
	}
}

func TestOfficialA2ASubmitterCanRetryOnlyItsSubmissionReceipt(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := intake.New(app.New(events.NewGateway(store)))
	actor := testExternalActor("submit-only", "org-1", testExternalToken, ExternalRoleSubmitter, intake.WorkScopeOwn)
	client, closeServer := officialA2AClient(t, service, actor)
	defer closeServer()

	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("echo private"))
	message.ID = "submit-only-message"
	first, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: message})
	if err != nil {
		t.Fatal(err)
	}
	receipt, ok := first.(*a2a.Task)
	if !ok || receipt.ID == "" || receipt.ContextID == "" || receipt.Status.State != a2a.TaskStateInputRequired || len(receipt.Artifacts) != 0 {
		t.Fatalf("submission receipt = %#v", first)
	}
	eventCount := len(gatewayExternalStream(t, store, receipt.ContextID))

	retry := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("echo private"))
	retry.ID = message.ID
	replayed, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: retry})
	if err != nil {
		t.Fatalf("retry error = %v", err)
	}
	replayedReceipt, ok := replayed.(*a2a.Task)
	if !ok || replayedReceipt.ID != receipt.ID || replayedReceipt.ContextID != receipt.ContextID || replayedReceipt.Status.State != receipt.Status.State || len(replayedReceipt.Artifacts) != 0 {
		t.Fatalf("replayed receipt = %#v, want task=%s context=%s", replayed, receipt.ID, receipt.ContextID)
	}
	if got := len(gatewayExternalStream(t, store, receipt.ContextID)); got != eventCount {
		t.Fatalf("receipt retry appended events: got=%d want=%d", got, eventCount)
	}
	confirmed := confirmOfficialIntent(t, client, receipt, "submit-only-confirmation")
	if confirmed.Status.State != a2a.TaskStateWorking || len(confirmed.Artifacts) != 0 {
		t.Fatalf("submitter confirmation receipt = %#v", confirmed)
	}
	if _, err := client.GetTask(context.Background(), &a2a.GetTaskRequest{ID: receipt.ID}); err == nil {
		t.Fatal("submit-only actor gained task status access")
	}
}

func confirmOfficialIntent(t *testing.T, client *a2aclient.Client, task *a2a.Task, messageID string) *a2a.Task {
	t.Helper()
	metadata, ok := task.Metadata[intentConfirmationURI].(map[string]any)
	if !ok {
		t.Fatalf("task has no intent metadata: %#v", task)
	}
	fingerprint, ok := metadata["fingerprint"].(string)
	if !ok || fingerprint == "" {
		t.Fatalf("task has no intent fingerprint: %#v", task)
	}
	message := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.NewTextPart("Confirm the reviewed Agent OS intent."))
	message.ID = messageID
	message.Extensions = []string{intentConfirmationURI}
	message.Metadata = map[string]any{intentConfirmationURI: map[string]any{"action": "CONFIRM", "fingerprint": fingerprint}}
	result, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: message})
	if err != nil {
		t.Fatalf("intent confirmation error = %v", err)
	}
	confirmed, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("intent confirmation = %#v", result)
	}
	return confirmed
}

func TestUnsupportedA2AHandlerMethodsDoNotReachLedger(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := &a2aRequestHandler{service: intake.New(app.New(events.NewGateway(store)))}
	assertError := func(name string, err, want error) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Fatalf("%s error = %v, want errors.Is(%v)", name, err, want)
		}
	}

	_, err = handler.ListTasks(context.Background(), &a2a.ListTasksRequest{})
	assertError("ListTasks", err, a2a.ErrUnsupportedOperation)
	_, err = handler.CancelTask(context.Background(), &a2a.CancelTaskRequest{})
	assertError("CancelTask", err, a2a.ErrUnsupportedOperation)
	for name, stream := range map[string]func() error{
		"SendStreamingMessage": func() error {
			for _, streamErr := range handler.SendStreamingMessage(context.Background(), &a2a.SendMessageRequest{}) {
				return streamErr
			}
			return nil
		},
		"SubscribeToTask": func() error {
			for _, streamErr := range handler.SubscribeToTask(context.Background(), &a2a.SubscribeToTaskRequest{}) {
				return streamErr
			}
			return nil
		},
	} {
		assertError(name, stream(), a2a.ErrUnsupportedOperation)
	}
	_, err = handler.GetTaskPushConfig(context.Background(), &a2a.GetTaskPushConfigRequest{})
	assertError("GetTaskPushConfig", err, a2a.ErrPushNotificationNotSupported)
	_, err = handler.ListTaskPushConfigs(context.Background(), &a2a.ListTaskPushConfigRequest{})
	assertError("ListTaskPushConfigs", err, a2a.ErrPushNotificationNotSupported)
	_, err = handler.CreateTaskPushConfig(context.Background(), &a2a.PushConfig{})
	assertError("CreateTaskPushConfig", err, a2a.ErrPushNotificationNotSupported)
	err = handler.DeleteTaskPushConfig(context.Background(), &a2a.DeleteTaskPushConfigRequest{})
	assertError("DeleteTaskPushConfig", err, a2a.ErrPushNotificationNotSupported)
	_, err = handler.GetExtendedAgentCard(context.Background(), &a2a.GetExtendedAgentCardRequest{})
	assertError("GetExtendedAgentCard", err, a2a.ErrExtendedCardNotConfigured)

	all, err := store.Events(context.Background(), "")
	if err != nil || len(all) != 0 {
		t.Fatalf("unsupported handler method reached ledger: events=%d err=%v", len(all), err)
	}
}

func officialA2AClient(t *testing.T, service *intake.Service, actor ExternalActor) (*a2aclient.Client, func()) {
	t.Helper()
	handler := testA2A(t, service, actor)
	server := httptest.NewServer(handler)
	card, err := agentcard.DefaultResolver.Resolve(context.Background(), server.URL)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	origin, err := url.Parse(server.URL)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	httpClient := &http.Client{Transport: testBearerTransport{origin: origin, token: actor.BearerToken, base: http.DefaultTransport}}
	client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithJSONRPCTransport(httpClient))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return client, server.Close
}

type testBearerTransport struct {
	origin *url.URL
	token  string
	base   http.RoundTripper
}

func (t testBearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != t.origin.Scheme || request.URL.Host != t.origin.Host {
		return nil, errors.New("unexpected test request origin")
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}
