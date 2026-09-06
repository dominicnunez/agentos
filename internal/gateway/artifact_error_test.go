package gateway

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestHumanDefaultContractRejectsArtifactsBeforeStorage(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := testHumanHandler(t, intake.New(app.New(events.NewGateway(store))))
	response := submitAndConfirmHuman(t, handler, humanMessageRequest{
		ConversationID: "artifact-failure", MessageID: "message-1", Text: "provide a report", ExecutionKind: core.ExecutionHuman,
	})
	var task humanTaskResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &task) != nil || task.TaskID == "" {
		t.Fatalf("create task: %d %s", response.Code, response.Body.String())
	}
	root := filepath.Join(t.TempDir(), "PRIVATE_STORAGE_CANARY")
	if err := os.WriteFile(root, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	handler.artifacts = &artifacts.Store{Root: root}
	request := humanCompletionRequest{MessageID: "completion-1", Fields: map[string]string{"response": "report supplied"}, Artifacts: []artifacts.Upload{{Role: "report", Name: "report.txt", Data: []byte("report content")}}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/tasks/"+task.TaskID+"/completion", testOwnerMarker, string(body))
	if strings.Contains(response.Body.String(), "PRIVATE_STORAGE_CANARY") || strings.Contains(response.Body.String(), root) {
		t.Fatalf("storage diagnostic exposed: %s", response.Body.String())
	}
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unexpected artifact role") {
		t.Fatalf("contract rejection response: %d %s", response.Code, response.Body.String())
	}
	correlation, found, err := store.ResolveExternalWork(t.Context(), "org-1", "artifact-failure")
	if err != nil || !found {
		t.Fatalf("work lookup: %v", err)
	}
	stream, err := store.Events(t.Context(), correlation)
	if err != nil {
		t.Fatal(err)
	}
	if countGatewayEvents(stream, "TASK_COMPLETED") != 0 {
		t.Fatal("storage failure admitted completion")
	}
}
