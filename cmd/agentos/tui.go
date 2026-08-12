package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

type tuiClient struct{ http *http.Client }

func runTUI(ctx context.Context, _ string, config bootstrap.Config, input *os.File, output io.Writer) error {
	client, err := localHTTPClient(config.Paths.UserSocket)
	if err != nil {
		return err
	}
	console := &tuiClient{http: client}
	reader := bufio.NewReader(input)
	if _, err := fmt.Fprintln(output, "Agent OS\n\n[Work]  Approvals  Agents  System\nType work in natural language. Commands: /user-task /complete /approvals /agents /system /quit"); err != nil {
		return err
	}
	for {
		if _, err := fmt.Fprint(output, "\n> "); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil && !(err == io.EOF && line != "") {
			return err
		}
		line = canonicalInput(line)
		switch line {
		case "":
			continue
		case "/quit", "/exit":
			return nil
		case "/approvals":
			if err := console.approvals(ctx, reader, output); err != nil {
				_, _ = fmt.Fprintf(output, "Approvals unavailable: %s\n", safeTerminalLine(err.Error()))
			}
		case "/agents":
			_, _ = fmt.Fprintln(output, "Agents\nDurable Agent roster views will appear here; this console never reads internal storage directly.")
		case "/system":
			_, _ = fmt.Fprintln(output, "System\nRun `agentos doctor` for the read-only health report.")
		default:
			if line == "/complete" || line == "/user-task" {
				_, _ = fmt.Fprintln(output, "A task description or task identity is required.")
				continue
			}
			if strings.HasPrefix(line, "/complete ") {
				if err := console.completeHumanTask(ctx, canonicalInput(strings.TrimPrefix(line, "/complete ")), reader, output); err != nil {
					_, _ = fmt.Fprintf(output, "Completion unavailable: %s\n", safeTerminalLine(err.Error()))
				}
				continue
			}
			messageID, idErr := randomID("message")
			if idErr != nil {
				return idErr
			}
			conversationID, idErr := randomID("user")
			if idErr != nil {
				return idErr
			}
			var response tuiTask
			request := map[string]any{"conversation_id": conversationID, "message_id": messageID, "text": line}
			if strings.HasPrefix(line, "/user-task ") {
				request["text"] = canonicalInput(strings.TrimPrefix(line, "/user-task "))
				request["execution_kind"] = core.ExecutionHuman
			}
			if err := console.request(ctx, http.MethodPost, "/v1/user/messages", request, &response); err != nil {
				_, _ = fmt.Fprintf(output, "Work unavailable: %s\n", safeTerminalLine(err.Error()))
				continue
			}
			_, _ = fmt.Fprintf(output, "Task %s - %s\n", response.TaskID, response.State)
			if response.Prompt != "" {
				_, _ = fmt.Fprintln(output, safeTerminalText(response.Prompt))
			}
			if response.CompletionContract != nil {
				_, _ = fmt.Fprintf(output, "Use /complete %s when every required item is ready.\n", response.TaskID)
			}
			if response.Result != "" {
				_, _ = fmt.Fprintln(output, safeTerminalText(response.Result))
			}
		}
	}
}

type tuiTask struct {
	TaskID             string                   `json:"task_id"`
	State              string                   `json:"state"`
	Prompt             string                   `json:"prompt"`
	Result             string                   `json:"result"`
	CompletionContract *core.CompletionContract `json:"completion_contract"`
}

func (c *tuiClient) completeHumanTask(ctx context.Context, taskID string, reader *bufio.Reader, output io.Writer) error {
	if err := intake.ValidateIdentifier("task", taskID); err != nil {
		return err
	}
	var task tuiTask
	if err := c.request(ctx, http.MethodGet, "/v1/user/tasks/"+taskID, nil, &task); err != nil {
		return err
	}
	if task.CompletionContract == nil {
		return fmt.Errorf("task has no user CompletionContract")
	}
	fields := make(map[string]string, len(task.CompletionContract.RequiredFields))
	for _, requirement := range task.CompletionContract.RequiredFields {
		_, _ = fmt.Fprintf(output, "%s: ", safeTerminalLine(requirement.Name))
		value, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		fields[requirement.Name] = strings.TrimRight(value, "\r\n")
	}
	uploads := make([]artifacts.Upload, 0)
	for _, requirement := range task.CompletionContract.ArtifactRequirements {
		for index := 0; index < requirement.MinCount; index++ {
			_, _ = fmt.Fprintf(output, "%s file %d: ", safeTerminalLine(requirement.Role), index+1)
			path, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			path = filepath.Clean(canonicalInput(path))
			if !filepath.IsAbs(path) {
				return fmt.Errorf("artifact path must be absolute")
			}
			body, err := readArtifactUpload(path)
			if err != nil {
				return fmt.Errorf("read artifact: %w", err)
			}
			mediaType, _, err := mime.ParseMediaType(http.DetectContentType(body))
			if err != nil {
				return fmt.Errorf("detect artifact media type: %w", err)
			}
			uploads = append(uploads, artifacts.Upload{Role: requirement.Role, Name: filepath.Base(path), MediaType: mediaType, Data: body})
		}
	}
	messageID, err := randomID("completion")
	if err != nil {
		return err
	}
	request := map[string]any{"message_id": messageID, "fields": fields, "artifacts": uploads}
	if err := c.request(ctx, http.MethodPost, "/v1/user/tasks/"+taskID+"/completion", request, &task); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "Task %s - %s\n%s\n", task.TaskID, task.State, safeTerminalText(task.Result))
	return err
}

type tuiApproval struct {
	ApprovalID                string            `json:"approval_id"`
	Action                    string            `json:"action"`
	Resource                  string            `json:"resource"`
	Scope                     string            `json:"scope"`
	CanonicalEffectDescriptor string            `json:"canonical_effect_descriptor"`
	EffectArguments           map[string]string `json:"effect_arguments"`
	Boundary                  string            `json:"boundary"`
	Risk                      string            `json:"risk"`
	Urgency                   string            `json:"urgency"`
	EffectFingerprint         string            `json:"effect_fingerprint"`
	Status                    string            `json:"status"`
	SingleUse                 bool              `json:"single_use"`
	ExpiresAt                 string            `json:"expires_at"`
}

func (c *tuiClient) approvals(ctx context.Context, reader *bufio.Reader, output io.Writer) error {
	var inbox struct {
		Approvals []tuiApproval `json:"approvals"`
	}
	if err := c.request(ctx, http.MethodGet, "/v1/control/approvals", nil, &inbox); err != nil {
		return err
	}
	if len(inbox.Approvals) == 0 {
		_, err := fmt.Fprintln(output, "Approvals\nNo pending approvals.")
		return err
	}
	_, _ = fmt.Fprintln(output, "Approvals")
	for index, approval := range inbox.Approvals {
		_, _ = fmt.Fprintf(output, "%d. %s %s - %s - %s - %s\n", index+1, safeTerminalLine(approval.Action), safeTerminalLine(approval.Resource), safeTerminalLine(approval.Boundary), safeTerminalLine(approval.Risk), safeTerminalLine(string(approval.Status)))
	}
	_, _ = fmt.Fprint(output, "Select approval number, or press Enter to return: ")
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = canonicalInput(line)
	if line == "" {
		return nil
	}
	var index int
	if _, err := fmt.Sscanf(line, "%d", &index); err != nil || index < 1 || index > len(inbox.Approvals) {
		return fmt.Errorf("selection is invalid")
	}
	approval := inbox.Approvals[index-1]
	short := approval.EffectFingerprint
	if len(short) > 12 {
		short = short[:12]
	}
	phrase := "APPROVE " + short
	_, _ = fmt.Fprintf(output, "\nExact effect\nDescription: %s\nAction: %s\nResource: %s\nScope: %s\nBoundary: %s\nRisk: %s\nUrgency: %s\nSingle use: %t\n", safeTerminalLine(approval.CanonicalEffectDescriptor), safeTerminalLine(approval.Action), safeTerminalLine(approval.Resource), safeTerminalLine(approval.Scope), safeTerminalLine(approval.Boundary), safeTerminalLine(approval.Risk), safeTerminalLine(approval.Urgency), approval.SingleUse)
	keys := make([]string, 0, len(approval.EffectArguments))
	for key := range approval.EffectArguments {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = fmt.Fprintf(output, "Argument %s: %s\n", safeTerminalLine(key), safeTerminalLine(approval.EffectArguments[key]))
	}
	if approval.ExpiresAt != "" {
		_, _ = fmt.Fprintf(output, "Expires: %s\n", safeTerminalLine(approval.ExpiresAt))
	}
	_, _ = fmt.Fprintf(output, "Fingerprint: %s\n\nType %q to approve, DENY to deny, or press Enter to cancel: ", approval.EffectFingerprint, phrase)
	confirmation, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	confirmation = canonicalInput(confirmation)
	if confirmation == "" {
		return nil
	}
	decision := "DENY"
	if confirmation == phrase {
		decision = "APPROVE"
	} else if confirmation != "DENY" {
		return fmt.Errorf("confirmation did not match the exact effect")
	}
	mutation := map[string]string{"effect_fingerprint": approval.EffectFingerprint}
	base := "/v1/control/approvals/" + approval.ApprovalID
	if approval.Status == "PENDING" || approval.Status == "NOTIFIED" {
		if err := c.request(ctx, http.MethodPost, base+"/acknowledge", mutation, &approval); err != nil {
			return err
		}
	}
	if approval.Status == "ACKNOWLEDGED" {
		if err := c.request(ctx, http.MethodPost, base+"/begin", mutation, &approval); err != nil {
			return err
		}
	}
	decisionRequest := map[string]string{"effect_fingerprint": approval.EffectFingerprint, "decision": decision}
	if err := c.request(ctx, http.MethodPost, base+"/decision", decisionRequest, &approval); err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "Approval %s.\n", strings.ToLower(decision))
	return err
}

func (c *tuiClient) request(ctx context.Context, method, path string, body, target any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://agentos.local"+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("local gateway is unavailable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("local gateway returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("local gateway response is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("local gateway response has trailing content")
	}
	return nil
}

func readArtifactUpload(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > artifacts.MaximumArtifactBytes {
		return nil, fmt.Errorf("artifact must be a bounded regular file, not a link")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("artifact changed while it was opened")
	}
	body, err := io.ReadAll(io.LimitReader(file, artifacts.MaximumArtifactBytes+1))
	if err != nil || int64(len(body)) != before.Size() {
		return nil, fmt.Errorf("artifact changed while it was read")
	}
	return body, nil
}

func randomID(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(value[:]), nil
}
