package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestIntentReviewDisplaysExactReplacementWork(t *testing.T) {
	var output bytes.Buffer
	printIntentReview(&output, core.IntentDraft{
		Objective: "retry bounded work", Mode: core.IntentModeStandard, ReplacesWork: &core.IntentValue{Value: "work-failed-1"},
	})
	if !strings.Contains(output.String(), "Replaces failed Work\nwork-failed-1") {
		t.Fatalf("replacement review omitted predecessor identity: %q", output.String())
	}
}

func TestTaskStatusDisplaysDurableWorkIdentity(t *testing.T) {
	var output bytes.Buffer
	printTaskStatus(&output, tuiTask{TaskID: "task-1", WorkID: "work-1", State: "FAILED"})
	if !strings.Contains(output.String(), "Task task-1 - FAILED") || !strings.Contains(output.String(), "Work work-1") {
		t.Fatalf("task status omitted Work identity: %q", output.String())
	}
}

func TestReadArtifactUploadRejectsLinksAndReadsBoundedRegularFiles(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "evidence.txt")
	want := []byte("bounded evidence\n")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readArtifactUpload(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("artifact=%q err=%v", got, err)
	}
	link := filepath.Join(directory, "evidence-link.txt")
	if err := os.Symlink(path, link); err == nil {
		if _, err := readArtifactUpload(link); err == nil {
			t.Fatal("artifact symlink was accepted")
		}
	}
}

func TestTUICompletionReviewRequiresExactEvidencePhrase(t *testing.T) {
	fingerprint := strings.Repeat("a", 64)
	client := &tuiClient{http: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"reviews":[{"review_id":"review-1","task_id":"task-child","task_version":2,"fingerprint":"` + fingerprint + `","state":"PENDING","objective":"candidate work","candidate_result":"result","criteria":[{"id":"criterion-1","description":"matches requested outcome","assurance":"HUMAN_JUDGMENT","required":true}],"evidence_refs":["event-1"],"updated_at":"2026-08-12T00:00:00Z"}]}`
		if request.Method == http.MethodPost {
			var mutation map[string]string
			if err := json.NewDecoder(request.Body).Decode(&mutation); err != nil {
				t.Fatal(err)
			}
			if request.URL.Path != "/v1/user/reviews/task-child" || mutation["review_id"] != "review-1" || mutation["fingerprint"] != fingerprint || mutation["decision"] != "APPROVE" {
				t.Fatalf("review mutation path=%s body=%+v", request.URL.Path, mutation)
			}
			body = strings.Replace(body, `"state":"PENDING"`, `"state":"APPROVE"`, 1)
			body = strings.TrimPrefix(strings.TrimSuffix(body, "}"), `{"reviews":[`)
			body = strings.TrimSuffix(body, "]")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	input := bufio.NewReader(strings.NewReader("1\nAPPROVE " + fingerprint[:12] + "\n"))
	var output bytes.Buffer
	if err := client.completionReviews(context.Background(), input, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "does not approve any consequential effect") || !strings.Contains(output.String(), "Completion review approve.") {
		t.Fatalf("output=%q", output.String())
	}
}
