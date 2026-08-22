package dashboard

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func TestEmbeddedDashboardServesOnlyGeneratedFiles(t *testing.T) {
	handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "<!doctype html>") {
		t.Fatalf("dashboard root=%d %q", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("index cache policy=%q", response.Header().Get("Cache-Control"))
	}
	if !strings.Contains(handler.ScriptPolicy(), "'sha256-") {
		t.Fatalf("script policy=%q", handler.ScriptPolicy())
	}
	if !strings.Contains(handler.StylePolicy(), "'unsafe-hashes' 'sha256-") {
		t.Fatalf("style policy=%q", handler.StylePolicy())
	}
	asset := regexp.MustCompile(`href="\.(/_app/immutable/[^"]+)"`).FindStringSubmatch(response.Body.String())
	if len(asset) != 2 {
		t.Fatal("dashboard index lacks a generated immutable asset")
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, asset[1], nil))
	if response.Code != http.StatusOK || response.Body.Len() == 0 || response.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Fatalf("embedded asset=%d bytes=%d cache=%q", response.Code, response.Body.Len(), response.Header().Get("Cache-Control"))
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/not-generated", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown dashboard resource=%d", response.Code)
	}
}

func TestEmbeddedDashboardRetainsGovernedWorkReviewFields(t *testing.T) {
	handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	var generated strings.Builder
	if err := fs.WalkDir(handler.files, "_app/immutable", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(name, ".js") {
			return nil
		}
		body, err := fs.ReadFile(handler.files, name)
		if err != nil {
			return err
		}
		generated.Write(body)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Replaces Work", "Requested execution", "Resolved decisions", "More information required",
		"Effect fingerprint", "Urgency", "Expires", "Evidence fingerprint", "Confirm exact Intent",
	} {
		if !strings.Contains(generated.String(), required) {
			t.Fatalf("embedded dashboard omitted governed review field %q", required)
		}
	}
}
