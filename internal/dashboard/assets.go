// Package dashboard serves the generated, embedded web dashboard assets.
// It owns no Agent OS authority, persistence, or domain state.
package dashboard

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

//go:embed all:dist
var embedded embed.FS

type Handler struct {
	files        fs.FS
	scriptPolicy string
	stylePolicy  string
}

func New() (Handler, error) {
	files, err := fs.Sub(embedded, "dist")
	if err != nil {
		return Handler{}, err
	}
	index, err := fs.ReadFile(files, "index.html")
	if err != nil {
		return Handler{}, fmt.Errorf("read dashboard index: %w", err)
	}
	script, err := oneInlineScript(index)
	if err != nil {
		return Handler{}, err
	}
	digest := sha256.Sum256(script)
	stylePolicy, err := generatedStylePolicy(files)
	if err != nil {
		return Handler{}, err
	}
	return Handler{
		files: files, scriptPolicy: "'self' 'sha256-" + base64.StdEncoding.EncodeToString(digest[:]) + "'",
		stylePolicy: stylePolicy,
	}, nil
}

func (h Handler) ScriptPolicy() string { return h.scriptPolicy }
func (h Handler) StylePolicy() string  { return h.stylePolicy }

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name == "." || name == "" {
		name = "index.html"
	}
	if !fs.ValidPath(name) || strings.HasSuffix(name, "/") {
		http.NotFound(w, r)
		return
	}
	body, err := fs.ReadFile(h.files, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(name, "_app/immutable/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-store")
	}
	if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(body))
}

func oneInlineScript(index []byte) ([]byte, error) {
	const startTag = "<script>"
	const endTag = "</script>"
	start := bytes.Index(index, []byte(startTag))
	if start < 0 {
		return nil, fmt.Errorf("dashboard index lacks its bootstrap script")
	}
	start += len(startTag)
	end := bytes.Index(index[start:], []byte(endTag))
	if end < 0 {
		return nil, fmt.Errorf("dashboard bootstrap script is incomplete")
	}
	end += start
	if bytes.Contains(index[end+len(endTag):], []byte(startTag)) {
		return nil, fmt.Errorf("dashboard index contains multiple inline scripts")
	}
	return index[start:end], nil
}

var inlineStyle = regexp.MustCompile(`style="([^"]+)"`)

func generatedStylePolicy(files fs.FS) (string, error) {
	hashes := make(map[string]struct{})
	err := fs.WalkDir(files, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || path.Ext(name) != ".js" && path.Ext(name) != ".html" {
			return nil
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return err
		}
		for _, match := range inlineStyle.FindAllSubmatch(body, -1) {
			digest := sha256.Sum256(match[1])
			hashes["'sha256-"+base64.StdEncoding.EncodeToString(digest[:])+"'"] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("inspect generated dashboard styles: %w", err)
	}
	if len(hashes) == 0 {
		return "'none'", nil
	}
	values := make([]string, 0, len(hashes))
	for value := range hashes {
		values = append(values, value)
	}
	sort.Strings(values)
	return "'unsafe-hashes' " + strings.Join(values, " "), nil
}
