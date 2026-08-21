package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

const (
	dashboardSessionLifetime = 8 * time.Hour
	maxDashboardResponse     = 48 << 20
)

type dashboardAssets interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
	ScriptPolicy() string
	StylePolicy() string
}

type dashboardBridge struct {
	upstream     *http.Client
	assets       dashboardAssets
	organization string
	mode         bootstrap.Mode
	version      string
	host         string
	origin       string
	scriptPolicy string
	stylePolicy  string
	now          func() time.Time

	mu               sync.Mutex
	bootstrapDigest  [sha256.Size]byte
	bootstrapUsed    bool
	sessionDigest    [sha256.Size]byte
	sessionExpires   time.Time
	bootstrapCleanup func()
}

type dashboardSessionRequest struct {
	BootstrapToken string `json:"bootstrap_token"`
}

func newDashboardBridge(upstream *http.Client, assets dashboardAssets, organization string, mode bootstrap.Mode, releaseVersion, host, bootstrapToken string, now func() time.Time, bootstrapCleanup func()) (*dashboardBridge, error) {
	if upstream == nil || assets == nil || organization == "" || releaseVersion == "" || host == "" || bootstrapToken == "" || now == nil || assets.ScriptPolicy() == "" || assets.StylePolicy() == "" || bootstrapCleanup == nil {
		return nil, fmt.Errorf("complete dashboard bridge configuration is required")
	}
	if !strings.HasPrefix(host, "127.0.0.1:") || strings.ContainsAny(host, "/@?#") {
		return nil, fmt.Errorf("dashboard bridge must use an exact IPv4 loopback host")
	}
	if mode != bootstrap.ModeSystem && mode != bootstrap.ModeUser {
		return nil, fmt.Errorf("dashboard installation mode is invalid")
	}
	return &dashboardBridge{
		upstream: upstream, assets: assets, organization: organization, mode: mode, version: releaseVersion,
		host: host, origin: "http://" + host, scriptPolicy: assets.ScriptPolicy(), stylePolicy: assets.StylePolicy(), now: now, bootstrapDigest: sha256.Sum256([]byte(bootstrapToken)),
		bootstrapCleanup: bootstrapCleanup,
	}, nil
}

func (b *dashboardBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setDashboardSecurityHeaders(w, b.scriptPolicy, b.stylePolicy)
	if r.Host != b.host {
		http.Error(w, "dashboard host is invalid", http.StatusMisdirectedRequest)
		return
	}
	if r.URL.Path == "/api/session" {
		b.establishSession(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		if origin := r.Header.Get("Origin"); origin != "" && origin != b.origin {
			writeDashboardJSON(w, http.StatusForbidden, map[string]string{"error": "dashboard origin is invalid"})
			return
		}
		if !b.authorized(r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeDashboardJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard session is invalid or expired"})
			return
		}
		if r.URL.Path == "/api/dashboard" {
			if r.Method != http.MethodGet || r.URL.RawQuery != "" {
				http.NotFound(w, r)
				return
			}
			b.mu.Lock()
			expires := b.sessionExpires
			b.mu.Unlock()
			writeDashboardJSON(w, http.StatusOK, map[string]string{
				"organization": b.organization, "mode": string(b.mode), "version": b.version,
				"session_expires_at": expires.UTC().Format(time.RFC3339Nano),
			})
			return
		}
		b.proxy(w, r)
		return
	}
	b.assets.ServeHTTP(w, r)
}

func (b *dashboardBridge) establishSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Origin") != b.origin {
		writeDashboardJSON(w, http.StatusForbidden, map[string]string{"error": "dashboard origin is invalid"})
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
		writeDashboardJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "dashboard session requires application/json"})
		return
	}
	defer func() { _ = r.Body.Close() }()
	var request dashboardSessionRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeDashboardJSON(w, http.StatusBadRequest, map[string]string{"error": "dashboard session request is invalid"})
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeDashboardJSON(w, http.StatusBadRequest, map[string]string{"error": "dashboard session request has trailing content"})
		return
	}
	received := sha256.Sum256([]byte(request.BootstrapToken))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bootstrapUsed || request.BootstrapToken == "" || subtle.ConstantTimeCompare(received[:], b.bootstrapDigest[:]) != 1 {
		writeDashboardJSON(w, http.StatusUnauthorized, map[string]string{"error": "dashboard bootstrap token is invalid or already used"})
		return
	}
	sessionToken, err := randomDashboardToken()
	if err != nil {
		writeDashboardJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "dashboard session is unavailable"})
		return
	}
	b.bootstrapUsed = true
	b.bootstrapDigest = [sha256.Size]byte{}
	b.bootstrapCleanup()
	b.bootstrapCleanup = func() {}
	b.sessionDigest = sha256.Sum256([]byte(sessionToken))
	b.sessionExpires = b.now().UTC().Add(dashboardSessionLifetime)
	writeDashboardJSON(w, http.StatusOK, map[string]string{"session_token": sessionToken})
}

func (b *dashboardBridge) authorized(header string) bool {
	if !strings.HasPrefix(header, "Bearer ") || strings.Count(header, " ") != 1 {
		return false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if token == "" {
		return false
	}
	received := sha256.Sum256([]byte(token))
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.sessionExpires.IsZero() && b.now().UTC().Before(b.sessionExpires) &&
		subtle.ConstantTimeCompare(received[:], b.sessionDigest[:]) == 1
}

func (b *dashboardBridge) proxy(w http.ResponseWriter, incoming *http.Request) {
	upstreamPath := strings.TrimPrefix(incoming.URL.Path, "/api")
	if !allowedDashboardRoute(incoming.Method, upstreamPath, incoming.URL.RawQuery) {
		http.NotFound(w, incoming)
		return
	}
	bodyLimit := int64(256 << 10)
	if incoming.Method == http.MethodPost && strings.HasPrefix(upstreamPath, "/v1/user/tasks/") && strings.HasSuffix(upstreamPath, "/completion") {
		bodyLimit = 48 << 20
	}
	var body io.Reader
	if incoming.Body != nil {
		defer func() { _ = incoming.Body.Close() }()
		body = http.MaxBytesReader(w, incoming.Body, bodyLimit)
	}
	request, err := http.NewRequestWithContext(incoming.Context(), incoming.Method, "http://agentos.local"+upstreamPath, body)
	if err != nil {
		writeDashboardJSON(w, http.StatusBadGateway, map[string]string{"error": "local user gateway request is invalid"})
		return
	}
	request.URL.RawQuery = incoming.URL.RawQuery
	request.Header.Set("Accept", "application/json")
	if incoming.Method == http.MethodPost {
		if incoming.Header.Get("Content-Type") != "application/json" {
			writeDashboardJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "dashboard mutations require application/json"})
			return
		}
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := b.upstream.Do(request)
	if err != nil {
		writeDashboardJSON(w, http.StatusBadGateway, map[string]string{"error": "local user gateway is unavailable"})
		return
	}
	defer func() { _ = response.Body.Close() }()
	mediaType, _, mediaTypeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaTypeErr != nil || mediaType != "application/json" {
		writeDashboardJSON(w, http.StatusBadGateway, map[string]string{"error": "local user gateway returned an invalid media type"})
		return
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxDashboardResponse+1))
	if err != nil || len(payload) > maxDashboardResponse {
		writeDashboardJSON(w, http.StatusBadGateway, map[string]string{"error": "local user gateway response is invalid"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(payload)
}

func allowedDashboardRoute(method, requestPath, rawQuery string) bool {
	segments := strings.Split(strings.TrimPrefix(requestPath, "/"), "/")
	for _, segment := range segments {
		if !validDashboardSegment(segment) {
			return false
		}
	}
	switch {
	case method == http.MethodPost && requestPath == "/v1/user/messages" && rawQuery == "":
		return true
	case method == http.MethodGet && requestPath == "/v1/user/intents/active" && rawQuery == "":
		return true
	case method == http.MethodPost && len(segments) == 5 && segments[0] == "v1" && segments[1] == "user" && segments[2] == "intents" && segments[4] == "confirm" && rawQuery == "":
		return true
	case method == http.MethodGet && len(segments) == 4 && segments[0] == "v1" && segments[1] == "user" && segments[2] == "tasks" && rawQuery == "":
		return true
	case method == http.MethodPost && len(segments) == 5 && segments[0] == "v1" && segments[1] == "user" && segments[2] == "tasks" && segments[4] == "completion" && rawQuery == "":
		return true
	case method == http.MethodGet && requestPath == "/v1/user/reviews":
		return validReviewQuery(rawQuery)
	case (method == http.MethodGet || method == http.MethodPost) && len(segments) == 4 && segments[0] == "v1" && segments[1] == "user" && segments[2] == "reviews" && rawQuery == "":
		return true
	case method == http.MethodGet && requestPath == "/v1/control/approvals" && rawQuery == "":
		return true
	case method == http.MethodGet && len(segments) == 4 && segments[0] == "v1" && segments[1] == "control" && segments[2] == "approvals" && rawQuery == "":
		return true
	case method == http.MethodPost && len(segments) == 5 && segments[0] == "v1" && segments[1] == "control" && segments[2] == "approvals" &&
		(segments[4] == "acknowledge" || segments[4] == "begin" || segments[4] == "decision") && rawQuery == "":
		return true
	default:
		return false
	}
}

func validReviewQuery(raw string) bool {
	if raw == "" {
		return true
	}
	values, err := parseDashboardQuery(raw)
	if err != nil {
		return false
	}
	for key, entries := range values {
		if key != "after" && key != "limit" || len(entries) != 1 {
			return false
		}
		if key == "after" && !validDashboardSegment(entries[0]) {
			return false
		}
		if key == "limit" {
			limit, err := strconv.Atoi(entries[0])
			if err != nil || limit < 1 || limit > 100 {
				return false
			}
		}
	}
	return true
}

func validDashboardSegment(segment string) bool {
	if segment == "" || len(segment) > 256 {
		return false
	}
	for _, character := range segment {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("-._:", character) {
			continue
		}
		return false
	}
	return true
}

func parseDashboardQuery(raw string) (map[string][]string, error) {
	values := make(map[string][]string)
	for _, pair := range strings.Split(raw, "&") {
		key, value, found := strings.Cut(pair, "=")
		if !found || key == "" || strings.ContainsAny(key+value, "%+;") {
			return nil, fmt.Errorf("non-canonical query")
		}
		values[key] = append(values[key], value)
	}
	return values, nil
}

func randomDashboardToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func setDashboardSecurityHeaders(w http.ResponseWriter, scriptPolicy, stylePolicy string) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; img-src 'self' data:; object-src 'none'; script-src "+scriptPolicy+"; style-src 'self'; style-src-attr "+stylePolicy)
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
}

func writeDashboardJSON(w http.ResponseWriter, status int, value any) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(value); err != nil {
		http.Error(w, "dashboard response is unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body.Bytes())
}
