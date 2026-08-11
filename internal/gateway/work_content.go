package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"

	"github.com/dominicnunez/agentos/internal/trustconfig"
)

func decodeWorkContent(w http.ResponseWriter, r *http.Request, target any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maximumA2ARequestBytes))
	if err != nil {
		return err
	}
	var content any
	if err := json.Unmarshal(body, &content); err != nil {
		return err
	}
	if err := rejectAuthorityContent(content); err != nil {
		return err
	}
	return trustconfig.DecodeObject(bytes.NewReader(body), "operator work content", target)
}

func hasJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}
