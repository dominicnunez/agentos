package modelinput

import "github.com/dominicnunez/agentos/internal/boundaryjson"

// Decode validates the closed request schema before any roles are consumed.
func Decode(body []byte) (Request, error) {
	if len(body) > MaximumBytes {
		return Request{}, ErrLimit
	}
	var request Request
	if boundaryjson.Unmarshal(body, &request) != nil {
		return Request{}, ErrInvalid
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}
