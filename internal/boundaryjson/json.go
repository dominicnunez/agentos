// Package boundaryjson decodes security-boundary JSON without ambiguous keys
// or input-derived diagnostic text. Domain validation remains with callers.
package boundaryjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

const (
	MaximumBytes = 16 << 20
	MaximumDepth = 64
)

var (
	ErrInvalid = errors.New("invalid or ambiguous boundary JSON")
	ErrSchema  = errors.New("boundary JSON does not match its exact schema")
	ErrLimit   = errors.New("boundary JSON exceeds resource limits")
)

// Validate rejects duplicate keys in every object, invalid UTF-8, excessive
// nesting, and trailing values. Open data objects keep case-sensitive keys.
// Callers must enforce any narrower transport or domain byte limits first.
func Validate(data []byte) error { return validate(data, nil) }

type validation struct {
	decoder *json.Decoder
	schemas map[reflect.Type]map[string]fieldShape
}

func validate(data []byte, typ reflect.Type) error {
	if len(data) > MaximumBytes {
		return ErrLimit
	}
	if !utf8.Valid(data) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v := validation{decoder: d, schemas: make(map[reflect.Type]map[string]fieldShape)}
	if err := v.value(0, typ); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrInvalid
	}
	return nil
}

func (v *validation) value(depth int, typ reflect.Type) error {
	if depth > MaximumDepth {
		return ErrLimit
	}
	for n := 0; typ != nil && typ.Kind() == reflect.Pointer; n++ {
		if n > MaximumDepth {
			return ErrSchema
		}
		typ = typ.Elem()
	}
	if typ == rawType || typ != nil && typ.Kind() == reflect.Interface {
		typ = nil
	}
	token, err := v.decoder.Token()
	if err != nil {
		return ErrInvalid
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delim {
	case '{':
		var fields map[string]fieldShape
		var element reflect.Type
		if typ != nil {
			if typ.Kind() == reflect.Struct {
				fields = v.schemas[typ]
				if fields == nil {
					fields = structFields(typ)
					v.schemas[typ] = fields
				}
			} else if typ.Kind() == reflect.Map {
				if typ.Key().Kind() != reflect.String {
					return ErrSchema
				}
				element = typ.Elem()
			} else {
				return ErrSchema
			}
		}
		seen := make(map[string]struct{})
		for v.decoder.More() {
			keyToken, err := v.decoder.Token()
			if err != nil {
				return ErrInvalid
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalid
			}
			if _, found := seen[key]; found {
				return ErrInvalid
			}
			seen[key] = struct{}{}
			child := element
			if fields != nil {
				field, found := fields[key]
				if !found || field.ambiguous {
					return ErrSchema
				}
				child = field.typ
			}
			if err := v.value(depth+1, child); err != nil {
				return err
			}
		}
		if end, err := v.decoder.Token(); err != nil || end != json.Delim('}') {
			return ErrInvalid
		}
	case '[':
		var element reflect.Type
		if typ != nil {
			if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Array {
				return ErrSchema
			}
			element = typ.Elem()
		}
		for v.decoder.More() {
			if err := v.value(depth+1, element); err != nil {
				return err
			}
		}
		if end, err := v.decoder.Token(); err != nil || end != json.Delim(']') {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// Unmarshal requires exact case-sensitive struct field names and a closed
// schema, including nested structs. Maps/interfaces/RawMessage are explicit
// open-data seams; their full JSON still passes Validate. Decoding is atomic:
// a failure leaves the caller's destination unchanged, and successful decoding
// starts from zero rather than retaining omitted values from an earlier input.
func Unmarshal(data []byte, target any) error {
	return decode(data, target, false)
}

// UnmarshalNumbers preserves exact number tokens in explicitly open data.
func UnmarshalNumbers(data []byte, target any) error {
	return decode(data, target, true)
}

func decode(data []byte, target any, numbers bool) error {
	destination := reflect.ValueOf(target)
	if !destination.IsValid() || destination.Kind() != reflect.Pointer || destination.IsNil() {
		return ErrSchema
	}
	if err := validate(data, destination.Type().Elem()); err != nil {
		return err
	}
	next := reflect.New(destination.Type().Elem())
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if numbers {
		d.UseNumber()
	}
	if err := d.Decode(next.Interface()); err != nil {
		return ErrSchema
	}
	destination.Elem().Set(next.Elem())
	return nil
}

var rawType = reflect.TypeFor[json.RawMessage]()

type fieldShape struct {
	typ               reflect.Type
	depth             int
	tagged, ambiguous bool
}

func structFields(typ reflect.Type) map[string]fieldShape {
	fields := make(map[string]fieldShape)
	collectFields(typ, 0, fields, make(map[reflect.Type]bool))
	return fields
}

func collectFields(typ reflect.Type, depth int, fields map[string]fieldShape, visiting map[reflect.Type]bool) {
	if visiting[typ] {
		return
	}
	visiting[typ] = true
	defer delete(visiting, typ)
	for index := 0; index < typ.NumField(); index++ {
		field := typ.Field(index)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		embedded := field.Type
		if embedded.Kind() == reflect.Pointer {
			embedded = embedded.Elem()
		}
		if field.Anonymous && name == "" && embedded.Kind() == reflect.Struct {
			collectFields(embedded, depth+1, fields, visiting)
			continue
		}
		if field.PkgPath != "" {
			continue
		}
		tagged := name != ""
		if name == "" {
			name = field.Name
		}
		candidate := fieldShape{typ: field.Type, depth: depth, tagged: tagged}
		prior, exists := fields[name]
		if !exists || depth < prior.depth || depth == prior.depth && tagged && !prior.tagged {
			fields[name] = candidate
		} else if depth == prior.depth && tagged == prior.tagged {
			prior.ambiguous = true
			fields[name] = prior
		}
	}
}
