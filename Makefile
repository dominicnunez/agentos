.PHONY: test run

test:
	go test ./...

run:
	go run ./cmd/agentos
