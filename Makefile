.PHONY: build vet fmt fmt-check test test-race run-server run-demo tidy

build:
	go build ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "gofmt needs to be run on:"; echo "$$out"; exit 1; \
	fi

test:
	go test ./...

test-race:
	go test -race ./...

run-server:
	go run ./cmd/syncforged

run-demo:
	go run ./cmd/syncforge-demo

tidy:
	go mod tidy
