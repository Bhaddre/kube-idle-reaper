.PHONY: build test lint manifests image

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/idle-reaper ./cmd/idle-reaper

test:
	go test -race -count=1 -cover ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...

manifests:
	kubeconform -strict -summary deploy/

image:
	docker build -t idle-reaper:dev .
