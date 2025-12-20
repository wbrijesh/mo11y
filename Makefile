.PHONY: build run test clean

build:
	@CGO_ENABLED=1 go build -o mo11y cmd/mo11y/main.go

run: build
	@./mo11y

test:
	@go test ./...

clean:
	@rm -f mo11y
