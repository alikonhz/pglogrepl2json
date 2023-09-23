.PHONY: test
test:
	go test ./replicator/*

.PHONY: run
run:
	go run ./example/cmd/main.go