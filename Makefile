PHONY: vendor
vendor:
	go get github.com/izumin5210/gex/cmd/gex
	# Gex depends on dep
	go get github.com/golang/dep/cmd/dep
	go get github.com/pierrre/gotestcover

	dep ensure -v -vendor-only

PHONY: test
test: vendor
	go test -race -coverprofile=coverage.txt -v ./... -covermode=atomic  | gex cgt

PHONY: coverage
coverage: test
	go tool cover -html=coverage.txt

PHONY: bench
bench: vendor
	go test -bench=. -race -v ./...
