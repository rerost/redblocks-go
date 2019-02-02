PHONY: test
test:
	go test -v ./... | gex cgt
