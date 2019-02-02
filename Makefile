PHONY: vendor
vendor:
	go get github.com/izumin5210/gex/cmd/gex
	# Gex depends on dep
	go get github.com/golang/dep/cmd/dep

	dep ensure -v -vendor-only

PHONY: test
test: vendor
	go test -v ./... | gex cgt
