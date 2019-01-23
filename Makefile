all:
	@go install github.com/varlink/go/cmd/varlink-go-interface-generator
	@go generate ./...
	@go build -o service
.PHONY: all

update:
	@go get -u github.com/varlink/go/varlink
	@go get -u github.com/varlink/go/cmd/varlink-go-interface-generator
.PHONY: update

clean:
	rm -f service
.PHONY: clean
