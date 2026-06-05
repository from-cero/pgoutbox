ROOT_MODULE = github.com/from-cero/pgoutbox
MODULES = . \
	relay/publisher/kafka \
	relay/publisher/redpanda

format-tools:
	go install mvdan.cc/gofumpt@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/daixiang0/gci@latest
	go install github.com/segmentio/golines@latest

format:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done
	goimports -w .
	gci write --custom-order -s standard -s default -s "prefix($(ROOT_MODULE))" -s blank \
		--no-lex-order --skip-generated --skip-vendor .
	golines -w -m 120 .
	gofumpt -l -w -extra .

lint-tools:
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $$(go env GOPATH)/bin v2.12.1

lint:
	@for m in $(MODULES); do echo "==> lint $$m" && (cd $$m && golangci-lint run ./...) || exit 1; done

test:
	@for m in $(MODULES); do echo "==> test $$m" && (cd $$m && go test -race -cover ./...) || exit 1; done

test-coverage:
	go test -race -coverprofile=coverage.out ./... && go tool cover -html=coverage.out

precommit: format lint test
