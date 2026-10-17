# Development tools are pinned here and installed into ./bin, so every machine
# and CI run uses the same versions. They are deliberately kept out of go.mod:
# see docs/adr/0001-dependency-policy.md. Dependabot does not update them.
GOLANGCI_LINT_VERSION := v2.15.0
GOVULNCHECK_VERSION   := v1.9.0
ACTIONLINT_VERSION    := v1.7.12

BIN           := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOVULNCHECK   := $(BIN)/govulncheck-$(GOVULNCHECK_VERSION)
ACTIONLINT    := $(BIN)/actionlint-$(ACTIONLINT_VERSION)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN { FS = ":.*## " } /^[a-z-]+:.*## / { printf "  %-12s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint test vuln ## Run every check that CI runs.

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format all Go files.
	$(GOLANGCI_LINT) fmt

.PHONY: lint
lint: $(GOLANGCI_LINT) $(ACTIONLINT) ## Run the Go and GitHub Actions linters.
	$(GOLANGCI_LINT) run
	$(ACTIONLINT)

.PHONY: test
test: ## Run all tests with the race detector, in random order.
	go test -race -shuffle=on -count=1 ./...

.PHONY: cover
cover: ## Run all tests and open the coverage report in a browser.
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: vuln
vuln: $(GOVULNCHECK) ## Scan for known vulnerabilities.
	$(GOVULNCHECK) ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum.
	go mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod or go.sum is not tidy.
	go mod tidy -diff

.PHONY: tools
tools: $(GOLANGCI_LINT) $(GOVULNCHECK) $(ACTIONLINT) ## Install the pinned development tools into ./bin.

.PHONY: clean
clean: ## Remove installed tools and generated reports.
	rm -rf $(BIN) coverage.out

$(GOLANGCI_LINT):
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN)/golangci-lint $@

$(GOVULNCHECK):
	GOBIN=$(BIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	mv $(BIN)/govulncheck $@

$(ACTIONLINT):
	GOBIN=$(BIN) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	mv $(BIN)/actionlint $@
