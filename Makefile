# Development tools are pinned here and installed into ./bin, so every machine
# and CI run uses the same versions. They are deliberately kept out of go.mod:
# see docs/adr/0001-dependency-policy.md. Dependabot does not update them.
GOLANGCI_LINT_VERSION := v2.15.0
GOVULNCHECK_VERSION   := v1.9.0
ACTIONLINT_VERSION    := v1.7.12

# The database that `make db` starts and `make test` uses.
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:54329/taskmill?sslmode=disable
TEST_POSTGRES     ?= postgres:18-alpine

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
test: ## Run all tests with the race detector, in random order (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -shuffle=on -count=1 ./...

.PHONY: cover
cover: ## Run all tests and open the coverage report in a browser (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: bench
bench: ## Run the benchmarks and the fault scenarios against the test database (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go run ./internal/cmd/bench

.PHONY: faults
faults: ## Run the fault scenarios as CI does, against databases of their own (needs Docker).
	./internal/cmd/bench/faults.sh

.PHONY: vuln
vuln: $(GOVULNCHECK) ## Scan for known vulnerabilities.
	$(GOVULNCHECK) ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum.
	go mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod or go.sum is not tidy.
	go mod tidy -diff

# The test database is thrown away, so a commit does not wait for the disk:
# a slow disk on a CI machine would stall heartbeats past the tests' short
# leases.
.PHONY: db
db: ## Start a PostgreSQL container for the tests (TEST_POSTGRES picks the image).
	@image=$$(docker inspect -f '{{.Config.Image}}' taskmill-test-db 2>/dev/null); \
	if [ -n "$$image" ] && [ "$$image" != "$(TEST_POSTGRES)" ]; then \
		echo "taskmill-test-db runs $$image, not $(TEST_POSTGRES): run \`make db-stop\` first" >&2; exit 1; \
	fi
	@docker start taskmill-test-db >/dev/null 2>&1 || docker run -d --name taskmill-test-db -p 54329:5432 \
		-e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill $(TEST_POSTGRES) -c max_connections=500 \
		-c fsync=off -c synchronous_commit=off -c full_page_writes=off >/dev/null
	@until docker exec taskmill-test-db pg_isready -h 127.0.0.1 -U postgres -q; do sleep 0.2; done
	@echo "PostgreSQL is ready at $(TEST_DATABASE_URL)"

.PHONY: db-stop
db-stop: ## Remove the test PostgreSQL container.
	docker rm -f taskmill-test-db

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
