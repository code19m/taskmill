# Development tools are pinned here and installed into ./bin, so every machine
# and CI run uses the same versions. They are deliberately kept out of go.mod:
# see docs/adr/0001-dependency-policy.md. Dependabot does not update them.
# golang.org/x/exp, which holds gorelease, has no releases: it is pinned to a
# commit.
GOLANGCI_LINT_VERSION := v2.15.0
GOVULNCHECK_VERSION   := v1.9.0
ACTIONLINT_VERSION    := v1.7.12
GORELEASE_VERSION     := v0.0.0-20261014143641-f0251eb4aa3e

# The database that `make db` starts and `make test` uses.
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:54329/taskmill?sslmode=disable
TEST_POSTGRES     ?= postgres:18-alpine

# The latest release: the version of taskmill that internal/compat requires.
# `make api` compares the API with it, and `make compat` runs its client and
# worker on the current schema. Each release updates it; see CONTRIBUTING.md.
RELEASE := $(shell awk '$$1 == "github.com/code19m/taskmill" { print $$2 }' internal/compat/go.mod)

# How long `make fuzz` runs each fuzz target.
FUZZTIME ?= 10s

BIN           := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOVULNCHECK   := $(BIN)/govulncheck-$(GOVULNCHECK_VERSION)
ACTIONLINT    := $(BIN)/actionlint-$(ACTIONLINT_VERSION)
GORELEASE     := $(BIN)/gorelease-$(GORELEASE_VERSION)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN { FS = ":.*## " } /^[a-z-]+:.*## / { printf "  %-12s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint test vuln compat ## Run every check that CI runs, except `make api`.

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format all Go files.
	$(GOLANGCI_LINT) fmt

.PHONY: lint
lint: $(GOLANGCI_LINT) $(ACTIONLINT) ## Run the Go and GitHub Actions linters.
	$(GOLANGCI_LINT) run
	cd internal/compat && $(GOLANGCI_LINT) run
	$(ACTIONLINT)

.PHONY: test
test: ## Run all tests with the race detector, in random order, and write coverage.out (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -shuffle=on -count=1 -coverprofile=coverage.out ./...

.PHONY: cover
cover: test ## Run all tests and open the coverage report in a browser (needs `make db`).
	go tool cover -html=coverage.out

# A block of code appears once for each test binary that covers it, so the
# report counts each block once, as covered if any test ran it.
.PHONY: cover-report
cover-report: ## Print the coverage of the last `make test` per package, in Markdown.
	@echo '### Coverage'
	@echo
	@echo '| Package | Coverage |'
	@echo '|---|---:|'
	@awk 'NR > 1 { n[$$1] = $$2; if ($$3 > 0) hit[$$1] = 1 } \
		END { \
			for (b in n) { p = b; sub(/\/[^\/]*:.*/, "", p); all[p] += n[b]; total += n[b]; \
				if (b in hit) { cov[p] += n[b]; covered += n[b] } } \
			for (p in all) printf "| `%s` | %.1f%% |\n", p, 100 * cov[p] / all[p] | "sort"; \
			close("sort"); \
			printf "| **Total** | **%.1f%%** |\n", 100 * covered / total \
		}' coverage.out

.PHONY: fuzz
fuzz: ## Run each fuzz target for FUZZTIME (needs `make db`).
	@go test -list '^Fuzz' ./... | awk '/^Fuzz/ { names[++n] = $$1 } /^ok/ { for (i = 1; i <= n; i++) print $$2, names[i]; n = 0 }' | \
	while read -r pkg name; do \
		echo "$$pkg $$name"; \
		TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -run='^$$' -fuzz="^$$name\$$" -fuzztime=$(FUZZTIME) $$pkg || exit 1; \
	done

.PHONY: api
api: $(GORELEASE) ## Compare the API with the latest release, and suggest the next version.
	$(GORELEASE) -base=$(RELEASE)

.PHONY: compat
compat: ## Run the latest release's client and worker on the current schema (needs `make db`).
	cd internal/compat && TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 ./...

.PHONY: bench
bench: ## Run the benchmarks and the chaos test against the test database (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go run ./internal/cmd/bench

.PHONY: vuln
vuln: $(GOVULNCHECK) ## Scan for known vulnerabilities.
	$(GOVULNCHECK) ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum, here and in internal/compat.
	go mod tidy
	cd internal/compat && go mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod or go.sum is not tidy, here or in internal/compat.
	go mod tidy -diff
	cd internal/compat && go mod tidy -diff

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
tools: $(GOLANGCI_LINT) $(GOVULNCHECK) $(ACTIONLINT) $(GORELEASE) ## Install the pinned development tools into ./bin.

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

$(GORELEASE):
	GOBIN=$(BIN) go install golang.org/x/exp/cmd/gorelease@$(GORELEASE_VERSION)
	mv $(BIN)/gorelease $@
