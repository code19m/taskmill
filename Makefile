# Development tools are pinned here and installed into ./bin, so every machine
# and CI run uses the same versions. They are deliberately kept out of go.mod:
# see docs/adr/0001-dependency-policy.md. Dependabot does not update them.
GOLANGCI_LINT_VERSION := v2.15.0
GOVULNCHECK_VERSION   := v1.9.0
ACTIONLINT_VERSION    := v1.7.12
GORELEASER_VERSION    := v2.19.0
SYFT_VERSION          := v1.55.0

# The repository's modules: the core, and the integrations that need modules
# of their own (docs/adr/0005-opentelemetry-in-a-module-of-its-own.md).
# Targets that check code run in each.
MODULES := . otel

# The core version that otel/go.mod requires. Its replace line builds the
# module against the core next to it; `make otel-release-check` builds it
# against this version instead, as its users do.
OTEL_CORE := $(shell awk '$$1 == "github.com/code19m/taskmill" && $$2 ~ /^v/ { print $$2; exit }' otel/go.mod)

# The database that `make db` starts and `make test` uses.
TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:54329/taskmill?sslmode=disable
TEST_POSTGRES     ?= postgres:18-alpine

BIN           := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOVULNCHECK   := $(BIN)/govulncheck-$(GOVULNCHECK_VERSION)
ACTIONLINT    := $(BIN)/actionlint-$(ACTIONLINT_VERSION)
GORELEASER    := $(BIN)/goreleaser-$(GORELEASER_VERSION)
# GoReleaser runs syft by name, so it gets a directory of its own on PATH.
SYFT          := $(BIN)/syft-$(SYFT_VERSION)/syft

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN { FS = ":.*## " } /^[a-z-]+:.*## / { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint test vuln otel-release-check ## Run every check that CI runs, except `make release-check`.

.PHONY: fmt
fmt: $(GOLANGCI_LINT) ## Format all Go files.
	@for m in $(MODULES); do (cd $$m && $(GOLANGCI_LINT) fmt --config $(CURDIR)/.golangci.yml) || exit 1; done

.PHONY: lint
lint: $(GOLANGCI_LINT) $(ACTIONLINT) ## Run the Go and GitHub Actions linters.
	@for m in $(MODULES); do (cd $$m && $(GOLANGCI_LINT) run --config $(CURDIR)/.golangci.yml) || exit 1; done
	$(ACTIONLINT)

.PHONY: test
test: ## Run all tests with the race detector, in random order (needs `make db`).
	@for m in $(MODULES); do \
		(cd $$m && TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -shuffle=on -count=1 ./...) || exit 1; \
	done

.PHONY: cover
cover: ## Run all tests and open the coverage report in a browser (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: bench
bench: ## Run the benchmarks and the chaos test against the test database (needs `make db`).
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go run ./internal/cmd/bench

# The check works on a copy of the module without its replace line. Until the
# required core version is released, it only says so, unless RELEASING is set:
# then, as before tagging the module, an unreleased core fails it.
.PHONY: otel-release-check
otel-release-check: ## Build and test otel against the core version it requires (needs `make db`).
	@tmp=$$(mktemp -d) && trap 'rm -rf "$$tmp"' EXIT && \
	if ! go mod download -json github.com/code19m/taskmill@$(OTEL_CORE) > "$$tmp/core.json"; then \
		if [ -z "$(RELEASING)" ] && grep -q 'unknown revision' "$$tmp/core.json"; then \
			echo "taskmill $(OTEL_CORE) is not released yet: release it before the otel module."; exit 0; \
		fi; \
		cat "$$tmp/core.json"; exit 1; \
	fi && \
	cp -R otel "$$tmp/otel" && cd "$$tmp/otel" && \
	go mod edit -dropreplace=github.com/code19m/taskmill && \
	export GOWORK=off GOFLAGS=-mod=mod && go vet ./... && \
	TASKMILL_TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 ./... && \
	echo "otel builds and passes its tests with taskmill $(OTEL_CORE)."

.PHONY: vuln
vuln: $(GOVULNCHECK) ## Scan for known vulnerabilities.
	@for m in $(MODULES); do (cd $$m && $(GOVULNCHECK) ./...) || exit 1; done

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum.
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

.PHONY: tidy-check
tidy-check: ## Fail if go.mod or go.sum is not tidy.
	@for m in $(MODULES); do (cd $$m && go mod tidy -diff) || exit 1; done

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

# The release targets read TAG only in the shell, quoted, since a tag name is
# not trusted. Make passes a TAG from the environment to the shell unchanged,
# but expands one given on its command line (`make TAG=...`) when it exports
# it, so the release workflow sets TAG in the environment.

.PHONY: release-check
release-check: $(GORELEASER) ## Check .goreleaser.yaml (the first run builds GoReleaser, which takes minutes).
	$(GORELEASER) check

# A snapshot takes its version from the latest tag of taskmill itself, not of
# a nested module such as otel/v0.1.0.
.PHONY: snapshot
snapshot: $(GORELEASER) $(SYFT) ## Build the release into ./dist and the image into Docker, publishing nothing.
	GORELEASER_CURRENT_TAG="$$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null)" \
		PATH='$(dir $(SYFT))':"$$PATH" $(GORELEASER) release --snapshot --clean

# A release's notes are its section of CHANGELOG.md, without the heading.
RELEASE_NOTES = test -n "$$TAG" || { echo 'set TAG to the version, such as TAG=v1.0.0' >&2; exit 1; }; \
	awk -v version="$${TAG\#v}" ' \
		/^\#\# \[/ { if (on) exit; on = index($$0, "\#\# [" version "]") == 1; next } \
		on && /^\[.*\]: / { exit } \
		on && (NF || notes != "") { notes = notes $$0 "\n" } \
		END { \
			if (notes == "") { print "CHANGELOG.md has no section for " version > "/dev/stderr"; exit 1 } \
			sub(/\n+$$/, "\n", notes); printf "%s", notes \
		}' CHANGELOG.md

.PHONY: release-notes
release-notes: ## Print the notes of TAG, such as TAG=v1.0.0, from CHANGELOG.md.
	@$(RELEASE_NOTES)

.PHONY: release
release: $(GORELEASER) $(SYFT) ## Publish TAG, such as TAG=v1.0.0, as a draft release (the release workflow runs this).
	notes=$$(mktemp) && { $(RELEASE_NOTES); } > "$$notes" && \
		GORELEASER_CURRENT_TAG="$$TAG" PATH='$(dir $(SYFT))':"$$PATH" $(GORELEASER) release --clean --release-notes="$$notes"

.PHONY: tools
tools: $(GOLANGCI_LINT) $(GOVULNCHECK) $(ACTIONLINT) $(GORELEASER) $(SYFT) ## Install the pinned development tools into ./bin.

.PHONY: clean
clean: ## Remove installed tools, generated reports, and built releases.
	rm -rf $(BIN) coverage.out dist

$(GOLANGCI_LINT):
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv $(BIN)/golangci-lint $@

$(GOVULNCHECK):
	GOBIN=$(BIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	mv $(BIN)/govulncheck $@

$(ACTIONLINT):
	GOBIN=$(BIN) go install github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION)
	mv $(BIN)/actionlint $@

$(GORELEASER):
	GOBIN=$(BIN) go install github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION)
	mv $(BIN)/goreleaser $@

# Syft names itself in every SBOM, so it is told its version.
$(SYFT):
	GOBIN=$(dir $@) go install -ldflags='-X main.version=$(SYFT_VERSION:v%=%)' github.com/anchore/syft/cmd/syft@$(SYFT_VERSION)
