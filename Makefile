.DEFAULT_GOAL := help
.PHONY: help check check-fmt check-tidy check-stdlib db-down db-up fix lint setup-worktree test test-db vet

MODULE := github.com/hummingbee-ai/hummingbee-go
# Must match the version CI installs in .github/workflows/check.yml.
GOLANGCI_LINT_VERSION := v2.15.0
# The packages a firm imports. They may import the standard library only.
STDLIB_ONLY_PKGS := . ./hummingbeetest
# Default points at make db-up's Postgres, which trusts loopback logins, so
# it holds no password. Override to use another local server.
HUMMINGBEE_TEST_DATABASE_URL ?= postgres://postgres@127.0.0.1:5434/postgres?sslmode=disable
TEST_PKGS ?= ./...
GINKGO_LABEL_FILTER ?=
COMPOSE := docker compose -f compose.yaml
# Exported per target rather than inlined, so make never echoes a URL or puts
# it on a command line, and plain make test still skips the database specs.
test-db: export HUMMINGBEE_TEST_DATABASE_URL := $(HUMMINGBEE_TEST_DATABASE_URL)

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

check: ## gofmt, vet, lint, tidy drift, the standard-library guard and the specs. The only quality gate.
	$(MAKE) check-fmt
	$(MAKE) vet
	$(MAKE) lint
	$(MAKE) check-tidy
	$(MAKE) check-stdlib
	$(MAKE) test

check-fmt: ## Fail, naming the files, if any need gofmt
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed (make fix):"; echo "$$unformatted"; exit 1; fi

vet: ## go vet every package
	go vet ./...

lint: ## golangci-lint, pinned; config in .golangci.yml
	@installed=$$(golangci-lint version --short 2>/dev/null || echo none); \
	if [ "v$$installed" != "$(GOLANGCI_LINT_VERSION)" ] && [ "$$installed" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "golangci-lint $(GOLANGCI_LINT_VERSION) required, found $$installed"; exit 1; fi
	golangci-lint run ./...

# go.sum is absent until the module has a dependency, so the files are hashed
# together and a missing one hashes as empty.
check-tidy: ## Fail if go.mod or go.sum need go mod tidy
	@set -eu; before=$$(cat go.mod go.sum 2>/dev/null | shasum); go mod tidy; \
	after=$$(cat go.mod go.sum 2>/dev/null | shasum); \
	if [ "$$before" != "$$after" ]; then echo "go.mod or go.sum was not tidy. Review and commit the tidied files."; exit 1; fi

# go list -deps without -test leaves out test-only imports, so Ginkgo and
# Gomega do not count. Packages of this module are allowed, so a guarded
# package may import another one.
check-stdlib: ## Fail, naming them, if a guarded package imports outside the standard library
	@set -eu; deps=$$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' $(STDLIB_ONLY_PKGS)); \
	outside=$$(printf '%s\n' "$$deps" | grep -v -e '^$$' -e '^$(MODULE)$$' -e '^$(MODULE)/' || true); \
	if [ -n "$$outside" ]; then echo "$(STDLIB_ONLY_PKGS) may import the standard library only, but import:"; echo "$$outside"; exit 1; fi; \
	echo "$(STDLIB_ONLY_PKGS): standard library only"

test: ## The specs (Ginkgo and Gomega) with the race detector
	go test -race ./...

# -count=1 because go test's cache cannot see the database's state.
test-db: ## Run the specs against Postgres on TEST_PKGS (default ./...)
	go test -race -count=1 $(TEST_PKGS)$(if $(GINKGO_LABEL_FILTER), -ginkgo.label-filter="$(GINKGO_LABEL_FILTER)")

db-up: ## Start the local Postgres 14 on port 5434
	$(COMPOSE) up -d --wait

db-down: ## Stop the local Postgres, keeping its data
	$(COMPOSE) down

fix: ## Apply gofmt
	gofmt -w .

setup-worktree: ## Prepare a git worktree (.env and Go modules)
	@set -eu; \
	worktree_dir="$${WORKTREE_DIR:-$(CURDIR)}"; \
	repo_dir="$${REPO_DIR:-$$(dirname "$$(git rev-parse --path-format=absolute --git-common-dir)")}"; \
	if [ -f "$$repo_dir/.env" ] && [ "$${repo_dir%/}" != "$${worktree_dir%/}" ]; then \
		install -m 600 "$$repo_dir/.env" "$$worktree_dir/.env"; \
	fi; \
	if [ -f "$$worktree_dir/go.mod" ]; then \
		cd "$$worktree_dir" && go mod download; \
	fi
