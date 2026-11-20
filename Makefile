NIX ?= nix
DEV := $(NIX) develop --command
GO ?= $(DEV) go
GOFMT ?= $(DEV) gofmt
NPM ?= $(DEV) npm
CONFIG ?= config.example.yaml
BIN ?= nfvis
VERSION ?= dev
LDFLAGS := -X main.version=$(VERSION)
DURATION ?= 60s
TARGET ?= 1000
UDP ?= 127.0.0.1:20555
HTTP ?= http://127.0.0.1:18080
SEED_DB ?= data/seed.db
SEED_DAYS ?= 60
SEED_FLOWS_PER_HOUR ?= 1500
SEED_PATTERN ?= data/nfvis.db
SEED_CONFIG ?= config-seed.yaml
SEED_RESET ?= -reset

.DEFAULT_GOAL := help

.PHONY: help dev deps web build build-go test test-race vet fmt fmt-check check bench soak seed seed-db seed-clean run clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

dev: ## Enter the Nix development shell
	$(NIX) develop

deps: ## Install dashboard dependencies from package-lock.json
	$(NPM) --prefix web ci

web: ## Build the dashboard into internal/webui/dist
	$(NPM) --prefix web run build

build: web ## Build the nfvis binary (dashboard first)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/nfvis

build-go: ## Build the nfvis binary using the existing embedded assets
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/nfvis

test: ## Run Go unit and integration tests
	$(GO) test ./...

test-race: ## Run Go tests with the race detector
	$(GO) test -race ./...

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Format Go sources
	$(GOFMT) -w .

fmt-check: ## Fail if Go sources need formatting
	@unformatted="$$($(GOFMT) -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Go files need formatting:"; echo "$$unformatted"; exit 1; \
	fi

check: fmt-check vet test ## Run all fast checks

bench: ## Run decoder benchmarks
	$(GO) test ./internal/decoder -run '^$$' -bench . -benchmem

soak: ## Run a 60s end-to-end load test at 1000 flows/s
	GO="$(GO)" DURATION="$(DURATION)" TARGET="$(TARGET)" UDP="$(UDP)" HTTP="$(HTTP)" ./benchmark/soak.sh

seed: seed-db ## Generate sample data (alias for seed-db)

seed-db: build-go ## Seed SEED_DB with SEED_DAYS of traffic shaped like SEED_PATTERN
	$(GO) run ./benchmark/seed \
		-db $(SEED_DB) \
		-days $(SEED_DAYS) \
		-flows-per-hour $(SEED_FLOWS_PER_HOUR) \
		-pattern-db $(SEED_PATTERN) \
		-config $(CONFIG) \
		-config-out $(SEED_CONFIG) \
		$(SEED_RESET)
	@echo "start it with: ./$(BIN) --config $(SEED_CONFIG)"

seed-clean: ## Remove the database and config produced by seed-db
	rm -f $(SEED_DB) $(SEED_DB)-wal $(SEED_DB)-shm $(SEED_CONFIG)

run: build-go ## Build and run nfvis with CONFIG (default: config.example.yaml)
	./$(BIN) --config $(CONFIG)

clean: ## Remove build outputs (keeps embedded dashboard assets)
	rm -f $(BIN) web/tsconfig.tsbuildinfo
