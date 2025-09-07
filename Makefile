# Makefile for CS6450 KVS Lab

# Variables
BIN_DIR := bin
SERVER_BINARY := $(BIN_DIR)/kvsserver
CLIENT_BINARY := $(BIN_DIR)/kvsclient
SERVER_PKG := ./kvs/server
CLIENT_PKG := ./kvs/client

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod
GOFMT := $(GOCMD) fmt

#protoc parameters
PROTOC := protoc
PROTOC_GO_OUT := .
PROTOC_GO_OPT := paths=source_relative
PROTOC_GO_GRPC_OUT := .
PROTOC_GO_GRPC_OPT := paths=source_relative
PROTOC_OUT := rpc.proto

# Build flags
BUILD_FLAGS := -v # print package names as they are compiled

.PHONY: help build build-server build-client run-server run-client test clean fmt vet deps tidy all

all: build

help:
	@echo 'Usage: make <target>'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: build-server build-client ## Build both server and client binaries (default)

build-server: build-proto $(SERVER_BINARY) ## Build the KVS server binary

build-client: build-proto $(CLIENT_BINARY) ## Build the KVS client binary

build-proto:
	@echo "Building protobuf..."
	$(PROOTC) --go_out=$(PROTOC_GO_OUT) --go_opt=$(PROTOC_GO_OPT) --go-grpc_out=$(PROTOC_GO_GRPC_OUT) --go-grpc_opt=$(PROTOC_GO_GRPC_OPT) $(PROTOC_OUT)

$(SERVER_BINARY): $(BIN_DIR) $(wildcard kvs/server/*.go) $(wildcard kvs/*.go)
	@echo "Building KVS server..."
	$(GOBUILD) $(BUILD_FLAGS) -o $(SERVER_BINARY) $(SERVER_PKG)

$(CLIENT_BINARY): $(BIN_DIR) $(wildcard kvs/client/*.go) $(wildcard kvs/*.go)
	@echo "Building KVS client..."
	$(GOBUILD) $(BUILD_FLAGS) -o $(CLIENT_BINARY) $(CLIENT_PKG)

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

run-server: build-server ## Build and run the KVS server (default port 8080)
	@echo "Starting KVS server on port 8080..."
	./$(SERVER_BINARY)

run-server-port: build-server ## Build and run the KVS server on custom port (usage: make run-server-port PORT=8081)
	@echo "Starting KVS server on port $(PORT)..."
	./$(SERVER_BINARY) -port=$(PORT)

run-client: build-client ## Build and run the KVS client
	@echo "Running KVS client..."
	./$(CLIENT_BINARY)

test: ## Run tests
	@echo "Running tests..."
	$(GOTEST) -v ./...

fmt: ## Format Go code
	@echo "Formatting code..."
	$(GOFMT) ./...

vet: ## Run go vet
	@echo "Running go vet..."
	$(GOCMD) vet ./...

lint: ## Run golint (requires golint to be installed)
	@echo "Running golint..."
	@if command -v golint >/dev/null 2>&1; then \
		golint ./...; \
	else \
		echo "golint not installed. Install with: go install golang.org/x/lint/golint@latest"; \
	fi

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	$(GOGET) -d ./...

tidy: ## Tidy up go.mod
	@echo "Tidying go.mod..."
	$(GOMOD) tidy

clean: ## Clean build artifacts
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BIN_DIR)

# Development helpers
check: fmt vet test ## Run format, vet, and test

rebuild: clean build ## Clean and rebuild everything

