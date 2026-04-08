.PHONY: all build test e2e proto clean docker-build compose-up compose-down

SERVICES := subscriber-registry config-distributor ingestion-router queue-writer \
            subscriber-queue-service webhook-dispatcher stream-gateway admin-api
SYNTHETIC := event-generator webhook-subscriber stream-subscriber
TOOLS := producer-ui consumer-ui

all: build

build:
	@for svc in $(SERVICES); do \
		echo "Building $$svc..."; \
		go build -o bin/$$svc ./services/$$svc; \
	done
	@for syn in $(SYNTHETIC); do \
		echo "Building $$syn..."; \
		go build -o bin/$$syn ./synthetic/$$syn; \
	done
	@for tool in $(TOOLS); do \
		echo "Building $$tool..."; \
		go build -o bin/$$tool ./tools/$$tool; \
	done

test:
	go test ./internal/... ./services/... -v -count=1

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/proto/queue/v1/queue.proto \
		internal/proto/config/v1/config.proto

clean:
	rm -rf bin/

docker-build:
	@for svc in $(SERVICES); do \
		echo "Building Docker image for $$svc..."; \
		docker build -t event-gateway/$$svc -f services/$$svc/Dockerfile .; \
	done
	@for syn in $(SYNTHETIC); do \
		echo "Building Docker image for $$syn..."; \
		docker build -t event-gateway/$$syn -f synthetic/$$syn/Dockerfile .; \
	done

compose-up:
	docker compose -f deploy/docker-compose/docker-compose.yml up --build -d

compose-down:
	docker compose -f deploy/docker-compose/docker-compose.yml down -v

compose-synthetic:
	docker compose -f deploy/docker-compose/docker-compose.yml --profile synthetic up --build -d

compose-tools:
	docker compose -f deploy/docker-compose/docker-compose.yml --profile tools up --build -d

e2e:
	@echo "Running E2E tests (requires: make compose-up && make compose-synthetic)"
	go test ./tests/e2e/... -v -count=1 -timeout 600s
