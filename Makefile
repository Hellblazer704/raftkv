GO ?= go

.PHONY: build test race chaos bench cover docker-up docker-down vet

build:
	$(GO) build -o bin/raftkvd ./cmd/raftkvd
	$(GO) build -o bin/raftkv-cli ./cmd/raftkv-cli
	$(GO) build -o bin/raftkv-bench ./cmd/raftkv-bench

vet:
	$(GO) vet ./...

test:
	$(GO) test -count=1 ./...

race:
	$(GO) test -race -short -count=1 ./...

# 50 seeded fault schedules locally; CI runs 1000.
chaos:
	RAFTKV_CHAOS_SEEDS=50 $(GO) test ./chaos -run TestRandomFaultSchedules -count=1 -parallel 8 -timeout 60m

bench: build
	./bin/raftkv-bench -mode ycsb -workload a -nodes 3 -clients 32 -duration 8s
	./bin/raftkv-bench -mode failover -nodes 5 -rounds 20
	./bin/raftkv-bench -mode recovery -nodes 3 -records 8000 -valuesize 256

cover:
	RAFTKV_CHAOS_SEEDS=6 $(GO) test -count=1 -coverprofile=coverage.out -coverpkg=./raft/,./raft/wal/ ./raft/... ./chaos/ ./kv/ ./shard/
	$(GO) tool cover -func=coverage.out | tail -1

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
