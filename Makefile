.PHONY: build test bench-data bench smoke

build:
	go build -o bin/gft ./cmd/gft

test:
	go vet ./...
	go test -race ./...

smoke: build
	bench/smoke.sh

# Synthetic 100k-file, 20k-commit monorepo (about a minute), imported into graft.
bench-data: build
	mkdir -p bench/work
	test -d bench/work/mono.git || python3 bench/gen_monorepo.py bench/work/mono.git
	git -C bench/work/mono.git config uploadpack.allowFilter true
	git -C bench/work/mono.git config uploadpack.allowAnySHA1InWant true
	test -d bench/work/mono.gft || bin/gft import --data bench/work/mono.gft --git bench/work/mono.git --ref refs/heads/main

bench: bench-data
	python3 bench/run.py --git bench/work/mono.git --gft bench/work/mono.gft
