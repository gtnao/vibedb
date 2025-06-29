.PHONY: check test build fmt clippy all commit

# Default target runs all checks
all: check build test fmt clippy
	@echo "✓ All checks passed!"

# Check for mod.rs files first
check:
	@./check_no_mod_rs.sh

build: check
	cargo build

test: check
	cargo test

fmt: check
	cargo fmt

clippy: check
	cargo clippy

# Convenient target for pre-commit checks
commit: all
	@echo "Ready to commit!"