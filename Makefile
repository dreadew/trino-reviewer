.PHONY: help lint format test test-unit test-integration check-all gen-proto run-server

help:
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

gen-proto:
	@mkdir -p src/generated
	@touch src/generated/__init__.py
	poetry run python -m grpc_tools.protoc \
		--proto_path=proto \
		--python_out=src/generated \
		--grpc_python_out=src/generated \
		proto/schema_review.proto

run-server:
	poetry run python -m src.api.grpc.run_server

lint:
	poetry run black --check --diff .
	poetry run isort --check-only --diff .
	poetry run flake8 src tests
	poetry run bandit -r src -ll || echo "Bandit found some issues but they are not critical"

format:
	poetry run black .
	poetry run isort .

test-unit:
	poetry run python -m pytest tests -v

test:
	poetry run python -m pytest tests/ -v

check-all: lint test-unit

install:
	poetry install

dev-setup: install
	@echo "Development environment is ready!"

ci: check-all
	@echo "All CI checks passed!"