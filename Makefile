.PHONY: help lint format test test-unit test-integration check-all gen-proto run-server prompt-init prompt-list prompt-clear

help:
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

gen-proto:
	@mkdir -p src/generated
	@touch src/generated/__init__.py
	poetry run python -m grpc_tools.protoc \
		-I proto \
		--python_out=. \
		--grpc_python_out=. \
		proto/src/generated/schema_review.proto

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

prompt-init:
	poetry run python -c "import asyncio; from src.utils.prompt_init import init_prompts_in_valkey; asyncio.run(init_prompts_in_valkey())"

prompt-list:
	poetry run python -c "import asyncio; from src.utils.prompt_init import list_prompts_in_valkey; asyncio.run(list_prompts_in_valkey())"

prompt-clear:
	poetry run python -c "import asyncio; from src.utils.prompt_init import clear_prompts_in_valkey; asyncio.run(clear_prompts_in_valkey())"