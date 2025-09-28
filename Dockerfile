FROM python:3.11-slim AS builder

# установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# установка poetry
RUN pip install --no-cache-dir poetry

# настройка poetry
RUN poetry config virtualenvs.create false

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-root --no-interaction --no-ansi

COPY . .

# Генерируем protobuf файлы
RUN mkdir -p src/generated && touch src/generated/__init__.py
RUN poetry run python -m grpc_tools.protoc \
    -I proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/src/generated/schema_review.proto

FROM python:3.11-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# установка poetry в runtime
RUN pip install --no-cache-dir poetry

# настройка poetry
RUN poetry config virtualenvs.create false

COPY --from=builder /usr/local /usr/local
COPY --from=builder /app /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

CMD ["poetry", "run", "python", "-m", "src.api.grpc.run_server"]
