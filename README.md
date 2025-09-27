# SQL RecSys - AI-powered Database Schema Review Service

Сервис для автоматического анализа и оптимизации схем баз данных с помощью LLM моделей через gRPC API.

## 🤖 Поддерживаемые модели

- **GigaChat** - Российская LLM модель от Сбера
- **OpenAI** - GPT модели (gpt-4, gpt-3.5-turbo, и др.)
- **Google Gemini** - Модели от Google (gemini-1.5-pro, gemini-1.5-flash)

## ⚡ Быстрый старт

### 1. Установка зависимостей

```bash
pip install poetry

poetry install
```

### 2. Настройка переменных окружения

Скопируйте файл с примером конфигурации:

```bash
cp .env.example .env
```

Отредактируйте `.env` файл:

**Для GigaChat:**

```bash
MODEL_TYPE=giga
API_KEY=your-gigachat-api-key
MODEL_NAME=GigaChat
```

**Для OpenAI:**

```bash
MODEL_TYPE=openai
OPENAI_API_KEY=your-openai-api-key
OPENAI_MODEL_NAME=gpt-4o-mini
```

**Для Google Gemini:**

```bash
MODEL_TYPE=gemini
GOOGLE_API_KEY=your-google-api-key
GEMINI_MODEL_NAME=gemini-1.5-flash
```

### 3. Генерация protobuf файлов

```bash
make gen-proto
```

### 4. Запуск сервера

```bash
make run-server
```

Сервер будет доступен на `localhost:50051`

## 📋 Обязательные переменные окружения

| Переменная   | Описание                                | Пример         |
| ------------ | --------------------------------------- | -------------- |
| `MODEL_TYPE` | Тип модели (`giga`, `openai`, `gemini`) | `giga`         |
| `API_KEY`    | API ключ для выбранной модели           | `your-api-key` |

### Дополнительные переменные

| Переменная    | Описание                        | По умолчанию |
| ------------- | ------------------------------- | ------------ |
| `GRPC_PORT`   | Порт gRPC сервера               | `50051`      |
| `MAX_TOKENS`  | Максимальное количество токенов | `2048`       |
| `TEMPERATURE` | Температура модели (0.0-1.0)    | `0.1`        |
| `LOG_LEVEL`   | Уровень логирования             | `INFO`       |

## 🛠️ Полезные команды

```bash
make gen-proto
make run-server
make ci
```

## 📡 gRPC API

Сервис предоставляет единственный метод:

```protobuf
rpc ReviewSchema(ReviewSchemaRequest) returns (ReviewSchemaResponse);
```

### Пример запроса:

```json
{
  "url": "trino://user:password@localhost:8080/catalog",
  "ddl": [{ "statement": "CREATE TABLE users (id INT, name VARCHAR(100))" }],
  "queries": [
    {
      "query_id": "q1",
      "query": "SELECT * FROM users WHERE id = 1",
      "runquantity": 1000,
      "executiontime": 150
    }
  ]
}
```

### Пример ответа:

```json
{
  "success": true,
  "message": "Schema review completed successfully",
  "ddl": [{ "statement": "CREATE INDEX idx_users_id ON users(id)" }],
  "migrations": [
    { "statement": "ALTER TABLE users ADD INDEX idx_users_id (id)" }
  ],
  "queries": [
    {
      "query_id": "q1",
      "query": "SELECT id, name FROM users WHERE id = 1"
    }
  ]
}
```

## 🏗️ Архитектура

```
src/
├── api/grpc/           # gRPC сервер и точка входа
├── application/        # Бизнес-логика
│   ├── agents/         # Агенты для анализа
│   ├── handlers/       # Обработчики сообщений
│   ├── services/       # Сервисы
│   └── workflows/      # Рабочие процессы
├── core/              # Ядро системы
│   ├── abstractions/   # Интерфейсы
│   ├── config.py       # Конфигурация
│   ├── factories/      # Фабрики сервисов
│   └── models/         # Модели данных
└── generated/         # Сгенерированные protobuf файлы
```

## 🔧 Разработка

Для добавления поддержки новых LLM моделей:

1. Добавьте зависимость в `pyproject.toml`
2. Расширьте `ServiceFactory._create_llm_model()`
3. Добавьте соответствующие переменные в `config.py`
4. Обновите валидацию в `ServiceFactory.validate_configuration()`

## 📝 Лицензия

MIT License
