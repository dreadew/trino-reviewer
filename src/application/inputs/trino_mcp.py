from pydantic import BaseModel, Field


class TrinoMCPQueryInput(BaseModel):
    """Входные параметры для MCP Trino tool."""

    query: str = Field(description="SQL запрос для выполнения через Trino MCP сервер")
