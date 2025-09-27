from dataclasses import dataclass


@dataclass
class DDLStatement:
    """DDL запрос"""

    statement: str


@dataclass
class Query:
    """Данные о запросе"""

    query_id: str
    query: str
    runquantity: int
    executiontime: int
