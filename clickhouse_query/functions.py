"""
Function utilities for ClickHouse database queries.
"""

from clickhouse_query.core.functions import Function


class _FunctionBuilder:
    def __getattr__(self, item: str) -> Function:
        if item[-1] == "_":
            item = item[:-1]
        return Function(item)


functions = _FunctionBuilder()


__all__ = ["Function", "functions"]
