"""
Expression classes for ClickHouse query building.
"""

import datetime
from zoneinfo import ZoneInfo

from clickhouse_query import mixins, utils
from clickhouse_query.core.functions import Function


class _Null:
    """Represents a NULL value in SQL."""

    def __sql__(self, *, uid_generator):
        return "NULL", {}


NULL = _Null()


class _F:
    """Internal helper class for field references."""

    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, *, uid_generator):
        return str(self.arg), {}

    def _arg_extend(self, arg):
        return _F(self.arg + arg)


class F(mixins.ASMixin, mixins.ArithmeticMixin):
    """Field reference for use in queries."""

    def __init__(self, arg):
        self.arg = arg.split("__") if isinstance(arg, str) else arg

    def __sql__(self, *, uid_generator):
        field, *operators = self.arg
        field = _F(field)
        for op in operators:
            field = utils._apply_operator(field, op)

        sql, sql_params = utils.get_sql(field, uid_generator=uid_generator)
        return sql, sql_params


def _get_value_sql(arg):
    """Convert Python value to SQL representation."""
    if isinstance(arg, str):
        return "%({uid})s", arg

    if isinstance(arg, (int, float)):
        return "%({uid})f", arg

    if isinstance(arg, datetime.datetime):
        return "%({uid})s", arg.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S.%f")

    if utils.is_iterable(arg):
        new_arg = tuple(_get_value_sql(a)[1] for a in arg)
        return "%({uid})s", new_arg

    raise Exception("Value not Valid", arg)


class Value(mixins.ASMixin, mixins.ArithmeticMixin):
    """Represents a literal value in SQL."""

    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, *, uid_generator):
        uid = uid_generator.get()
        value_sql, value_param = _get_value_sql(self.arg)
        return value_sql.format(uid=uid), {uid: value_param}


class Q(Function):
    """Query expression that can be used to encapsulate a set of keyword arguments."""

    def __init__(self, *args, _connector="and", **kwargs) -> None:
        args = list(args)

        if len(args) + len(kwargs) == 0:
            raise ValueError("At least one argument is required")

        if len(args) + len(kwargs) == 1:
            _connector = ""

        for item, value in kwargs.items():
            expression = utils.get_expression(value)
            condition = utils._extract_condition(item, expression)
            args.append(condition)

        super().__init__(_connector)
        self(*args)


__all__ = [
    "F",
    "Q",
    "Value",
    "NULL",
]
