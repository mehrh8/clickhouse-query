from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_query.models import Value


class UIDGenerator:
    def __init__(self, prefix=None):
        if prefix is None:
            prefix = ""
        self.prefix = prefix
        self.prev_num = 0

    def get(self):
        self.prev_num += 1
        return self.prefix + str(self.prev_num)


def get_sql(arg, uid_generator: UIDGenerator | None = None):
    from clickhouse_query import mixins

    if uid_generator is None:
        uid_generator = UIDGenerator(prefix="__U_")

    sql, sql_params = arg.__sql__(uid_generator=uid_generator)

    if isinstance(arg, mixins.ASMixin):
        sql += arg.__asmixin__(uid_generator=uid_generator)

    return sql, sql_params


def _extract_condition(item: str, value: "Value"):
    from clickhouse_query import functions, models
    from clickhouse_query.config import INLINE_CONDITIONS

    *_field, op = item.split("__")

    field = models.F(_field)

    if op in INLINE_CONDITIONS:
        return INLINE_CONDITIONS[op](field, value)

    field = models.F(_field + [op])
    return functions.equals(field, value)


def _apply_operator(field, op):
    from clickhouse_query.config import INLINE_FUNCTIONS

    if op[0] == "_":  # nested operator
        return field._arg_extend("." + op[1:])
    elif op in INLINE_FUNCTIONS:
        return INLINE_FUNCTIONS[op](field)

    return field._arg_extend("__" + op)


def get_expression(v, str_is_field=False):
    from clickhouse_query import models

    if str_is_field and isinstance(v, str):
        return models.F(v)

    if hasattr(v, "__sql__"):
        return v

    return models.Value(v)


def is_iterable(obj):
    try:
        iter(obj)
        return True
    except TypeError:
        return False
