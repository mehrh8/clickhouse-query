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

    *_field, op = item.split("__")

    field = models.F(_field)
    if op == "exact":
        if value is None:
            condition = functions.isNull(field)
        else:
            condition = functions.equals(field, value)
    elif op == "iexact":
        if value is None:
            condition = functions.isNull(field)
        else:
            condition = functions.iLike(field, value)
    elif op == "contains":
        condition = functions.like(field, functions.concat(models.Value("%"), value, models.Value("%")))
    elif op == "icontains":
        condition = functions.iLike(field, functions.concat(models.Value("%"), value, models.Value("%")))
    elif op == "in":
        condition = functions.in_(field, value)
    elif op == "notin":
        condition = functions.notIn(field, value)
    elif op == "gt":
        condition = functions.greater(field, value)
    elif op == "gte":
        condition = functions.greaterOrEquals(field, value)
    elif op == "lt":
        condition = functions.less(field, value)
    elif op == "lte":
        condition = functions.lessOrEquals(field, value)
    elif op == "startswith":
        condition = functions.like(field, functions.concat(value, models.Value("%")))
    elif op == "istartswith":
        condition = functions.iLike(field, functions.concat(value, models.Value("%")))
    elif op == "endswith":
        condition = functions.like(field, functions.concat(models.Value("%"), value))
    elif op == "iendswith":
        condition = functions.iLike(field, functions.concat(models.Value("%"), value))
    elif op == "isnull":
        if value.arg is True:
            condition = functions.isNull(field)
        elif value.arg is False:
            condition = functions.isNotNull(field)
        else:
            raise ValueError("isnull value should be True or False")
    else:  # equals
        field = models.F(_field + [op])
        condition = functions.equals(field, value)

    return condition


def _apply_operator(field, op):
    from clickhouse_query import functions

    if op[0] == "_":  # nested
        field = field._arg_extend("." + op[1:])
    elif op == "year":
        field = functions.toYear(field)
    elif op == "month":
        field = functions.toMonth(field)
    elif op == "day":
        field = functions.toDayOfYear(field)
    elif op == "week":
        field = functions.toWeek(field)
    elif op == "week_day":
        field = functions.toDayOfWeek(field)
    elif op == "quarter":
        field = functions.toQuarter(field)
    elif op == "time":
        field = functions.toTime(field)
    elif op == "hour":
        field = functions.toHour(field)
    elif op == "minute":
        field = functions.toMinute(field)
    elif op == "second":
        field = functions.toSecond(field)
    else:
        # not changed
        return field._arg_extend("__" + op)

    return field


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
