class UIDGenerator:
    def __init__(self, prefix=None):
        if prefix is None:
            prefix = ""
        self.prefix = prefix
        self.prev_num = 0

    def get(self):
        self.prev_num += 1
        return self.prefix + str(self.prev_num)


def get_sql(arg, uid_generator=None):
    from clickhouse_query import mixins

    if uid_generator is None:
        uid_generator = UIDGenerator(prefix="__U_")

    sql, sql_params = arg.__sql__(uid_generator=uid_generator)

    if isinstance(arg, mixins.ASMixin):
        sql += arg.__asmixin__(uid_generator=uid_generator)

    return sql, sql_params


def _extract_condition(item, value):
    from clickhouse_query import functions, models

    *_field, op = item.split("__")

    field = models.F(_field)
    if op == "exact":
        if value is None:
            condition = functions.IsNull(field)
        else:
            condition = functions.Equals(field, value)
    elif op == "iexact":
        if value is None:
            condition = functions.IsNull(field)
        else:
            condition = functions.ILike(field, value)
    elif op == "contains":
        condition = functions.Like(field, functions.Concat(models.Value("%"), value, models.Value("%")))
    elif op == "icontains":
        condition = functions.ILike(field, functions.Concat(models.Value("%"), value, models.Value("%")))
    elif op == "in":
        condition = functions.In(field, value)
    elif op == "gt":
        condition = functions.Greater(field, value)
    elif op == "gte":
        condition = functions.GreaterOrEquals(field, value)
    elif op == "lt":
        condition = functions.Less(field, value)
    elif op == "lte":
        condition = functions.LessOrEquals(field, value)
    elif op == "startswith":
        condition = functions.Like(field, functions.Concat(value, models.Value("%")))
    elif op == "istartswith":
        condition = functions.ILike(field, functions.Concat(value, models.Value("%")))
    elif op == "endswith":
        condition = functions.Like(field, functions.Concat(models.Value("%"), value))
    elif op == "iendswith":
        condition = functions.ILike(field, functions.Concat(models.Value("%"), value))
    elif op == "isnull":
        condition = functions.IsNull(field)
    else:  # equals
        field = models.F(_field + [op])
        condition = functions.Equals(field, value)

    return condition


def _apply_operator(field, op):
    from clickhouse_query import functions

    if op[0] == "_":  # nested
        field = field._arg_extend("." + op[1:])
    elif op == "year":
        field = functions.ToYear(field)
    elif op == "month":
        field = functions.ToMonth(field)
    elif op == "day":
        field = functions.ToDayOfYear(field)
    elif op == "week":
        field = functions.ToWeek(field)
    elif op == "week_day":
        field = functions.ToDayOfWeek(field)
    elif op == "quarter":
        field = functions.ToQuarter(field)
    elif op == "time":
        field = functions.ToTime(field)
    elif op == "hour":
        field = functions.ToHour(field)
    elif op == "minute":
        field = functions.ToMinute(field)
    elif op == "second":
        field = functions.ToSecond(field)
    else:
        # not changed
        return field._arg_extend("__" + op)

    return field


def _get_expression(v):
    from clickhouse_query import models

    if hasattr(v, "__sql__"):
        return v
    return models.Value(v)


def _get_field_or_expression(v):
    from clickhouse_query import models

    if isinstance(v, str):
        return models.F(v)
    return _get_expression(v)
