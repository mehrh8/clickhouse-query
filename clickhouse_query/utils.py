class _GetAs:
    _u = 0

    @classmethod
    @property
    def as_(cls):
        cls._u += 1
        return "__U_{u}".format(u=cls._u)

    @classmethod
    def reset(cls):
        cls._u = 0


class _GetP:
    _p = 0

    @classmethod
    @property
    def p(cls):
        cls._p += 1
        return "__P_{u}".format(u=cls._p)

    @classmethod
    def reset(cls):
        cls._p = 0


def _get_sql(arg, sql_params):
    from clickhouse_query import mixins

    sql = arg.__sql__(sql_params=sql_params)
    if isinstance(arg, mixins.ASMixin):
        sql += arg.__asmixin__()
    return sql


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
    elif op == "range":
        raise NotImplementedError
        condition = functions.And(
            functions.GreaterOrEquals(field, value[0]),
            functions.LessOrEquals(field, value[1]),
        )
    elif op == "isnull":
        condition = functions.IsNull(field)
    elif op == "regex":
        raise NotImplementedError
    elif op == "iregex":
        raise NotImplementedError
    else:  # equals
        field = models.F(_field + [op])
        condition = functions.Equals(field, value)

    return condition


def _apply_operator(field, op):
    from clickhouse_query import functions

    if op[0] == "_":  # nested
        field = field.add("." + op[1:])
    elif op == "date":
        raise NotImplementedError
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
        return field.add("__" + op)


def _get_expression(v):
    from clickhouse_query import mixins, models

    if isinstance(v, mixins.ExpressionMixin):
        return v
    return models.Value(v)


def _get_field_or_expression(v):
    from clickhouse_query import models

    if isinstance(v, str):
        return models.F(v)
    return _get_expression(v)
