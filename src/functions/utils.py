def get_sql(arg):
    if isinstance(arg, str):
        return "'{}'".format(arg)
    if isinstance(arg, (int, float)):
        return str(arg)
    return arg.__sql__()

def extract_q(item, value):
    from .. import functions as func

    *_field, op = item.split("__")

    field = func.F(_field)
    if op == "exact":
        if value is None:
            condition = func.IsNull(field)
        else:
            condition = func.Equals(field, value)
    elif op == "iexact":
        if value is None:
            condition = func.IsNull(field)
        else:
            condition = func.ILike(field, value)
    elif op == "contains":
        condition = func.Like(field, func.Concat("%", value, "%"))
    elif op == "icontains":
        condition = func.ILike(field, func.Concat("%", value, "%"))
    elif op == "in":
        condition = func.In(field, value)
    elif op == "gt":
        condition = func.Greater(field, value)
    elif op == "gte":
        condition = func.GreaterOrEquals(field, value)
    elif op == "lt":
        condition = func.Less(field, value)
    elif op == "lte":
        condition = func.LessOrEquals(field, value)
    elif op == "startswith":
        condition = func.Like(field, func.Concat(value, "%"))
    elif op == "istartswith":
        condition = func.ILike(field, func.Concat(value, "%"))
    elif op == "endswith":
        condition = func.Like(field, func.Concat("%", value))
    elif op == "iendswith":
        condition = func.ILike(field, func.Concat("%", value))
    elif op == "range":
        condition = func.And(func.GreaterOrEquals(field, value[0]), func.LessOrEquals(field, value[1]))
    elif op == "isnull":
        condition = func.IsNull(field)
    elif op == "regex":
        raise NotImplementedError
    elif op == "iregex":
        raise NotImplementedError
    else: # equals
        field = func.F(_field + [op])
        condition = func.Equals(field, value)

    return condition

def apply_operator(field, op):
    from .. import functions as func

    if op[0] == "_": # nested
        field = field.add("." + op[1:])
    elif op == "date":
        raise NotImplementedError
    elif op == "year":
        field = func.ToYear(field)
    elif op == "month":
        field = func.ToMonth(field)
    elif op == "day":
        field = func.ToDayOfYear(field)
    elif op == "week":
        field = func.ToWeek(field)
    elif op == "week_day":
        field = func.ToDayOfWeek(field)
    elif op == "quarter":
        field = func.ToQuarter(field)
    elif op == "time":
        field = func.ToTime(field)
    elif op == "hour":
        field = func.ToHour(field)
    elif op == "minute":
        field = func.ToMinute(field)
    elif op == "second":
        field = func.ToSecond(field)
    else:
        raise Exception("Field operator is not valid.")
    return field