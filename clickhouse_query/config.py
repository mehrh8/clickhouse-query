from clickhouse_query import functions
from clickhouse_query.core.expressions import Value

__all__ = ["INLINE_CONDITIONS", "INLINE_FUNCTIONS"]


def _exact(field, value):
    if value is None:
        return functions.isNull(field)
    else:
        return functions.equals(field, value)


def _iexact(field, value):
    if value is None:
        return functions.isNull(field)
    else:
        return functions.iLike(field, value)


def _contains(field, value):
    return functions.like(field, functions.concat(Value("%"), value, Value("%")))


def _icontains(field, value):
    return functions.iLike(field, functions.concat(Value("%"), value, Value("%")))


def _isnull(field, value):
    if value.arg is True:
        return functions.isNull(field)
    elif value.arg is False:
        return functions.isNotNull(field)
    else:
        raise ValueError("isnull value should be True or False")


INLINE_CONDITIONS = {
    "exact": _exact,
    "iexact": _iexact,
    "contains": _contains,
    "icontains": _icontains,
    "in": functions.in_,
    "notin": functions.notIn,
    "gt": functions.greater,
    "gte": functions.greaterOrEquals,
    "lt": functions.less,
    "lte": functions.lessOrEquals,
    "startswith": lambda field, value: functions.like(field, functions.concat(value, Value("%"))),
    "istartswith": lambda field, value: functions.iLike(field, functions.concat(value, Value("%"))),
    "endswith": lambda field, value: functions.like(field, functions.concat(Value("%"), value)),
    "iendswith": lambda field, value: functions.iLike(field, functions.concat(Value("%"), value)),
    "isnull": _isnull,
}


INLINE_FUNCTIONS = {
    "year": functions.toYear,
    "month": functions.toMonth,
    "day": functions.toDayOfYear,
    "week": functions.toWeek,
    "week_day": functions.toDayOfWeek,
    "quarter": functions.toQuarter,
    "time": functions.toTime,
    "hour": functions.toHour,
    "minute": functions.toMinute,
    "second": functions.toSecond,
}
