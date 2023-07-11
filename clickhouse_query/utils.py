class GetAs:
    _u = 0

    @classmethod
    @property
    def as_(cls):
        cls._u += 1
        return "__U_{u}".format(u=cls._u)
    
    @classmethod
    def reset(cls):
        cls._u = 0


def get_sql(arg):
    if isinstance(arg, str):
        return "'{}'".format(arg)
    if isinstance(arg, (int, float)):
        return str(arg)
    return arg.__sql__()

def extract_q(item, value):
    from . import functions
    from . import models

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
        condition = functions.Like(field, functions.Concat("%", value, "%"))
    elif op == "icontains":
        condition = functions.ILike(field, functions.Concat("%", value, "%"))
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
        condition = functions.Like(field, functions.Concat(value, "%"))
    elif op == "istartswith":
        condition = functions.ILike(field, functions.Concat(value, "%"))
    elif op == "endswith":
        condition = functions.Like(field, functions.Concat("%", value))
    elif op == "iendswith":
        condition = functions.ILike(field, functions.Concat("%", value))
    elif op == "range":
        condition = functions.And(functions.GreaterOrEquals(field, value[0]), functions.LessOrEquals(field, value[1]))
    elif op == "isnull":
        condition = functions.IsNull(field)
    elif op == "regex":
        raise NotImplementedError
    elif op == "iregex":
        raise NotImplementedError
    else: # equals
        field = models.F(_field + [op])
        condition = functions.Equals(field, value)

    return condition

def apply_operator(field, op):
    from . import functions

    if op[0] == "_": # nested
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
        raise Exception("Field operator is not valid.")
    return field

class ASMixin:
    def as_(self, as_):
        self._as = as_
        return self

    def get_as(self):
        return getattr(self, "_as", None)

class ArithmeticMixin:
    def __add__(self, other):
        from . import functions
        return functions.Plus(self, other)

    def __sub__(self, other):
        from . import functions
        return functions.Minus(self, other)
    
    def __mul__(self, other):
        from . import functions
        return functions.Multiply(self, other)
    
    def __truediv__(self, other):
        from . import functions
        return functions.Divide(self, other)
    
    def __floordiv__(self, other):
        from . import functions
        # TODO: cast self to Float64
        return functions.IntDiv(self, other)
    
    def __mod__(self, other):
        from . import functions
        return functions.Modulo(self, other)

    def __neg__(self):
        from . import functions
        return functions.Negate(self)


