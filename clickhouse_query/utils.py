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


class GetP:
    _p = 0

    @classmethod
    @property
    def p(cls):
        cls._p += 1
        return "__P_{u}".format(u=cls._p)

    @classmethod
    def reset(cls):
        cls._p = 0


def get_sql(arg, params):
    sql = arg.__sql__(params=params)
    if isinstance(arg, ASMixin):
        sql += arg.__asmixin__()
    return sql

def extract_q(item, value):
    from . import functions

    *_field, op = item.split("__")

    field = F(_field)
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
        condition = functions.Like(field, functions.Concat(Value("%"), value, Value("%")))
    elif op == "icontains":
        condition = functions.ILike(field, functions.Concat(Value("%"), value, Value("%")))
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
        condition = functions.Like(field, functions.Concat(value, Value("%")))
    elif op == "istartswith":
        condition = functions.ILike(field, functions.Concat(value, Value("%")))
    elif op == "endswith":
        condition = functions.Like(field, functions.Concat(Value("%"), value))
    elif op == "iendswith":
        condition = functions.ILike(field, functions.Concat(Value("%"), value))
    elif op == "range":
        raise NotImplementedError
        condition = functions.And(functions.GreaterOrEquals(field, value[0]), functions.LessOrEquals(field, value[1]))
    elif op == "isnull":
        condition = functions.IsNull(field)
    elif op == "regex":
        raise NotImplementedError
    elif op == "iregex":
        raise NotImplementedError
    else: # equals
        field = F(_field + [op])
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
        return field.add('__' + op)

class ASMixin:
    def as_(self, as_):
        self._as = as_
        return self

    def get_as(self):
        return getattr(self, "_as", None)
    
    def has_as(self):
        return self.get_as() is not None
    
    def __asmixin__(self):
        as_ = self.get_as()
        if as_ is not None:
            return " AS {as_}".format(as_=as_)
        return ""

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

class ExpressionMixin:
    pass

class JoinableMixin:
    def inner_join(self, other, on=None, using=None):
        from clickhouse_query.joins import InnerJoin
        if using is not None:
            using = [F(u) if isinstance(u, str) else get_expression(u) for u in using]
        return InnerJoin(self, other, on=on, using=using)

class _F:
    def __init__(self, arg):
        self.arg = arg
    
    def __sql__(self, params):
        return str(self.arg)
    
    def add(self, arg):
        return _F(self.arg + arg)


class F(ASMixin, ArithmeticMixin, ExpressionMixin):
    def __init__(self, arg):
        self.arg = arg.split("__") if isinstance(arg, str) else arg

    def __sql__(self, params):
        field, *operators = self.arg
        field = _F(field)
        for op in operators:
            field = apply_operator(field, op)
        sql = get_sql(field, params=params)

        return sql

class Value(ASMixin, ArithmeticMixin, ExpressionMixin):
    def __init__(self, arg):
        self.arg = arg
    
    def __sql__(self, params):
        p = GetP.p
        if isinstance(self.arg, str):
            params[p] = self.arg
            return '%({p})s'.format(p=p)
        if isinstance(self.arg, (int, float)):
            params[p] = self.arg
            return '%({p})f'.format(p=p)
        raise Exception("Value not Valid")

def get_expression(v):
    if isinstance(v, ExpressionMixin):
        return v
    return Value(v)

def get_expression_or_f(v):
    if isinstance(v, str):
        return F(v)
    return get_expression(v)