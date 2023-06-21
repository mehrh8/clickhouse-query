from .utils import apply_operator, get_sql

class ASMixin:
    def as_(self, as_):
        self._as = as_
        return self

    def get_as(self):
        return getattr(self, "_as", None)

class ArithmeticMixin:
    def __add__(self, other):
        from .arithmetic import Plus
        return Plus(self, other)

    def __sub__(self, other):
        from .arithmetic import Minus
        return Minus(self, other)
    
    def __mul__(self, other):
        from .arithmetic import Multiply
        return Multiply(self, other)
    
    def __truediv__(self, other):
        from .arithmetic import Divide
        return Divide(self, other)
    
    def __floordiv__(self, other):
        from .arithmetic import IntDiv
        # TODO: cast self to Float64
        return IntDiv(self, other)
    
    def __mod__(self, other):
        from .arithmetic import Modulo
        return Modulo(self, other)

    def __neg__(self):
        from .arithmetic import Negate
        return Negate(self)



class Func(ASMixin, ArithmeticMixin):
    function = None

    def __init__(self, *args):
        self.args = args

    def __sql__(self, *additional_args):
        sql =  "{func}({args})".format(
            func=self.get_function(), args=", ".join(map(get_sql, self.args + additional_args))
        )
        as_ = self.get_as()
        if as_ is not None:
            sql += " AS {}".format(as_)
        return sql

    def get_function(self):
        assert self.function is not None
        return self.function
    
class AggFunc(Func):
    suffix_order = ["If"]

    def __init__(self, *args, if_=None):
        self.if_ = if_
        super().__init__(*args)

    @property
    def suffix_dict(self):
        suffix_dict = {}
        if self.if_ is not None:
            suffix_dict["if"] = self.if_
        return suffix_dict

    def __sql__(self):
        additional_args = [
            self.suffix_dict[suffix_name] for suffix_name in self.suffix_order if self.suffix_dict[suffix_name] is not None
        ]
        return super().__sql__(*additional_args)

    def get_function(self):
        function = super().get_function()
        suffix_dict = self.suffix_dict
        return function + "".join(
            [suffix_name for suffix_name in self.suffix_order if suffix_name in suffix_dict]
        )

class _Func0Args(Func):
    def __init__(self):
        super().__init__()

class _Func1Args(Func):
    def __init__(self, arg):
        super().__init__(arg)

class _Func2Args(Func):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)

class _AggFunc0Args(AggFunc):
    def __init__(self, *, if_=None):
        super().__init__(if_=if_)

class _AggFunc1Args(AggFunc):
    def __init__(self, arg, *, if_=None):
        super().__init__(arg, if_=if_)

class _AggFunc2Args(AggFunc):
    def __init__(self, arg1, arg2, *, if_=None):
        super().__init__(arg1, arg2, if_=if_)

class _F:
    def __init__(self, arg):
        self.arg = arg
    
    def __sql__(self):
        return str(self.arg)
    
    def add(self, arg):
        return _F(self.arg + arg)


class F(ASMixin, ArithmeticMixin):
    def __init__(self, arg):
        self.arg = arg.split("__") if isinstance(arg, str) else arg

    def __sql__(self):
        field, *operators = self.arg
        field = _F(field)
        for op in operators:
            field = apply_operator(field, op)
        sql = get_sql(field)

        as_ = self.get_as()
        if as_ is not None:
            sql += " AS {}".format(as_)

        return sql

class Value(ASMixin):
    def __init__(self, arg):
        self.arg = arg
    
    def __sql__(self):
        return get_sql(self.arg)

class _Null:
    def __sql__(self):
        return "NULL"

NULL = _Null()

class Lambda(Func):
    function = "lambda"

    def __init__(self, x, expr):
        super().__init__(x, expr)