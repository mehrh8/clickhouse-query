from clickhouse_query.utils import get_sql, ASMixin, ArithmeticMixin

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


class _Func0Args(Func):
    def __init__(self):
        super().__init__()

class _Func1Args(Func):
    def __init__(self, arg):
        super().__init__(arg)

class _Func2Args(Func):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)


class Lambda(Func):
    function = "lambda"

    def __init__(self, x, expr):
        super().__init__(x, expr)

class Distinct(Func):
    function = "distinct"
