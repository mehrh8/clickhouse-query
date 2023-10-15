from clickhouse_query import mixins, utils


class Func(mixins.ASMixin, mixins.ArithmeticMixin, mixins.ExpressionMixin):
    function = None

    def __init__(self, *args):
        self.args = [utils._get_expression(arg) for arg in args]

    def __sql__(self, *additional_args, sql_params):
        sql = "{func}({args})".format(
            func=self.get_function(sql_params=sql_params),
            args=", ".join(
                [utils._get_sql(a, sql_params=sql_params) for a in self.args + list(additional_args)]
            ),
        )
        return sql

    def get_function(self, sql_params):
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
