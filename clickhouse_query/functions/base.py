from clickhouse_query import mixins, utils


class Function(mixins.ASMixin, mixins.ArithmeticMixin):
    function = None

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __sql__(self, uid_generator):
        sql_params = {}

        function_sql, function_params = self.__clickhouse_query_function_sql__(uid_generator=uid_generator)
        sql_params.update(function_params)

        function_args_sqls_list, function_args_params = self.__clickhouse_query_function_args_sqls__(
            uid_generator=uid_generator
        )
        sql_params.update(function_args_params)

        sql = "{func}({args})".format(func=function_sql, args=", ".join(function_args_sqls_list))
        return sql, sql_params

    def __clickhouse_query_function_sql__(self, *, uid_generator):
        return self.function, {}

    def __clickhouse_query_function_args_sqls__(self, *, uid_generator):
        args_sqls_list = []
        sql_params = {}
        for item in self.args:
            sql, params = utils.get_sql(item, uid_generator=uid_generator)
            args_sqls_list.append(sql)
            sql_params.update(params)
        return args_sqls_list, sql_params


class _Function0Args(Function):
    def __init__(self):
        super().__init__()


class _Function1Args(Function):
    def __init__(self, arg):
        super().__init__(arg)


class _Function2Args(Function):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)


class Lambda(Function):
    function = "lambda"

    def __init__(self, x, expr):
        super().__init__(x, expr)


class Distinct(Function):
    function = "distinct"
