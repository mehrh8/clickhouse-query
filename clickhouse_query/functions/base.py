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


class AggregationFunction(Function):
    combinators_order = ["If"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.combinator_dict = {}

    def __clickhouse_query_function_sql__(self, *, uid_generator):
        function_sql, function_sql_params = super().__clickhouse_query_function_sql__(
            uid_generator=uid_generator
        )
        for combinator in self.combinators_order:
            if combinator in self.combinator_dict:
                function_sql += "{combinator}".format(combinator=combinator)

        return function_sql, function_sql_params

    def __clickhouse_query_function_args_sqls__(self, *, uid_generator):
        args_sqls_list, sql_params = super().__clickhouse_query_function_args_sqls__(
            uid_generator=uid_generator
        )
        for combinator in self.combinators_order:
            if combinator in self.combinator_dict:
                combinator_function_args = self.combinator_dict[combinator].get("function_args", [])
                for arg in combinator_function_args:
                    expression = utils._get_expression(arg)
                    sql, params = utils.get_sql(expression, uid_generator=uid_generator)
                    args_sqls_list.append(sql)
                    sql_params.update(params)
        return args_sqls_list, sql_params

    def if_(self, if_):
        self.combinator_dict["If"] = {"function_args": [if_]}
        return self


class AggregationFunctionWithParams(AggregationFunction):
    def __init__(self, *args, agg_params=None):
        super().__init__(*args)
        self.agg_params = agg_params

    def __clickhouse_query_function_sql__(self, *, uid_generator):
        function_sql, function_sql_params = super().__clickhouse_query_function_sql__(
            uid_generator=uid_generator
        )
        agg_params_sql_list = []
        if self.agg_params is not None:
            for agg_param in self.agg_params:
                expression = utils._get_expression(agg_param)
                sql, params = utils.get_sql(expression, uid_generator=uid_generator)
                agg_params_sql_list.append(sql)
                function_sql_params.update(params)

        function_sql = function_sql + "({agg_params})".format(agg_params=", ".join(agg_params_sql_list))
        return function_sql, function_sql_params


class _AggregationFunction0Args(AggregationFunction):
    def __init__(self):
        super().__init__()


class _AggregationFunction1Args(AggregationFunction):
    def __init__(self, arg):
        super().__init__(arg)


class _AggregationFunction2Args(AggregationFunction):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)


class Lambda(Function):
    function = "lambda"

    def __init__(self, x, expr):
        super().__init__(x, expr)


class Distinct(Function):
    function = "distinct"
