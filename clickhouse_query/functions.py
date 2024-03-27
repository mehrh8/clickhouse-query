from clickhouse_query import mixins, utils


class Function(mixins.ASMixin, mixins.ArithmeticMixin):
    def __init__(self, function_name):
        self.args_list: list[list] = []
        self.function_name = function_name

    def __call__(self, *args):
        self.args_list.append(args)
        return self

    def get_function_name(self) -> str:
        return self.function_name

    def __sql__(self, uid_generator):
        sql_params = {}
        sql = self.get_function_name()
        for args in self.args_list:
            args_sqls_list = []
            for item in args:
                expression = utils.get_expression(item, str_is_field=True)
                item_sql, item_sql_params = utils.get_sql(expression, uid_generator=uid_generator)
                args_sqls_list.append(item_sql)
                sql_params.update(item_sql_params)
            sql += "({args})".format(args=", ".join(args_sqls_list))
        return sql, sql_params


class AggregationFunction(Function):
    combinators_order = ["If"]

    def __init__(self, function_name):
        super().__init__(function_name)
        self.combinator_dict = {}

    def get_function_name(self) -> str:
        get_function_name = self.get_function_name()
        for combinator in self.combinators_order:
            if combinator in self.combinator_dict:
                get_function_name += "{combinator}".format(combinator=combinator)
        return get_function_name

    def __sql__(self):
        for combinator in self.combinators_order:
            if combinator in self.combinator_dict:
                combinator_function_args = self.combinator_dict[combinator].get("args", [])
                self.args_list[-1].extend(combinator_function_args)
        return super().__sql__()

    def __getattr__(self, item: str):
        def combinator_method(*args):
            combinator = item[0].upper() + item[1:]
            self.combinator_dict[combinator] = {"args": args}
            return self

        return combinator_method


class _FunctionBuilder:
    def __getattr__(self, item: str) -> Function:
        item = item[0].lower() + item[1:]
        return Function(item)


class _AggregationFunctionBuilder:
    def __getattr__(self, item) -> AggregationFunction:
        item = item[0].lower() + item[1:]
        return AggregationFunction(item)


functions = _FunctionBuilder()
aggregation_functions = _AggregationFunctionBuilder()
