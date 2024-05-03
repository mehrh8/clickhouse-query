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


class _FunctionBuilder:
    def __getattr__(self, item: str) -> Function:
        if item[-1] == "_":
            item = item[:-1]
        return Function(item)


functions = _FunctionBuilder()
