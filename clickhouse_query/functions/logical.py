from clickhouse_query import utils
from clickhouse_query.functions import base


class _Logical(base.Function):
    def __clickhouse_query_function_sql__(self, *, uid_generator):
        if len(self.args) + len(self.kwargs) == 0:
            raise ValueError("At least one argument is required")

        if len(self.args) + len(self.kwargs) == 1:
            return "", {}

        return super().__clickhouse_query_function_sql__(uid_generator=uid_generator)

    def __clickhouse_query_function_args_sqls__(self, *, uid_generator):
        function_args_sqls_list, function_args_params = super().__clickhouse_query_function_args_sqls__(
            uid_generator=uid_generator
        )
        for item, value in self.kwargs.items():
            expression = utils._get_expression(value)
            condition = utils._extract_condition(item, expression)
            sql, params = utils.get_sql(condition, uid_generator=uid_generator)
            function_args_sqls_list.append(sql)
            function_args_params.update(params)

        return function_args_sqls_list, function_args_params


class And(_Logical):
    function = "and"


class Or(_Logical):
    function = "or"


class Not(base._Function1Args):
    function = "not"


class Xor(_Logical):
    function = "xor"
