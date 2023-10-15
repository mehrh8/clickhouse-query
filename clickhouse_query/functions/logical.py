from clickhouse_query import utils
from clickhouse_query.functions import base


class _Logical(base.Func):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args)

    def __sql__(self, sql_params):
        additional_args = [
            utils._extract_condition(item, utils._get_expression(value))
            for item, value in self.kwargs.items()
        ]
        if len(self.args) + len(additional_args) == 1:
            if additional_args:
                return utils._get_sql(additional_args[0], sql_params=sql_params)
            else:
                return utils._get_sql(self.args[0], sql_params=sql_params)
        return super().__sql__(*additional_args, sql_params=sql_params)


class And(_Logical):
    function = "and"


class Or(_Logical):
    function = "or"


class Not(base._Func1Args):
    function = "not"


class Xor(_Logical):
    function = "xor"
