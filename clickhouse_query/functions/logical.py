from clickhouse_query.functions import base
from clickhouse_query.utils import extract_q, get_expression

class _Logical(base.Func):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args)

    def __sql__(self, params):
        additional_args = [extract_q(item, get_expression(value)) for item, value in self.kwargs.items()]
        if len(self.args) + len(additional_args) == 1:
            if additional_args:
                return base.get_sql(additional_args[0], params=params)
            else:
                return base.get_sql(self.args[0], params=params)
        return super().__sql__(*additional_args, params=params)

class And(_Logical):
    function = "and"

class Or(_Logical):
    function = "or"

class Not(base._Func1Args):
    function = "not"

class Xor(_Logical):
    function = "xor"
