from clickhouse_query.functions import base
from clickhouse_query.utils import extract_q

class _Logical(base.Func):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args)

    def __sql__(self):
        additional_args = [extract_q(item, value) for item, value in self.kwargs.items()]
        if len(self.args) + len(additional_args) == 1:
            return base.get_sql(self.args[0])
        return super().__sql__(*additional_args)

class And(_Logical):
    function = "and"

class Or(_Logical):
    function = "or"

class Not(base._Func1Args):
    function = "not"

class Xor(_Logical):
    function = "xor"
