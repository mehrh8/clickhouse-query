from clickhouse_query.functions.base import Func
from clickhouse_query.utils import get_sql, get_expression

class AggFunc(Func):
    suffix_order = ["If"]

    def __init__(self, *args):
        super().__init__(*args)
    
    def if_(self, if_):
        self._if = get_expression(if_)
        return self

    @property
    def suffix_dict(self):
        suffix_dict = {}
        if getattr(self, "_if", None) is not None:
            suffix_dict["If"] = self._if
        return suffix_dict

    def __sql__(self, params):
        additional_args = [
            self.suffix_dict[suffix_name] for suffix_name in self.suffix_order if self.suffix_dict.get(suffix_name) is not None
        ]
        return super().__sql__(*additional_args, params=params)

    def get_function(self, params):
        function = super().get_function(params=params)
        suffix_dict = self.suffix_dict
        return function + "".join(
            [suffix_name for suffix_name in self.suffix_order if suffix_name in suffix_dict]
        )

class AggFuncWithParams(AggFunc):
    def __init__(self, *args, params=None):
        super().__init__(*args)
        self.params = params

    def get_function(self, params):
        # self.params and params is not equals.
        func = super().get_function(params=params)
        if self.params is not None:
            return "{func}({params})".format(func=func, params=", ".join([get_sql(p, params=params) for p in self.params]))
        return func

class _AggFunc0Args(AggFunc):
    def __init__(self):
        super().__init__()

class _AggFunc1Args(AggFunc):
    def __init__(self, arg):
        super().__init__(arg)

class _AggFunc2Args(AggFunc):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
