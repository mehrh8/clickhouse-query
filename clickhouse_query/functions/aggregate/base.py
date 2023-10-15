from clickhouse_query.functions import base
from clickhouse_query import utils


class AggFunc(base.Func):
    suffix_order = ["If"]

    def __init__(self, *args):
        super().__init__(*args)

    def if_(self, if_):
        self._if = utils._get_expression(if_)
        return self

    @property
    def suffix_dict(self):
        suffix_dict = {}
        if getattr(self, "_if", None) is not None:
            suffix_dict["If"] = self._if
        return suffix_dict

    def __sql__(self, sql_params):
        additional_args = [
            self.suffix_dict[suffix_name]
            for suffix_name in self.suffix_order
            if self.suffix_dict.get(suffix_name) is not None
        ]
        return super().__sql__(*additional_args, sql_params=sql_params)

    def get_function(self, sql_params):
        function = super().get_function(sql_params=sql_params)
        suffix_dict = self.suffix_dict
        return function + "".join(
            [suffix_name for suffix_name in self.suffix_order if suffix_name in suffix_dict]
        )


class AggFuncWithParams(AggFunc):
    def __init__(self, *args, params=None):
        super().__init__(*args)
        self.params = params

    def get_function(self, sql_params):
        # self.params and sql_params is not equals.
        func = super().get_function(sql_params=sql_params)
        if self.params is not None:
            return "{func}({params})".format(
                func=func,
                params=", ".join([utils._get_sql(p, sql_params=sql_params) for p in self.params]),
            )
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
