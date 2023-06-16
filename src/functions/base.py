def get_sql(arg):
    if isinstance(arg, str):
        return "'{}'".format(arg)
    if isinstance(arg, (int, float)):
        return str(arg)
    return arg.__sql__()

class ASMixin:
    def as_(self, as_):
        self._as = as_
        return self

    def get_as(self):
        return getattr(self, "_as", None)


class Func(ASMixin):
    function = None

    def __init__(self, *args):
        self.args = args

    def __sql__(self, *additional_args):
        sql =  "{func}({args})".format(
            func=self.get_function(), args=", ".join(map(get_sql, self.args + additional_args))
        )
        as_ = self.get_as()
        if as_ is not None:
            sql += " AS {}".format(as_)
        return sql

    def get_function(self):
        assert self.function is not None
        return self.function
    
class AggFunc(Func):
    suffix_order = ["If"]

    def __init__(self, *args, if_=None):
        self.if_ = if_
        super().__init__(*args)

    @property
    def suffix_dict(self):
        suffix_dict = {}
        if self.if_ is not None:
            suffix_dict["if"] = self.if_
        return suffix_dict

    def __sql__(self):
        additional_args = [
            self.suffix_dict[suffix_name] for suffix_name in self.suffix_order if self.suffix_dict[suffix_name] is not None
        ]
        return super().__sql__(*additional_args)

    def get_function(self):
        function = super().get_function()
        suffix_dict = self.suffix_dict
        return function + "".join(
            [suffix_name for suffix_name in self.suffix_order if suffix_name in suffix_dict]
        )

class _Func0Args(Func):
    def __init__(self):
        super().__init__()

class _Func1Args(Func):
    def __init__(self, arg):
        super().__init__(arg)

class _Func2Args(Func):
    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)

class _AggFunc0Args(AggFunc):
    def __init__(self, *, if_=None):
        super().__init__(if_=if_)

class _AggFunc1Args(AggFunc):
    def __init__(self, arg, *, if_=None):
        super().__init__(arg, if_=if_)

class _AggFunc2Args(AggFunc):
    def __init__(self, arg1, arg2, *, if_=None):
        super().__init__(arg1, arg2, if_=if_)


class F(ASMixin):
    def __init__(self, arg):
        self.arg = arg

    def __sql__(self):
        return str(self.arg)

class Value(ASMixin):
    def __init__(self, arg):
        self.arg = arg
    
    def __sql__(self):
        return get_sql(self.arg)

class _Null:
    def __sql__(self):
        return "NULL"

NULL = _Null()