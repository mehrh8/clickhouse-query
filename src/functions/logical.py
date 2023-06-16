from . import base

class _Logical(base.Func):
    def __sql__(self):
        if len(self.args) == 1:
            return base.get_sql(self.args[0])
        return super().__sql__()

class And(_Logical):
    function = "and"

class Or(_Logical):
    function = "or"

class Not(base._Func1Args):
    function = "not"

class Xor(_Logical):
    function = "xor"
