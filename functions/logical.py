from . import base

class And(base.Func):
    function = "and"

    def __sql__(self):
        if len(self.args) == 1:
            return base.get_sql(self.args[0])
        return super().__sql__()

class Or(And):
    function = "or"

class Not(base._Func1Args):
    function = "not"

class Xor(And):
    function = "xor"
