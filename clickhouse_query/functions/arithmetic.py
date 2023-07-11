from clickhouse_query.functions import base


class Plus(base._Func2Args):
    function = "plus"

class Minus(base._Func2Args):
    function = "minus"

class Multiply(base._Func2Args):
    function = "multiply"

class Divide(base._Func2Args):
    function = "divide"

class IntDiv(base._Func2Args):
    function = "intDiv"

class IntDivOrZero(base._Func2Args):
    function = "intDivOrZero"

class Modulo(base._Func2Args):
    function = "modulo"

class ModuloOrZero(base._Func2Args):
    function = "moduloOrZero"

class PositiveModulo(base._Func2Args):
    function = "positiveModulo"

class Negate(base._Func1Args):
    function = "negate"

class Abs(base._Func1Args):
    function = "abs"

class GCD(base._Func2Args):
    function = "gcd"

class LCM(base._Func2Args):
    function = "lcm"

class Max2(base._Func2Args):
    function = "max2"

class Min2(base._Func2Args):
    function = "min2"

class MultiplyDecimal(base.Func):
    function = "multiplyDecimal"
    def __init__(self, arg1, arg2, result_scale=None):
        if result_scale is not None:
            super().__init__(arg1, arg2, result_scale)
        else:
            super().__init__(arg1, arg2)

class DivideDecimal(MultiplyDecimal):
    function = "divideDecimal"
