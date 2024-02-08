from clickhouse_query.functions import base


class Plus(base._Function2Args):
    function = "plus"


class Minus(base._Function2Args):
    function = "minus"


class Multiply(base._Function2Args):
    function = "multiply"


class Divide(base._Function2Args):
    function = "divide"


class IntDiv(base._Function2Args):
    function = "intDiv"


class IntDivOrZero(base._Function2Args):
    function = "intDivOrZero"


class Modulo(base._Function2Args):
    function = "modulo"


class ModuloOrZero(base._Function2Args):
    function = "moduloOrZero"


class PositiveModulo(base._Function2Args):
    function = "positiveModulo"


class Negate(base._Function2Args):
    function = "negate"


class Abs(base._Function2Args):
    function = "abs"


class GCD(base._Function2Args):
    function = "gcd"


class LCM(base._Function2Args):
    function = "lcm"


class Max2(base._Function2Args):
    function = "max2"


class Min2(base._Function2Args):
    function = "min2"


class MultiplyDecimal(base.Function):
    function = "multiplyDecimal"

    def __init__(self, arg1, arg2, result_scale=None):
        if result_scale is not None:
            super().__init__(arg1, arg2, result_scale)
        else:
            super().__init__(arg1, arg2)


class DivideDecimal(base.Function):
    function = "divideDecimal"

    def __init__(self, arg1, arg2, result_scale=None):
        if result_scale is not None:
            super().__init__(arg1, arg2, result_scale)
        else:
            super().__init__(arg1, arg2)
