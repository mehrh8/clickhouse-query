from clickhouse_query.functions.aggregate import base

class Count(base.AggFunc):
    function = "count"
    
    def __init__(self, arg=None):
        if arg is None:
            super().__init__()
        else:
            super().__init__(arg)

class BoundingRatio(base._AggFunc2Args):
    function = "boundingRatio"

class Min(base._AggFunc1Args):
    function = "min"

class Max(base._AggFunc1Args):
    function = "max"

class Sum(base._AggFunc1Args):
    function = "sum"

class Avg(base._AggFunc1Args):
    function = "avg"

class Any(base._AggFunc1Args):
    function = "any"

class FirstValue(base._AggFunc1Args):
    function = "first_value"

class LastValue(base._AggFunc1Args):
    function = "last_value"

class StddevPop(base._AggFunc1Args):
    function = "stddevPop"

class StddevSamp(base._AggFunc1Args):
    function = "stddevSamp"

class VarPop(base._AggFunc1Args):
    function = "varPop"

class VarSamp(base._AggFunc1Args):
    function = "varSamp"

class CovarPop(base._AggFunc1Args):
    function = "covarPop"

class CovarSamp(base._AggFunc1Args):
    function = "covarSamp"

class AnyHeavy(base._AggFunc1Args):
    function = "anyHeavy"

class AnyLast(base._AggFunc1Args):
    function = "anyLast"

class ArgMin(base._AggFunc2Args):
    function = "argMin"

class ArgMax(base._AggFunc2Args):
    function = "argMax"

class AvgWeighted(base._AggFunc2Args):
    function = "avgWeighted"

class Corr(base._AggFunc2Args):
    function = "corr"

class ExponentialMovingAverage(base.AggFuncWithParams):
    function = "exponentialMovingAverage"

    def __init__(self, value, timeunit, *, x):
        super().__init__(value, timeunit, params=[x])

class TopK(base.AggFuncWithParams):
    function = "topK"

    def __init__(self, column, *, k):
        super().__init__(column, params=[k])

class TopKWeighted(base.AggFuncWithParams):
    function = "topKWeighted"

    def __init__(self, column, weight, *, k):
        super().__init__(column, weight, params=[k])

class _AggFuncWithMaxSizeParam(base.AggFuncWithParams):
    def __init__(self, x, max_size=None):
        if max_size is None:
            super().__init__(x)
        else:
            super().__init__(x, params=[max_size])


class GroupArray(_AggFuncWithMaxSizeParam):
    function = "groupArray"

class GroupArrayLast(_AggFuncWithMaxSizeParam):
    function = "groupArrayLast"

class GroupUniqArray(_AggFuncWithMaxSizeParam):
    function = "groupUniqArray"

class GroupArrayInsertAt(base.AggFuncWithParams):
    function = "groupArrayInsertAt"

    def __init__(self, x, pos, default_x=None, size=None):
        if default_x is None and size is None:
            super().__init__(x, pos)
        elif default_x is not None and size is None:
            super().__init__(x, pos, params=[default_x])
        elif default_x is not None and size is not None:
            super().__init__(x, pos, params=[default_x, size])
        else:
            raise Exception()

class _AggFuncWithWindowSizeParam(base.AggFuncWithParams):
    def __init__(self, arg1, window_size=None):
        if window_size is None:
            super().__init__(arg1)
        else:
            super().__init__(arg1, params=[window_size])

class GroupArrayMovingSum(_AggFuncWithWindowSizeParam):
    function = "groupArrayMovingSum"

class GroupArrayMovingAvg(_AggFuncWithWindowSizeParam):
    function = "groupArrayMovingAvg"

class GroupArraySample(base.AggFuncWithParams):
    def __init__(self, arg1, *, max_size, seed=None):
        if seed is None:
            super().__init__(arg1, params=[max_size])
        else:
            super().__init__(arg1, params=[max_size, seed])


class GroupBitAnd(base._AggFunc1Args):
    function = "groupBitAnd"

class GroupBitOr(base._AggFunc1Args):
    function = "groupBitOr"

class GroupBitXor(base._AggFunc1Args):
    function = "groupBitXor"

class GroupBitmap(base._AggFunc1Args):
    function = "groupBitmap"

class GroupBitmapAnd(base._AggFunc1Args):
    function = "groupBitmapAnd"

class GroupBitmapOr(base._AggFunc1Args):
    function = "groupBitmapOr"

class groupBitmapXor(base._AggFunc1Args):
    function = "groupBitmapXor"

class SumWithOverflow(base._AggFunc1Args):
    function = "sumWithOverflow"

class DeltaSum(base._AggFunc1Args):
    function = "deltaSum"

class DeltaSumTimestamp(base._AggFunc2Args):
    function = "deltaSumTimestamp"

class SumCount(base._AggFunc1Args):
    function = "sumCount"

class RankCorr(base._AggFunc2Args):
    function = "rankCorr"

class SumKahan(base._AggFunc1Args):
    function = "sumKahan"

class IntervalLengthSum(base._AggFunc2Args):
    function = "intervalLengthSum"

class SkewPop(base._AggFunc1Args):
    function = "skewPop"

class SkewSamp(base._AggFunc1Args):
    function = "skewSamp"

class KurtPop(base._AggFunc1Args):
    function = "kurtPop"

class KurtSamp(base._AggFunc1Args):
    function = "kurtSamp"


class Uniq(base.AggFunc):
    function = "uniq"

class UniqExact(base.AggFunc):
    function = "uniqExact"

class _AggFuncWithHLLPrecisionParams(base.AggFuncWithParams):
    def __init__(self, *args, HLL_precision=None):
        if HLL_precision is None:
            super().__init__(*args)
        else:
            super().__init__(*args, params=[HLL_precision])

class UniqCombined(_AggFuncWithHLLPrecisionParams):
    function = "uniqCombined"

class UniqCombined64(_AggFuncWithHLLPrecisionParams):
    function = "uniqCombined64"

class UniqHLL12(base.AggFunc):
    function = "uniqHLL12"

class UniqTheta(base.AggFunc):
    function = "uniqTheta"

