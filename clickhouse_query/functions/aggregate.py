from clickhouse_query.functions import base


class _AggregationFunctionWithMaxSizeParam(base.AggregationFunctionWithParams):
    def __init__(self, x, max_size=None):
        if max_size is None:
            super().__init__(x)
        else:
            super().__init__(x, params=[max_size])


class Count(base.AggregationFunction):
    function = "count"

    def __init__(self, arg=None):
        if arg is None:
            super().__init__()
        else:
            super().__init__(arg)


class Min(base._AggregationFunction1Args):
    function = "min"


class Max(base._AggregationFunction1Args):
    function = "max"


class Sum(base._AggregationFunction1Args):
    function = "sum"


class Avg(base._AggregationFunction1Args):
    function = "avg"


class Any(base._AggregationFunction1Args):
    function = "any"


class FirstValue(base._AggregationFunction1Args):
    function = "first_value"


class LastValue(base._AggregationFunction1Args):
    function = "last_value"


class ArgMin(base._AggregationFunction2Args):
    function = "argMin"


class ArgMax(base._AggregationFunction2Args):
    function = "argMax"


class AvgWeighted(base._AggregationFunction2Args):
    function = "avgWeighted"


class TopK(base.AggregationFunctionWithParams):
    function = "topK"

    def __init__(self, column, *, k):
        super().__init__(column, params=[k])


class TopKWeighted(base.AggregationFunctionWithParams):
    function = "topKWeighted"

    def __init__(self, column, weight, *, k):
        super().__init__(column, weight, params=[k])


class GroupArray(_AggregationFunctionWithMaxSizeParam):
    function = "groupArray"


class GroupUniqArray(_AggregationFunctionWithMaxSizeParam):
    function = "groupUniqArray"


class GroupArraySample(base.AggregationFunctionWithParams):
    def __init__(self, arg1, *, max_size, seed=None):
        if seed is None:
            super().__init__(arg1, params=[max_size])
        else:
            super().__init__(arg1, params=[max_size, seed])


class SumCount(base._AggregationFunction1Args):
    function = "sumCount"


class Uniq(base.AggregationFunction):
    function = "uniq"


class UniqExact(base.AggregationFunction):
    function = "uniqExact"
