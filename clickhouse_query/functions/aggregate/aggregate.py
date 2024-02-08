from clickhouse_query.functions.aggregate import base as aggregate_base


class _AggregationFunctionWithMaxSizeParam(aggregate_base.AggregationFunctionWithParams):
    def __init__(self, x, max_size=None):
        if max_size is None:
            super().__init__(x)
        else:
            super().__init__(x, params=[max_size])


class Count(aggregate_base.AggregationFunction):
    function = "count"

    def __init__(self, arg=None):
        if arg is None:
            super().__init__()
        else:
            super().__init__(arg)


class Min(aggregate_base._AggregationFunction1Args):
    function = "min"


class Max(aggregate_base._AggregationFunction1Args):
    function = "max"


class Sum(aggregate_base._AggregationFunction1Args):
    function = "sum"


class Avg(aggregate_base._AggregationFunction1Args):
    function = "avg"


class Any(aggregate_base._AggregationFunction1Args):
    function = "any"


class FirstValue(aggregate_base._AggregationFunction1Args):
    function = "first_value"


class LastValue(aggregate_base._AggregationFunction1Args):
    function = "last_value"


class ArgMin(aggregate_base._AggregationFunction2Args):
    function = "argMin"


class ArgMax(aggregate_base._AggregationFunction2Args):
    function = "argMax"


class AvgWeighted(aggregate_base._AggregationFunction2Args):
    function = "avgWeighted"


class TopK(aggregate_base.AggregationFunctionWithParams):
    function = "topK"

    def __init__(self, column, *, k):
        super().__init__(column, params=[k])


class TopKWeighted(aggregate_base.AggregationFunctionWithParams):
    function = "topKWeighted"

    def __init__(self, column, weight, *, k):
        super().__init__(column, weight, params=[k])


class GroupArray(_AggregationFunctionWithMaxSizeParam):
    function = "groupArray"


class GroupUniqArray(_AggregationFunctionWithMaxSizeParam):
    function = "groupUniqArray"


class GroupArraySample(aggregate_base.AggregationFunctionWithParams):
    def __init__(self, arg1, *, max_size, seed=None):
        if seed is None:
            super().__init__(arg1, params=[max_size])
        else:
            super().__init__(arg1, params=[max_size, seed])


class SumCount(aggregate_base._AggregationFunction1Args):
    function = "sumCount"


class Uniq(aggregate_base.AggregationFunction):
    function = "uniq"


class UniqExact(aggregate_base.AggregationFunction):
    function = "uniqExact"
