from clickhouse_query.functions import base


class Equals(base._Function2Args):
    function = "equals"


class NotEquals(base._Function2Args):
    function = "notEquals"


class Less(base._Function2Args):
    function = "less"


class Greater(base._Function2Args):
    function = "greater"


class LessOrEquals(base._Function2Args):
    function = "lessOrEquals"


class GreaterOrEquals(base._Function2Args):
    function = "greaterOrEquals"
