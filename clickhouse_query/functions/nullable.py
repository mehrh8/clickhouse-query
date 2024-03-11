from clickhouse_query.functions import base


class IsNull(base._Function1Args):
    function = "isNull"


class IsNotNull(base._Function1Args):
    function = "isNotNull"
