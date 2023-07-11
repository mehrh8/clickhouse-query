from clickhouse_query.functions import base

class IsNull(base._Func1Args):
    function = "isNull"