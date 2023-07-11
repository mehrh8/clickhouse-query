from clickhouse_query.functions import base

class In(base._Func2Args):
    function = "in"
