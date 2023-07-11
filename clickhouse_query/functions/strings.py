from clickhouse_query.functions import base

class Concat(base.Func):
    function = "concat"