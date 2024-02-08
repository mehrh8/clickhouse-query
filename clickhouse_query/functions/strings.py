from clickhouse_query.functions import base


class Concat(base.Function):
    function = "concat"
