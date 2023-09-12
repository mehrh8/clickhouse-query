from clickhouse_query.functions import base


class In(base._FuncListArgs):
    function = "in"
