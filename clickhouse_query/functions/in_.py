from clickhouse_query.functions import base


class In(base._Function2Args):
    function = "in"
