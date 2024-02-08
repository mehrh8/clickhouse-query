from clickhouse_query.functions import base


class Like(base._Function2Args):
    function = "like"


class ILike(base._Function2Args):
    function = "ilike"
