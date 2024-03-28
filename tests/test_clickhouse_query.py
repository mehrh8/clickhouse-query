import clickhouse_query as ch


def test_array():
    ch.QuerySet().select(a="b").prewhere(b__in=[1, 2, 3])
    ch.QuerySet().select(a="b").prewhere(b__in=[(1, "a"), (2, "b"), (3, "c")])
