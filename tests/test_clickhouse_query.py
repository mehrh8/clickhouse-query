import clickhouse_query as ch


def test_select():
    q = ch.QuerySet().select("a")
    assert ch.get_sql(q) == ("SELECT a", {})
    q = ch.QuerySet().select(a="a")
    assert ch.get_sql(q) == ("SELECT a AS a", {})

    q = ch.QuerySet().select(1)
    assert ch.get_sql(q) == ("SELECT %(__U_1)f", {"__U_1": 1})
    q = ch.QuerySet().select(a=1)
    assert ch.get_sql(q) == ("SELECT %(__U_1)f AS a", {"__U_1": 1})

    q = ch.QuerySet().select(ch.Value("a"))
    assert ch.get_sql(q) == ("SELECT %(__U_1)s", {"__U_1": "a"})
    q = ch.QuerySet().select(a=ch.Value("a"))
    assert ch.get_sql(q) == ("SELECT %(__U_1)s AS a", {"__U_1": "a"})

    q = ch.QuerySet().select([1, 2, 3])
    assert ch.get_sql(q) == ("SELECT %(__U_1)s", {"__U_1": (1, 2, 3)})
    q = ch.QuerySet().select(a=[1, 2, 3])
    assert ch.get_sql(q) == ("SELECT %(__U_1)s AS a", {"__U_1": (1, 2, 3)})


def test_from():
    q = ch.QuerySet().from_("table1")
    assert ch.get_sql(q) == ("SELECT * FROM table1", {})

    sub_q = ch.QuerySet().from_("table1")
    q = ch.QuerySet().from_(ch.Subquery(sub_q))
    assert ch.get_sql(q) == ("SELECT * FROM (SELECT * FROM table1)", {})


def test_distinct():
    q = ch.QuerySet().distinct()
    assert ch.get_sql(q) == ("SELECT DISTINCT *", {})

    q = ch.QuerySet().select("a").distinct()
    assert ch.get_sql(q) == ("SELECT DISTINCT a", {})

    q = ch.QuerySet().select("a").distinct("b")
    assert ch.get_sql(q) == ("SELECT DISTINCT ON (b) a", {})


def test_prewhere():
    q = ch.QuerySet().from_("t1").prewhere(a="b")
    assert ch.get_sql(q) == ("SELECT * FROM t1 PREWHERE (equals(a, %(__U_1)s))", {"__U_1": "b"})

    q = ch.QuerySet().from_("t1").prewhere(ch.functions.equals("a", ch.Value("b")))
    assert ch.get_sql(q) == ("SELECT * FROM t1 PREWHERE (equals(a, %(__U_1)s))", {"__U_1": "b"})

    q = ch.QuerySet().from_("t1").prewhere(a="b", c="d")
    assert ch.get_sql(q) == (
        "SELECT * FROM t1 PREWHERE and(equals(a, %(__U_1)s), equals(c, %(__U_2)s))",
        {"__U_1": "b", "__U_2": "d"},
    )


def test_functions():
    f = ch.functions.func(1, 2)
    assert ch.get_sql(f) == ("func(%(__U_1)f, %(__U_2)f)", {"__U_1": 1, "__U_2": 2})

    f = ch.functions.func(1, 2).as_("f1")
    assert ch.get_sql(f) == ("func(%(__U_1)f, %(__U_2)f) AS f1", {"__U_1": 1, "__U_2": 2})

    f = ch.functions.func(1, 2) + 1
    assert ch.get_sql(f) == (
        "plus(func(%(__U_1)f, %(__U_2)f), %(__U_3)f)",
        {"__U_1": 1, "__U_2": 2, "__U_3": 1},
    )

    f = ch.functions.sumIf("a", "b")
    assert ch.get_sql(f) == ("sumIf(a, b)", {})


def test_group_by():
    q = ch.QuerySet().select(key="col1", a=ch.functions.count()).from_("t1").group_by("key")
    assert ch.get_sql(q) == ("SELECT col1 AS key, count() AS a FROM t1 GROUP BY key", {})

    q = ch.QuerySet().select(key="col1", a=ch.functions.count()).from_("t1").group_by("key", "key2")
    assert ch.get_sql(q) == ("SELECT col1 AS key, count() AS a FROM t1 GROUP BY key, key2", {})


def test_order_by():
    q = ch.QuerySet().order_by("col1")
    assert ch.get_sql(q) == ("SELECT * ORDER BY col1 ASC", {})

    q = ch.QuerySet().order_by("-col1")
    assert ch.get_sql(q) == ("SELECT * ORDER BY col1 DESC", {})

    q = ch.QuerySet().order_by("col1", "-col2")
    assert ch.get_sql(q) == ("SELECT * ORDER BY col1 ASC, col2 DESC", {})


def test_q():
    f = ch.Q(a=1, b=2)
    assert ch.get_sql(f) == ("and(equals(a, %(__U_1)f), equals(b, %(__U_2)f))", {"__U_1": 1, "__U_2": 2})

    f = ch.Q(a=1, b=2, _connector="or")
    assert ch.get_sql(f) == ("or(equals(a, %(__U_1)f), equals(b, %(__U_2)f))", {"__U_1": 1, "__U_2": 2})

    f = ch.Q(a=1, b=2, _connector="my_connector")
    assert ch.get_sql(f) == (
        "my_connector(equals(a, %(__U_1)f), equals(b, %(__U_2)f))",
        {"__U_1": 1, "__U_2": 2},
    )


def test_limit():
    q = ch.QuerySet().limit(2)
    assert ch.get_sql(q) == ("SELECT * LIMIT %(__U_1)f", {"__U_1": 2})

    q = ch.QuerySet().limit(5, offset=10)
    assert ch.get_sql(q) == ("SELECT * LIMIT %(__U_1)f OFFSET %(__U_2)f", {"__U_1": 5, "__U_2": 10})

    q = ch.QuerySet().limit(None)
    assert ch.get_sql(q) == ("SELECT *", {})


def test_limit_by():
    q = ch.QuerySet().limit_by(2, "col1")
    assert ch.get_sql(q) == ("SELECT * LIMIT %(__U_1)f BY col1", {"__U_1": 2})

    q = ch.QuerySet().limit_by(2, "col1", "col2")
    assert ch.get_sql(q) == ("SELECT * LIMIT %(__U_1)f BY col1, col2", {"__U_1": 2})
