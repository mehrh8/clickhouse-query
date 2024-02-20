import unittest

import clickhouse_query as ch


class TestClickhouseQuery(unittest.TestCase):

    def test_array(self):
        q = ch.QuerySet().select(a="b").prewhere(b__in=[1, 2, 3])
        print(ch.get_sql(q))

        q = ch.QuerySet().select(a="b").prewhere(b__in=[(1, "a"), (2, "b"), (3, "c")])
        print(ch.get_sql(q))

    def test_field(self):
        q = ch.QuerySet().select(a="b").prewhere(ch.Equals("b", ch.Value("salam")))
        print(ch.get_sql(q))

    def test_multi_filter_on_field(self):
        q = ch.QuerySet().select(a="b").prewhere(b__lt=4).prewhere(b__lt=5)
        print(ch.get_sql(q))

    def test_int(self):
        q = ch.QuerySet().select(a="b").prewhere(ch.Equals("b", 2))
        print("#1", ch.get_sql(q))
