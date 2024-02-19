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
