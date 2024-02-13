import unittest

import clickhouse_query as ch


class TestClickhouseQuery(unittest.TestCase):

    def test_test(self):

        q = ch.QuerySet().select(a="b").prewhere(b__in=[1, 2, 3])
        ch.get_sql(q)
