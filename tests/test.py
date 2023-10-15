import unittest
from clickhouse_query import models, utils


class TestClickhouseQuery(unittest.TestCase):
    def test_test(self):
        class TableTest(models.Table):
            class Meta:
                table_name = "table_test"

        utils._GetAs.reset()
        q = TableTest().queryset.select("a", "b").where(a__lt=10)
        models.Subquery(q).inner_join(TableTest()).queryset.where(b__lt=20).limit
        q.run()
        # self.assertEqual(q, "SELECT a, b FROM table_test AS __U_1 WHERE less(a, 10)")

        # utils._GetAs.reset()
        # q = TableTest().queryset.select("a", "b").where(a__lt=20)
        # self.assertEqual(utils._get_sql(q), "SELECT a, b FROM table_test AS __U_1 WHERE less(a, 20)")
