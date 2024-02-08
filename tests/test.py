import unittest

import pytz

from clickhouse_query import functions, models, utils


class TestClickhouseQuery(unittest.TestCase):
    def test_test(self):

        tzinfo = pytz.timezone("Asia/Tehran")
        q = (
            models.QuerySet()
            .select(c=functions.aggregate.Count())
            .from_("click")
            .prewhere(date__year__gte="2024", date__month__gte="2")
            .limit(10)
        )
        print(utils.get_sql(q))
