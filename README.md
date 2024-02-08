# ClickHouse Query

Simple example:

```python
from clickhouse_query import models


class TableTest(models.Table):
    class Meta:
        table_name = "table_temp"


q = TableTest().queryset.select("a", "b").where(a__lt=10)

q.get_sql()
#  "SELECT a, b FROM table_temp AS __U_1 WHERE less(a, 10);"
```