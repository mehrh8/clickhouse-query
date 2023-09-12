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
Simple example using columns:

```python
from clickhouse_query import models
from clickhouse_query import Column
class ColumnTest(models.Table):
    column_a = Column('column_a')
    column_b = Column('column_b')

    class Meta:
        table_name = "table_test"

    def get_queryset(self):
        qs = self.queryset.select(self.column_a.as_('id'), self.column_b.as_('name')).where(self.column_a >= 15)
        return qs

    def get_query_string(self, queryset) -> str:
        query_string = queryset.get_sql()
        return query_string

    def get_simple_query(self) -> str:
        _qs = self.get_queryset()
        query = self.get_query_string(queryset=_qs)
        return query

query_string = ColumnTest().get_simple_query()
# produced "SELECT column_a AS id, column_b AS name FROM table_test AS __U_1 WHERE column_a >= '15';"
```