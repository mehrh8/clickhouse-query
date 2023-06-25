from src.query import QuerySet, Table
from src.functions import *
from src.functions.aggregate.aggregate import ExponentialMovingAverage

# q = QuerySet(database="mydb").select(F("a___b__year").as_("mehr"), 2, F("a___c") // 2).from_("mytbl").where().group_by("b", "c").limit_by(2, "b", offset=2).limit(100)

e = ExponentialMovingAverage("a", "b", x=4).if_("c")

class TableTest(Table):
    class Meta:
        table_name = "table_test"

class TableTest2(Table):
    class Meta:
        table_name = "table_test2"

t = TableTest().inner_join(t2:=TableTest2(), on=1).inner_join(t2, on=1)
q = t.queryset.select("a", e).where(a___date__lt=2020, b___c__month=2).order_by()

# print(t._joins[0])

print(q.get_sql())
a = "SELECT a FROM mytbl WHERE and(equals(a, 'b'), equals('a', 'b'), NULL) GROUP BY b, c"
