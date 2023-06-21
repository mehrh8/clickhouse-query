from src.query import QuerySet
from src.functions import *

q = QuerySet(database="mydb").select(F("a___b__year").as_("mehr"), 2, F("a___c") // 2).from_("mytbl").where().group_by("b", "c").limit_by(2, "b", offset=2).limit(100)

print(q.get_sql())
# SELECT a FROM mytbl WHERE and(equals(a, 'b'), equals('a', 'b'), NULL) GROUP BY b, c;
