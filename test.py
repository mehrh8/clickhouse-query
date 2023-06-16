from query import Query
from functions import *

q = Query(database="mydb").select("a").from_("mytbl").where(And(Equals(F("a"), "b"), Equals("a", "b"), NULL)).group_by("b", "c")

print(q.get_sql())
# SELECT a FROM mytbl WHERE and(equals(a, 'b'), equals('a', 'b'), NULL) GROUP BY b, c;