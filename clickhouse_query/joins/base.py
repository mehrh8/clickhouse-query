from clickhouse_query.utils import get_sql, ExpressionMixin


class _Join(ExpressionMixin):
    join_type = None

    def __init__(self, arg1, arg2, on=None, using=None):
        self.arg1 = arg1
        self.arg2 = arg2
        self._on = on
        self._using = using
    
    def on(self, on):
        self._on = on
        return self
    
    def using(self, *args):
        self._using = args
        return self
    
    @property
    def queryset(self):
        from clickhouse_query.models import QuerySet

        return QuerySet().from_(self)

    def __sql__(self, params):
        sql = "{arg1} {join_type} {arg2}".format(
            arg1=get_sql(self.arg1, params=params), join_type=self.join_type, arg2=get_sql(self.arg2, params=params)
        )
        assert self._on is None or self._using is None
        
        if self._on is not None:
            sql += " ON {on}".format(on=get_sql(self._on, params=params))
        elif self._using is not None:
            sql += " USING ({using})".format(using=", ".join([get_sql(u, params=params) for u in self._using]))

        return sql

class InnerJoin(_Join):
    join_type = "INNER JOIN"