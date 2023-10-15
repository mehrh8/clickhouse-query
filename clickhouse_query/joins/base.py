from clickhouse_query import utils, mixins


class _Join(mixins.ExpressionMixin):
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

    def __sql__(self, sql_params):
        sql = "{arg1} {join_type} {arg2}".format(
            arg1=utils._get_sql(self.arg1, sql_params=sql_params),
            join_type=self.join_type,
            arg2=utils._get_sql(self.arg2, sql_params=sql_params),
        )
        assert self._on is None or self._using is None

        if self._on is not None:
            sql += " ON {on}".format(on=utils._get_sql(self._on, sql_params=sql_params))
        elif self._using is not None:
            sql += " USING ({using})".format(
                using=", ".join([utils._get_sql(u, sql_params=sql_params) for u in self._using])
            )

        return sql


class InnerJoin(_Join):
    join_type = "INNER JOIN"
