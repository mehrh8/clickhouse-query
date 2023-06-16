from functions import base

class Query:
    def __init__(self, database):
        self.database = database
        self._from = None
        self._select = None
        self._distinct = None
        self._prewhere = None
        self._where = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._limit = None
        self._limit_by = None
        self._offset = None
        self._settings = None

    def select(self, *select):
        self._select = [base.F(s) if isinstance(s, str) else s for s in select]
        return self
    
    def distinct(self, *distinct):
        self._distinct = [base.F(d) if isinstance(d, str) else d for d in distinct]
        return self

    def from_(self, from_):
        self._from = base.F(from_) if isinstance(from_, str) else from_
        return self

    def prewhere(self, prewhere):
        self._prewhere = prewhere
        return self

    def where(self, where):
        self._where = where
        return self

    def group_by(self, *group_by):
        assert len(group_by) != 0
        self._group_by = [base.F(g) if isinstance(g, str) else g for g in group_by]
        return self

    def having(self, having):
        self._having = having
        return self
    
    def order_by(self, *order_by):
        assert len(order_by) != 0
        self._order_by = [base.F(o) if isinstance(o, str) else o for o in order_by]
        return self

    def limit(self, limit, *, offset=None):
        self._limit = (limit, offset)
        return self

    def limit_by(self, limit, *by, offset=None):
        assert len(by) != 0
        by = [base.F(b) if isinstance(b, str) else b for b in by]
        self._limit_by = (limit, offset, by)
        return self
    
    def settings(self, **kwargs):
        self._settings = kwargs
        return self
    
    def _get_raw_distinct(self):
        if self._distinct is None:
            return ""
        raw_distinct = " DISTINCT"
        if self._distinct:
            raw_distinct += " ON ({})".format(", ".join(map(base.get_sql, self._distinct)))
        return raw_distinct

    def _get_raw_select(self):
        if self._select is None or len(self._select) == 0:
            return " *"
        raw_select = " {}".format(", ".join(map(base.get_sql, self._select)))
        return raw_select

    def _get_raw_from(self):
        if self._from is None:
            return ""
        return " FROM {}".format(base.get_sql(self._from))
    
    def _get_raw_prewhere(self):
        if self._prewhere is None:
            return ""
        raw_prewhere = " PREWHERE {}".format(base.get_sql(self._prewhere))
        return raw_prewhere

    def _get_raw_where(self):
        if self._where is None:
            return ""
        raw_where = " WHERE {}".format(base.get_sql(self._where))
        return raw_where
    
    def _get_raw_group_by(self):
        if self._group_by is None:
            return ""
        raw_group_by = " GROUP BY {}".format(", ".join(map(base.get_sql, self._group_by)))
        return raw_group_by
    
    def _get_raw_having(self):
        if self._having is None:
            return ""
        raw_having = " HAVING {}".format(base.get_sql(self._having))
        return raw_having
    
    def _get_raw_order_by(self):
        if self._order_by is None:
            return ""
        raw_order_by = " ORDER BY {}".format(", ".join(map(base.get_sql, self._order_by)))
        return raw_order_by

    def _get_raw_limit_by(self):
        if self._limit_by is None:
            return ""
        limit, offset, by = self._limit_by
        raw_limit_by = " LIMIT {}".format(base.get_sql(limit))
        if offset is not None:
            raw_limit_by += " OFFSET {}".format(base.get_sql(offset))
        raw_limit_by += " BY {}".format(", ".join(map(base.get_sql, by)))
        return raw_limit_by
    
    def _get_raw_limit(self):
        if self._limit is None:
            return ""
        limit, offset = self._limit
        raw_limit = " LIMIT {}".format(base.get_sql(limit))
        if offset is not None:
            raw_limit += " OFFSET {}".format(base.get_sql(offset))
        return raw_limit
    
    def _get_raw_settings(self):
        if self._settings is None or len(self._settings) == 0:
            return ""
        settings_list = [k + "=" + v for k, v in self._settings.items()]
        raw_settings=" SETTINGS {}".format(", ".join(settings_list))
        return raw_settings

    def get_sql(self):
        str_format = "SELECT{distinct}{select}{from_}{prewhere}{where}{group_by}{having}{order_by}{limit_by}{limit}{settings};"
        return str_format.format(
            distinct=self._get_raw_distinct(),
            select=self._get_raw_select(),
            from_=self._get_raw_from(),
            prewhere=self._get_raw_prewhere(),
            where=self._get_raw_where(),
            group_by=self._get_raw_group_by(),
            having=self._get_raw_having(),
            order_by=self._get_raw_order_by(),
            limit_by=self._get_raw_limit_by(),
            limit=self._get_raw_limit(),
            settings=self._get_raw_settings(),
        )

    def run(self):
        pass
