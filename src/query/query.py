from ..functions import base
from .. import functions as func
from ..functions.utils import extract_q, get_sql

class _U:
    _u = 0

    @classmethod
    @property
    def u(cls):
        cls._u += 1
        return "__U_{u}".format(u=cls._u)



class QuerySet:
    def __init__(self):
        self._from = None
        self._select_list = []
        self._distinct = False
        self._distinct_on_list = []
        self._prewhere_list = []
        self._where_list = []
        self._group_by_list = []
        self._having_list = []
        self._order_by_list = []
        self._limit = None
        self._limit_by = None
        self._settings_dict = {}

    def select(self, *select):
        self._select_list = [base.F(s) if isinstance(s, str) else s for s in select]
        return self
    
    def distinct(self, *distinct):
        self._distinct = True
        self._distinct_on_list = [base.F(d) if isinstance(d, str) else d for d in distinct]
        return self

    def from_(self, from_):
        self._from = base.F(from_) if isinstance(from_, str) else from_
        return self

    def prewhere(self, *args, **kwargs):
        new_args = list(args) + [extract_q(k, v) for k, v in kwargs.items()]
        self._prewhere_list = new_args
        return self

    def where(self, *args, **kwargs):
        new_args = list(args) + [extract_q(k, v) for k, v in kwargs.items()]
        self._where_list = new_args
        return self

    def group_by(self, *args):
        self._group_by_list = [base.F(arg) if isinstance(arg, str) else arg for arg in args]
        return self

    def having(self, *args, **kwargs):
        new_args = list(args) + [extract_q(k, v) for k, v in kwargs.items()]
        self._having_list = new_args
        return self
    
    def order_by(self, *args):
        self._order_by_list = [base.F(arg) if isinstance(arg, str) else arg for arg in args]
        return self

    def limit(self, limit, *, offset=None):
        self._limit = (limit, offset)
        return self

    def limit_by(self, limit, *by, offset=None):
        assert len(by) != 0
        by = [base.F(arg) if isinstance(arg, str) else arg for arg in by]
        self._limit_by = (limit, offset, by)
        return self
    
    def settings(self, **kwargs):
        self._settings = kwargs
        return self
    
    def _get_raw_distinct(self):
        if not self._distinct:
            return ""
        raw_distinct = " DISTINCT"
        if self._distinct_on_list:
            raw_distinct += " ON ({})".format(", ".join(map(base.get_sql, self._distinct_on_list)))
        return raw_distinct

    def _get_raw_select(self):
        if not self._select_list:
            return " *"
        raw_select = " {}".format(", ".join(map(base.get_sql, self._select_list)))
        return raw_select

    def _get_raw_from(self):
        if self._from is None:
            return ""
        return " FROM {}".format(base.get_sql(self._from))
    
    def _get_raw_prewhere(self):
        if not self._prewhere_list:
            return ""
        raw_prewhere = " PREWHERE {}".format(base.get_sql(func.And(*self._prewhere_list)))
        return raw_prewhere

    def _get_raw_where(self):
        if not self._where_list:
            return ""
        raw_where = " WHERE {}".format(base.get_sql(func.And(*self._where_list)))
        return raw_where
    
    def _get_raw_group_by(self):
        if not self._group_by_list:
            return ""
        raw_group_by = " GROUP BY {}".format(", ".join(map(base.get_sql, self._group_by_list)))
        return raw_group_by
    
    def _get_raw_having(self):
        if not self._having_list:
            return ""
        raw_having = " HAVING {}".format(base.get_sql(func.And(*self._having_list)))
        return raw_having
    
    def _get_raw_order_by(self):
        if not self._order_by_list:
            return ""
        raw_order_by = " ORDER BY {}".format(", ".join(map(base.get_sql, self._order_by_list)))
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
        if not self._settings_dict:
            return ""
        settings_list = [k + "=" + v for k, v in self._settings_dict.items()]
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


class Table:
    class Meta:
        pass

    def __init__(self):
        self._as = _U.u
        self._joins = []

    @property
    def queryset(self):
        return QuerySet().from_(self)

    def get_as(self):
        return self._as
    
    def get_table(self):
        return getattr(self.Meta, "table_name")

    def inner_join(self, other, on=None, using=None):
        self._joins.append(base.InnerJoin(other, on=on, using=using))
        return self
    
    def __sql__(self):
        sql = "{table} AS {as_}".format(table=self.get_table(), as_=self.get_as())

        for join in self._joins:
            sql += " {join}".format(join=get_sql(join))

        return sql
