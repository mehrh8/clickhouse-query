from clickhouse_query import functions
from clickhouse_query.utils import extract_q, get_sql, ASMixin, GetAs, ExpressionMixin, get_expression, F, JoinableMixin, get_expression_or_f


class QuerySet:
    def __init__(self):
        self._from = None
        self._select_list = []
        self._distinct = False
        self._distinct_on_list = []
        self._prewhere_list = []
        self._prewhere_dict = {}
        self._where_list = []
        self._where_dict = {}
        self._group_by_list = []
        self._having_list = []
        self._having_dict = {}
        self._order_by_list = []
        self._limit = None
        self._limit_by = None

    def select(self, *select):
        self._select_list = list(select)
        return self
    
    def distinct(self, *distinct):
        self._distinct = True
        self._distinct_on_list = list(distinct)
        return self

    def from_(self, from_):
        self._from = from_
        return self

    def prewhere(self, *args, **kwargs):
        self._prewhere_list = list(args)
        self._prewhere_dict = dict(kwargs)
        return self

    def where(self, *args, **kwargs):
        self._where_list = list(args)
        self._where_dict = dict(kwargs)
        return self

    def group_by(self, *group_by):
        self._group_by_list = list(group_by)
        return self

    def having(self, *args, **kwargs):
        self._having_list = list(args)
        self._having_dict = dict(kwargs)
        return self
    
    def order_by(self, *order_by):
        self._order_by_list = list(order_by)
        return self

    def limit(self, limit, *, offset=None):
        self._limit = (limit, offset)
        return self

    def limit_by(self, limit, *by, offset=None):
        assert len(by) != 0
        self._limit_by = (limit, offset, list(by))
        return self
    
    def _get_raw_distinct(self, params):
        if not self._distinct:
            return ""
        raw_distinct = " DISTINCT"
        if self._distinct_on_list:
            raw_distinct += " ON ({})".format(", ".join([get_sql(get_expression_or_f(d), params=params) for d in self._distinct_on_list]))
        return raw_distinct

    def _get_raw_select(self, params):
        if not self._select_list:
            return " *"
        raw_select = " {}".format(", ".join([get_sql(get_expression_or_f(s), params=params) for s in self._select_list]))
        return raw_select

    def _get_raw_from(self, params):
        if self._from is None:
            return ""
        return " FROM {}".format(get_sql(get_expression_or_f(self._from), params=params))
    
    def _get_raw_prewhere(self, params):
        _list = self._prewhere_list + [extract_q(k, get_expression(v)) for k, v in self._prewhere_dict.items()]

        if not _list:
            return ""
        raw_prewhere = " PREWHERE {}".format(get_sql(functions.And(*_list), params=params))
        return raw_prewhere

    def _get_raw_where(self, params):
        _list = self._where_list + [extract_q(k, get_expression(v)) for k, v in self._where_dict.items()]

        if not _list:
            return ""
        raw_where = " WHERE {}".format(get_sql(functions.And(*_list), params=params))
        return raw_where
    
    def _get_raw_group_by(self, params):
        if not self._group_by_list:
            return ""
        raw_group_by = " GROUP BY {}".format(", ".join([get_sql(get_expression_or_f(g), params=params) for g in self._group_by_list]))
        return raw_group_by
    
    def _get_raw_having(self, params):
        _list = self._having_list + [extract_q(k, get_expression(v)) for k, v in self._having_dict.items()]

        if not _list:
            return ""
        raw_having = " HAVING {}".format(get_sql(functions.And(*_list), params=params))
        return raw_having
    
    def _get_raw_order_by(self, params):
        if not self._order_by_list:
            return ""
        raw_order_by = " ORDER BY {}".format(", ".join([get_sql(get_expression_or_f(o), params=params) for o in self._order_by_list]))
        return raw_order_by

    def _get_raw_limit_by(self, params):
        if self._limit_by is None:
            return ""
        limit, offset, by = self._limit_by
        raw_limit_by = " LIMIT {}".format(get_sql(get_expression(limit), params=params))
        if offset is not None:
            raw_limit_by += " OFFSET {}".format(get_sql(get_expression(offset), params=params))
        raw_limit_by += " BY {}".format(", ".join([get_sql(get_expression_or_f(b), params=params) for b in by]))
        return raw_limit_by
    
    def _get_raw_limit(self, params):
        if self._limit is None:
            return ""
        limit, offset = self._limit
        raw_limit = " LIMIT {}".format(get_sql(get_expression(limit), params=params))
        if offset is not None:
            raw_limit += " OFFSET {}".format(get_sql(get_expression(offset), params=params))
        return raw_limit

    def __sql__(self, params):
        str_format = "SELECT{distinct}{select}{from_}{prewhere}{where}{group_by}{having}{order_by}{limit_by}{limit}"
        return str_format.format(
            distinct=self._get_raw_distinct(params=params),
            select=self._get_raw_select(params=params),
            from_=self._get_raw_from(params=params),
            prewhere=self._get_raw_prewhere(params=params),
            where=self._get_raw_where(params=params),
            group_by=self._get_raw_group_by(params=params),
            having=self._get_raw_having(params=params),
            order_by=self._get_raw_order_by(params=params),
            limit_by=self._get_raw_limit_by(params=params),
            limit=self._get_raw_limit(params=params),
        )

    def run(self, settings=None):
        params = {}
        sql = get_sql(self, params=params)
        print(sql, params)



class Table(ASMixin, JoinableMixin, ExpressionMixin):
    class Meta:
        pass

    def get_as(self):
        as_ = super().get_as()
        if as_ is None:
            as_ = GetAs.as_
            self.as_(as_)
        return as_

    @property
    def queryset(self):
        return QuerySet().from_(self)
    
    def get_table(self, params):
        return getattr(self.Meta, "table_name")

    def __sql__(self, params):
        sql = self.get_table(params=params)
        return sql

class _Null(ExpressionMixin):
    def __sql__(self, params):
        return "NULL"

NULL = _Null()


class Subquery(Table):
    def __init__(self, inner_queryset):
        self._inner_queryset = inner_queryset

    def get_table(self, params):
        return "({inner_queryset})".format(inner_queryset=get_sql(self._inner_queryset, params=params))
