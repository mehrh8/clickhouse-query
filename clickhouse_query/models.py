from clickhouse_query import functions, mixins, utils


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

    def _get_raw_distinct(self, sql_params):
        if not self._distinct:
            return ""
        raw_distinct = " DISTINCT"
        if self._distinct_on_list:
            raw_distinct += " ON ({})".format(
                ", ".join(
                    [
                        utils._get_sql(utils._get_field_or_expression(d), sql_params=sql_params)
                        for d in self._distinct_on_list
                    ]
                )
            )
        return raw_distinct

    def _get_raw_select(self, sql_params):
        if not self._select_list:
            return " *"
        raw_select = " {}".format(
            ", ".join(
                [
                    utils._get_sql(utils._get_field_or_expression(s), sql_params=sql_params)
                    for s in self._select_list
                ]
            )
        )
        return raw_select

    def _get_raw_from(self, sql_params):
        if self._from is None:
            return ""
        return " FROM {}".format(
            utils._get_sql(utils._get_field_or_expression(self._from), sql_params=sql_params)
        )

    def _get_raw_prewhere(self, sql_params):
        _list = self._prewhere_list + [
            utils._extract_condition(k, utils._get_expression(v)) for k, v in self._prewhere_dict.items()
        ]

        if not _list:
            return ""
        raw_prewhere = " PREWHERE {}".format(utils._get_sql(functions.And(*_list), sql_params=sql_params))
        return raw_prewhere

    def _get_raw_where(self, sql_params):
        _list = self._where_list + [
            utils._extract_condition(k, utils._get_expression(v)) for k, v in self._where_dict.items()
        ]

        if not _list:
            return ""
        raw_where = " WHERE {}".format(utils._get_sql(functions.And(*_list), sql_params=sql_params))
        return raw_where

    def _get_raw_group_by(self, sql_params):
        if not self._group_by_list:
            return ""
        raw_group_by = " GROUP BY {}".format(
            ", ".join(
                [
                    utils._get_sql(utils._get_field_or_expression(g), sql_params=sql_params)
                    for g in self._group_by_list
                ]
            )
        )
        return raw_group_by

    def _get_raw_having(self, sql_params):
        _list = self._having_list + [
            utils._extract_condition(k, utils._get_expression(v)) for k, v in self._having_dict.items()
        ]

        if not _list:
            return ""
        raw_having = " HAVING {}".format(utils._get_sql(functions.And(*_list), sql_params=sql_params))
        return raw_having

    def _get_raw_order_by(self, sql_params):
        if not self._order_by_list:
            return ""
        raw_order_by = " ORDER BY {}".format(
            ", ".join(
                [
                    utils._get_sql(utils._get_field_or_expression(o), sql_params=sql_params)
                    for o in self._order_by_list
                ]
            )
        )
        return raw_order_by

    def _get_raw_limit_by(self, sql_params):
        if self._limit_by is None:
            return ""
        limit, offset, by = self._limit_by
        raw_limit_by = " LIMIT {}".format(utils._get_sql(utils._get_expression(limit), sql_params=sql_params))
        if offset is not None:
            raw_limit_by += " OFFSET {}".format(
                utils._get_sql(utils._get_expression(offset), sql_params=sql_params)
            )
        raw_limit_by += " BY {}".format(
            ", ".join([utils._get_sql(utils._get_field_or_expression(b), sql_params=sql_params) for b in by])
        )
        return raw_limit_by

    def _get_raw_limit(self, sql_params):
        if self._limit is None:
            return ""
        limit, offset = self._limit
        raw_limit = " LIMIT {}".format(utils._get_sql(utils._get_expression(limit), sql_params=sql_params))
        if offset is not None:
            raw_limit += " OFFSET {}".format(
                utils._get_sql(utils._get_expression(offset), sql_params=sql_params)
            )
        return raw_limit

    def __sql__(self, sql_params):
        str_format = (
            "SELECT{distinct}{select}{from_}{prewhere}{where}{group_by}{having}{order_by}{limit_by}{limit}"
        )
        return str_format.format(
            distinct=self._get_raw_distinct(sql_params=sql_params),
            select=self._get_raw_select(sql_params=sql_params),
            from_=self._get_raw_from(sql_params=sql_params),
            prewhere=self._get_raw_prewhere(sql_params=sql_params),
            where=self._get_raw_where(sql_params=sql_params),
            group_by=self._get_raw_group_by(sql_params=sql_params),
            having=self._get_raw_having(sql_params=sql_params),
            order_by=self._get_raw_order_by(sql_params=sql_params),
            limit_by=self._get_raw_limit_by(sql_params=sql_params),
            limit=self._get_raw_limit(sql_params=sql_params),
        )

    def run(self, settings=None):
        sql_params = {}
        sql = utils._get_sql(self, sql_params=sql_params)
        print(sql, sql_params)


class Table(mixins.ASMixin, mixins.JoinableMixin, mixins.ExpressionMixin):
    _AS_FORCE = True

    class Meta:
        pass

    @property
    def queryset(self):
        return QuerySet().from_(self)

    def get_table(self, sql_params):
        return getattr(self.Meta, "table_name")

    def __sql__(self, sql_params):
        sql = self.get_table(sql_params=sql_params)
        return sql


class Subquery(Table):
    def __init__(self, inner_queryset):
        self._inner_queryset = inner_queryset

    def get_table(self, sql_params):
        return "({inner_queryset})".format(
            inner_queryset=utils._get_sql(self._inner_queryset, sql_params=sql_params)
        )


class _Null(mixins.ExpressionMixin):
    def __sql__(self, sql_params):
        return "NULL"


NULL = _Null()


class _F:
    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, sql_params):
        return str(self.arg)

    def add(self, arg):
        return _F(self.arg + arg)


class F(mixins.ASMixin, mixins.ArithmeticMixin, mixins.ExpressionMixin):
    def __init__(self, arg):
        self.arg = arg.split("__") if isinstance(arg, str) else arg

    def __sql__(self, sql_params):
        field, *operators = self.arg
        field = _F(field)
        for op in operators:
            field = utils._apply_operator(field, op)
        sql = utils._get_sql(field, sql_params=sql_params)

        return sql


class Value(mixins.ASMixin, mixins.ArithmeticMixin, mixins.ExpressionMixin):
    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, sql_params):
        p = utils._GetP.p
        if isinstance(self.arg, str):
            sql_params[p] = self.arg
            return "%({p})s".format(p=p)
        if isinstance(self.arg, (int, float)):
            sql_params[p] = self.arg
            return "%({p})f".format(p=p)
        raise Exception("Value not Valid")
