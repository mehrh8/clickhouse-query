"""
QuerySet implementation for ClickHouse database.
"""

import copy

from clickhouse_query import utils
from clickhouse_query.core.expressions import Q


class OrderBy:
    """Class for handling ORDER BY expressions."""

    def __init__(self, expr, asc=True):
        self.expr = expr
        self.asc = asc

    def __sql__(self, *, uid_generator):
        sql, sql_params = utils.get_sql(self.expr, uid_generator=uid_generator)
        return "{} {}".format(sql, "ASC" if self.asc else "DESC"), sql_params

    @classmethod
    def get_from_str(cls, arg):
        if arg.startswith("-"):
            expr = utils.get_expression(arg[1:], str_is_field=True)
            asc = False
        else:
            expr = utils.get_expression(arg, str_is_field=True)
            asc = True
        return cls(expr, asc=asc)


class QuerySet:
    """QuerySet for building and executing ClickHouse queries."""

    def __init__(self, _from=None, inplace=False, clickhouse_client=None):
        self._from = None
        self._select = []
        self._distinct = None
        self._prewhere = []
        self._where = []
        self._group_by = []
        self._having = []
        self._order_by = []
        self._limit = None
        self._limit_by = None

        self.inplace = inplace
        self.clickhouse_client = clickhouse_client

        self.enable_inplace().from_(_from).disable_inplace()

    def _clone(self):
        if not self.inplace:
            self = copy.deepcopy(self)
        return self

    def disable_inplace(self):
        self.inplace = False
        return self

    def enable_inplace(self):
        self.inplace = True
        return self

    def select(self, *args, **kwargs):
        self = self._clone()

        if args and args[0] is None:
            self._select = []
            return self

        self._select = []
        for item in args:
            expression = utils.get_expression(item, str_is_field=True)
            self._select.append(expression)
        for as_, item in kwargs.items():
            expression = utils.get_expression(item, str_is_field=True)
            expression = expression.as_(as_)
            self._select.append(expression)
        return self

    def distinct(self, *args):
        self = self._clone()

        if args and args[0] is None:
            self._distinct = None
            return self

        self._distinct = []
        for item in args:
            expression = utils.get_expression(item, str_is_field=True)
            self._distinct.append(expression)
        return self

    def from_(self, from_):
        self = self._clone()

        if from_ is None:
            self._from = None
            return self

        self._from = utils.get_expression(from_, str_is_field=True)
        return self

    def prewhere(self, *args, **kwargs):
        self = self._clone()

        if args and args[0] is None:
            self._prewhere = []
            return self

        prewhere_dict_expr = {k: utils.get_expression(v) for k, v in kwargs.items()}
        prewhere_condition_list = [utils._extract_condition(k, v) for k, v in prewhere_dict_expr.items()]
        self._prewhere += list(args) + prewhere_condition_list
        return self

    def where(self, *args, **kwargs):
        self = self._clone()

        if args and args[0] is None:
            self._where = []
            return self

        where_dict_expr = {k: utils.get_expression(v) for k, v in kwargs.items()}
        where_condition_list = [utils._extract_condition(k, v) for k, v in where_dict_expr.items()]
        self._where += list(args) + where_condition_list
        return self

    def group_by(self, *args):
        self = self._clone()

        if args and args[0] is None:
            self._group_by = []
            return self

        self._group_by = []
        for item in args:
            expression = utils.get_expression(item, str_is_field=True)
            self._group_by.append(expression)
        return self

    def having(self, *args, **kwargs):
        self = self._clone()

        if args and args[0] is None:
            self._having = []
            return self

        having_dict_expr = {k: utils.get_expression(v) for k, v in kwargs.items()}
        having_condition_list = [utils._extract_condition(k, v) for k, v in having_dict_expr.items()]
        self._having += list(args) + having_condition_list
        return self

    def order_by(self, *args):
        self = self._clone()

        if args and args[0] is None:
            self._order_by = []
            return self

        self._order_by = []
        for item in args:
            if not isinstance(item, OrderBy):
                item = OrderBy.get_from_str(item)
            self._order_by.append(item)
        return self

    def limit(self, limit, *, offset=None):
        self = self._clone()

        if limit is None and offset is None:
            self._limit = None
            return self

        if limit is not None:
            limit = utils.get_expression(limit)
        if offset is not None:
            offset = utils.get_expression(offset)

        self._limit = (limit, offset)
        return self

    def limit_by(self, limit, *by, offset=None):
        self = self._clone()

        if limit is None:
            self._limit_by = None
            return self

        limit = utils.get_expression(limit)
        if offset is not None:
            offset = utils.get_expression(offset)

        _by = []
        for item in by:
            expression = utils.get_expression(item, str_is_field=True)
            _by.append(expression)

        self._limit_by = (limit, offset, _by)
        return self

    def _get_select_sql(self, uid_generator):
        if not self._select:
            return " *", {}

        sql_params = {}
        sqls = []
        for expression in self._select:
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        select_sql = " {}".format(", ".join(sqls))
        return select_sql, sql_params

    def _get_distinct_sql(self, uid_generator):
        if self._distinct is None:
            return "", {}
        distinct_sql = " DISTINCT"
        sql_params = {}
        if self._distinct:
            sqls = []
            for expression in self._distinct:
                sql, params = utils.get_sql(expression, uid_generator=uid_generator)
                sqls.append(sql)
                sql_params.update(params)

            distinct_sql += " ON ({})".format(", ".join(sqls))
        return distinct_sql, sql_params

    def _get_from_sql(self, uid_generator):
        if self._from is None:
            return "", {}

        expression = self._from
        sql, sql_params = utils.get_sql(expression, uid_generator=uid_generator)
        from_sql = " FROM {}".format(sql)
        return from_sql, sql_params

    def _get_prewhere_sql(self, uid_generator):
        if not self._prewhere:
            return "", {}

        sql, sql_params = utils.get_sql(Q(*self._prewhere), uid_generator=uid_generator)
        prewhere_sql = " PREWHERE {}".format(sql)
        return prewhere_sql, sql_params

    def _get_where_sql(self, uid_generator):
        if not self._where:
            return "", {}

        sql, sql_params = utils.get_sql(Q(*self._where), uid_generator=uid_generator)
        where_sql = " WHERE {}".format(sql)
        return where_sql, sql_params

    def _get_group_by_sql(self, uid_generator):
        if not self._group_by:
            return "", {}

        sql_params = {}
        sqls = []
        for expression in self._group_by:
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        group_by_sql = " GROUP BY {}".format(", ".join(sqls))
        return group_by_sql, sql_params

    def _get_having_sql(self, uid_generator):
        if not self._having:
            return "", {}

        sql, sql_params = utils.get_sql(Q(*self._having), uid_generator=uid_generator)
        having_sql = " HAVING {}".format(sql)
        return having_sql, sql_params

    def _get_order_by_sql(self, uid_generator):
        if not self._order_by:
            return "", {}

        sql_params = {}
        sqls = []
        for expression in self._order_by:
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        order_by_sql = " ORDER BY {}".format(", ".join(sqls))
        return order_by_sql, sql_params

    def _get_limit_sql(self, uid_generator):
        if self._limit is None or self._limit == (None, None):
            return "", {}

        limit_expr, offset_expr = self._limit
        sql_params = {}

        if limit_expr is not None:
            _limit_sql, limit_params = utils.get_sql(limit_expr, uid_generator=uid_generator)
            limit_sql = " LIMIT {}".format(_limit_sql)
            sql_params.update(limit_params)

        if offset_expr is not None:
            _offset_sql, offset_params = utils.get_sql(offset_expr, uid_generator=uid_generator)
            limit_sql += " OFFSET {}".format(_offset_sql)
            sql_params.update(offset_params)

        return limit_sql, sql_params

    def _get_limit_by_sql(self, uid_generator):
        if self._limit_by is None:
            return "", {}

        limit_expr, offset_expr, by_exprs = self._limit_by
        sql_params = {}

        _limit_sql, limit_params = utils.get_sql(limit_expr, uid_generator=uid_generator)
        limit_by_sql = " LIMIT {}".format(_limit_sql)
        sql_params.update(limit_params)

        if offset_expr is not None:
            _offset_sql, offset_params = utils.get_sql(offset_expr, uid_generator=uid_generator)
            limit_by_sql += " OFFSET {}".format(_offset_sql)
            sql_params.update(offset_params)

        _by_sqls = []
        for expression in by_exprs:
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            _by_sqls.append(sql)
            sql_params.update(params)
        limit_by_sql += " BY {}".format(", ".join(_by_sqls))

        return limit_by_sql, sql_params

    def __sql__(self, *, uid_generator):
        distinct_sql, distinct_params = self._get_distinct_sql(uid_generator=uid_generator)
        select_sql, select_params = self._get_select_sql(uid_generator=uid_generator)
        from_sql, from_params = self._get_from_sql(uid_generator=uid_generator)
        prewhere_sql, prewhere_params = self._get_prewhere_sql(uid_generator=uid_generator)
        where_sql, where_params = self._get_where_sql(uid_generator=uid_generator)
        group_by_sql, group_by_params = self._get_group_by_sql(uid_generator=uid_generator)
        having_sql, having_params = self._get_having_sql(uid_generator=uid_generator)
        order_by_sql, order_by_params = self._get_order_by_sql(uid_generator=uid_generator)
        limit_by_sql, limit_by_params = self._get_limit_by_sql(uid_generator=uid_generator)
        limit_sql, limit_params = self._get_limit_sql(uid_generator=uid_generator)

        sql_params = {}
        sql_params.update(distinct_params)
        sql_params.update(select_params)
        sql_params.update(from_params)
        sql_params.update(prewhere_params)
        sql_params.update(where_params)
        sql_params.update(group_by_params)
        sql_params.update(having_params)
        sql_params.update(order_by_params)
        sql_params.update(limit_by_params)
        sql_params.update(limit_params)

        str_format = (
            "SELECT{distinct}{select}{from_}{prewhere}{where}{group_by}{having}{order_by}{limit_by}{limit}"
        )
        sql = str_format.format(
            distinct=distinct_sql,
            select=select_sql,
            from_=from_sql,
            prewhere=prewhere_sql,
            where=where_sql,
            group_by=group_by_sql,
            having=having_sql,
            order_by=order_by_sql,
            limit_by=limit_by_sql,
            limit=limit_sql,
        )
        return sql, sql_params

    def execute(self, clickhouse_client=None):
        if clickhouse_client is None:
            clickhouse_client = self.clickhouse_client

        if callable(clickhouse_client):
            clickhouse_client = clickhouse_client()

        sql, sql_params = utils.get_sql(self)

        values_list, column__type = clickhouse_client.execute(sql, params=sql_params, with_column_types=True)

        data = []
        for values in values_list:
            data.append({column: value for (column, type), value in zip(column__type, values, strict=False)})

        return data


__all__ = [
    "QuerySet",
    "OrderBy",
]
