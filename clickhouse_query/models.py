import datetime
from zoneinfo import ZoneInfo

from clickhouse_query import functions, mixins, utils


class QuerySet:
    def __init__(self):
        self._from = None
        self._select_list = []
        self._select_dict = []
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

    def select(self, *args, **kwargs):
        self._select_list = list(args)
        self._select_dict = dict(kwargs)
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

    def _get_distinct_sql(self, uid_generator):
        if not self._distinct:
            return "", {}
        distinct_sql = " DISTINCT"
        sql_params = {}
        if self._distinct_on_list:
            sqls = []
            for item in self._distinct_on_list:
                expression = utils.get_expression(item, str_is_field=True)
                sql, params = utils.get_sql(expression, uid_generator=uid_generator)
                sqls.append(sql)
                sql_params.update(params)

            distinct_sql += " ON ({})".format(", ".join(sqls))
        return distinct_sql, sql_params

    def _get_select_sql(self, uid_generator):
        if not self._select_list and not self._select_dict:
            return " *", {}

        sql_params = {}
        sqls = []
        for item in self._select_list:
            expression = utils.get_expression(item, str_is_field=True)
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        for as_, item in self._select_dict.items():
            expression = utils.get_expression(item, str_is_field=True)
            expression.as_(as_)
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        select_sql = " {}".format(", ".join(sqls))
        return select_sql, sql_params

    def _get_from_sql(self, uid_generator):
        if self._from is None:
            return "", {}

        expression = utils.get_expression(self._from, str_is_field=True)
        sql, sql_params = utils.get_sql(expression, uid_generator=uid_generator)
        from_sql = " FROM {}".format(sql)
        return from_sql, sql_params

    def _get_prewhere_sql(self, uid_generator):

        prewhere_dict_expr = {k: utils.get_expression(v) for k, v in self._prewhere_dict.items()}
        prewhere_dict_condition_list = [utils._extract_condition(k, v) for k, v in prewhere_dict_expr.items()]
        condition_list = self._prewhere_list + prewhere_dict_condition_list

        if not condition_list:
            return "", {}

        sql, sql_params = utils.get_sql(functions.And(*condition_list), uid_generator=uid_generator)
        prewhere_sql = " PREWHERE {}".format(sql)
        return prewhere_sql, sql_params

    def _get_where_sql(self, uid_generator):
        where_dict_expr = {k: utils.get_expression(v) for k, v in self._where_dict.items()}
        where_dict_condition_list = [utils._extract_condition(k, v) for k, v in where_dict_expr.items()]
        condition_list = self._where_list + where_dict_condition_list

        if not condition_list:
            return "", {}

        sql, sql_params = utils.get_sql(functions.And(*condition_list), uid_generator=uid_generator)
        where_sql = " WHERE {}".format(sql)
        return where_sql, sql_params

    def _get_group_by_sql(self, uid_generator):
        if not self._group_by_list:
            return "", {}

        sql_params = {}
        sqls = []
        for item in self._group_by_list:
            expression = utils.get_expression(item, str_is_field=True)
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        group_by_sql = " GROUP BY {}".format(", ".join(sqls))
        return group_by_sql, sql_params

    def _get_having_sql(self, uid_generator):
        having_dict_expr = {k: utils.get_expression(v) for k, v in self._having_dict.items()}
        having_dict_condition_list = [utils._extract_condition(k, v) for k, v in having_dict_expr.items()]
        condition_list = self._having_list + having_dict_condition_list

        if not condition_list:
            return "", {}

        sql, sql_params = utils.get_sql(functions.And(*condition_list), uid_generator=uid_generator)
        having_sql = " HAVING {}".format(sql)
        return having_sql, sql_params

    def _get_order_by_sql(self, uid_generator):
        if not self._order_by_list:
            return "", {}

        sql_params = {}
        sqls = []
        for item in self._order_by_list:
            expression = utils.get_expression(item, str_is_field=True)
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            sqls.append(sql)
            sql_params.update(params)

        order_by_sql = " ORDER BY {}".format(", ".join(sqls))
        return order_by_sql, sql_params

    def _get_limit_by_sql(self, uid_generator):
        if self._limit_by is None:
            return "", {}

        limit, offset, by = self._limit_by
        sql_params = {}

        limit_expr = utils.get_expression(limit)
        _limit_sql, limit_params = utils.get_sql(limit_expr, uid_generator=uid_generator)
        limit_by_sql = " LIMIT {}".format(_limit_sql)
        sql_params.update(limit_params)

        if offset is not None:
            offset_expr = utils.get_expression(offset)
            _offset_sql, offset_params = utils.get_sql(offset_expr, uid_generator=uid_generator)
            limit_by_sql += " OFFSET {}".format(_offset_sql)
            sql_params.update(offset_params)

        _by_sqls = []
        for item in by:
            expression = utils.get_expression(item, str_is_field=True)
            sql, params = utils.get_sql(expression, uid_generator=uid_generator)
            _by_sqls.append(sql)
            sql_params.update(params)
        limit_by_sql += " BY {}".format(", ".join(_by_sqls))

        return limit_by_sql, sql_params

    def _get_limit_sql(self, uid_generator):
        if self._limit is None:
            return "", {}

        limit, offset = self._limit
        sql_params = {}

        limit_expr = utils.get_expression(limit)
        _limit_sql, limit_params = utils.get_sql(limit_expr, uid_generator=uid_generator)
        limit_sql = " LIMIT {}".format(_limit_sql)
        sql_params.update(limit_params)

        if offset is not None:
            offset_expr = utils.get_expression(offset)
            _offset_sql, offset_params = utils.get_sql(offset_expr, uid_generator=uid_generator)
            limit_sql += " OFFSET {}".format(_offset_sql)
            sql_params.update(offset_params)

        return limit_sql, sql_params

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

    def execute(self, clickhouse_client):
        sql, sql_params = utils.get_sql(self)

        values_list, column__type = clickhouse_client.execute(sql, params=sql_params, with_column_types=True)

        data = []
        for values in values_list:
            data.append({column: value for (column, type), value in zip(column__type, values)})  # noqa: B905

        return data


class Subquery(mixins.ASMixin):
    def __init__(self, inner_queryset):
        self._inner_queryset = inner_queryset

    def __sql__(self, *, uid_generator):
        inner_queryset_sql, sql_params = utils.get_sql(self._inner_queryset, uid_generator=uid_generator)
        sql = "({inner_queryset})".format(inner_queryset=inner_queryset_sql)
        return sql, sql_params


class _Null:
    def __sql__(self, *, uid_generator):
        return "NULL", {}


NULL = _Null()


class _F:
    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, *, uid_generator):
        return str(self.arg), {}

    def _arg_extend(self, arg):  # TODO: rename
        return _F(self.arg + arg)


class F(mixins.ASMixin, mixins.ArithmeticMixin):
    def __init__(self, arg):
        self.arg = arg.split("__") if isinstance(arg, str) else arg

    def __sql__(self, *, uid_generator):
        field, *operators = self.arg
        field = _F(field)
        for op in operators:
            field = utils._apply_operator(field, op)

        sql, sql_params = utils.get_sql(field, uid_generator=uid_generator)
        return sql, sql_params


def _get_value_sql(arg):
    if isinstance(arg, str):
        return "%({uid})s", arg

    if isinstance(arg, (int, float)):
        return "%({uid})f", arg

    if isinstance(arg, datetime.datetime):
        return "%({uid})s", arg.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S.%f")

    if utils.is_iterable(arg):
        new_arg = tuple(_get_value_sql(a)[1] for a in arg)
        return "%({uid})s", new_arg

    raise Exception("Value not Valid", arg)


class Value(mixins.ASMixin, mixins.ArithmeticMixin):
    def __init__(self, arg):
        self.arg = arg

    def __sql__(self, *, uid_generator):
        uid = uid_generator.get()
        value_sql, value_param = _get_value_sql(self.arg)
        return value_sql.format(uid=uid), {uid: value_param}
