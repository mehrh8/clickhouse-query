import copy
import datetime
from zoneinfo import ZoneInfo

from clickhouse_query import Function, mixins, utils


class QuerySet:
    def __init__(self, inplace=False):
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
            expression.as_(as_)
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


class OrderBy:
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


class Q(Function):
    def __init__(self, *args, _connector="and", **kwargs) -> None:
        args = list(args)

        if len(args) + len(kwargs) == 0:
            raise ValueError("At least one argument is required")

        if len(args) + len(kwargs) == 1:
            _connector = ""

        for item, value in kwargs.items():
            expression = utils.get_expression(value)
            condition = utils._extract_condition(item, expression)
            args.append(condition)

        super().__init__(_connector)
        self(*args)
