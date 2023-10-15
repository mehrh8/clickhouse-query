class ASMixin:
    _AS_FORCE = False

    def as_(self, as_):
        self._as = as_
        return self

    def _get_as(self):
        from clickhouse_query import utils

        as_ = getattr(self, "_as", None)
        if as_ is None and self._AS_FORCE:
            as_ = utils._GetAs.as_
            self.as_(as_)
        return as_

    def has_as(self):
        return self._get_as() is not None

    def __asmixin__(self):
        as_ = self._get_as()
        if as_ is not None:
            return " AS {as_}".format(as_=as_)
        return ""


class ArithmeticMixin:
    def __add__(self, other):
        from . import functions

        return functions.Plus(self, other)

    def __sub__(self, other):
        from . import functions

        return functions.Minus(self, other)

    def __mul__(self, other):
        from . import functions

        return functions.Multiply(self, other)

    def __truediv__(self, other):
        from . import functions

        return functions.Divide(self, other)

    def __floordiv__(self, other):
        from . import functions

        # TODO: cast self to Float64
        return functions.IntDiv(self, other)

    def __mod__(self, other):
        from . import functions

        return functions.Modulo(self, other)

    def __neg__(self):
        from . import functions

        return functions.Negate(self)


class ExpressionMixin:
    pass


class JoinableMixin:
    def inner_join(self, other, on=None, using=None):
        from clickhouse_query.joins import InnerJoin
        from clickhouse_query import utils, models

        if using is not None:
            using = [models.F(u) if isinstance(u, str) else utils._get_expression(u) for u in using]
        return InnerJoin(self, other, on=on, using=using)
