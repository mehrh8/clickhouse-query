import copy


class ASMixin:
    _AS_FORCE = False

    def as_(self, as_):
        self = copy.deepcopy(self)
        self._as = as_
        return self

    def _get_as(self, uid_generator):
        as_ = getattr(self, "_as", None)
        if as_ is None and self._AS_FORCE:
            if not hasattr(self, "uid_generator_id") or id(uid_generator) != self.uid_generator_id:
                self.uid_generator_id = id(uid_generator)
                self.uid = uid_generator.get()
            return self.uid
        return as_

    def has_as(self, uid_generator):
        return self._get_as(uid_generator=uid_generator) is not None

    def __asmixin__(self, uid_generator):
        as_ = self._get_as(uid_generator=uid_generator)
        if as_ is not None:
            return " AS {as_}".format(as_=as_)
        return ""


class ArithmeticMixin:
    def __add__(self, other):
        from clickhouse_query import functions

        return functions.Plus(self, other)

    def __sub__(self, other):
        from clickhouse_query import functions

        return functions.Minus(self, other)

    def __mul__(self, other):
        from clickhouse_query import functions

        return functions.Multiply(self, other)

    def __truediv__(self, other):
        from clickhouse_query import functions

        return functions.Divide(self, other)

    def __floordiv__(self, other):
        from clickhouse_query import functions

        # TODO: cast self to Float64
        return functions.IntDiv(self, other)

    def __mod__(self, other):
        from clickhouse_query import functions

        return functions.Modulo(self, other)

    def __neg__(self):
        from clickhouse_query import functions

        return functions.Negate(self)
