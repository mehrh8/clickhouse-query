from utils import ASMixin


class TwoArgs:
    def __init__(self, left_arg, right_arg):
        self.left_arg = left_arg
        self.right_arg = right_arg


class LT(TwoArgs):

    def __sql__(self):
        return f"{self.left_arg} < '{str(self.right_arg)}'"


class GT(TwoArgs):

    def __sql__(self):
        return f"{self.left_arg} > '{str(self.right_arg)}'"


class LE(TwoArgs):

    def __sql__(self):
        return f"{self.left_arg} <= '{str(self.right_arg)}'"


class GE(TwoArgs):

    def __sql__(self):
        return f"{self.left_arg} >= '{str(self.right_arg)}'"


class In:
    def __init__(self, arg_name, args):
        self.arg_name = arg_name
        self.args = args

    def __sql__(self):
        sql = "{arg_name} IN ({args})".format(arg_name=self.arg_name, args=", ".join(map(str, self.args)))
        return sql


class Column(ASMixin):
    """ Class to represent column in clickhouse table"""

    def __init__(self, name: str):
        self.name = name

    def __sql__(self):
        sql = self.name
        as_ = self.get_as()
        if as_ is not None:
            sql += " AS {}".format(as_)
        return sql

    # overload < operator
    def __lt__(self, other):
        return LT(left_arg=self.name, right_arg=other)

    # overload > operator
    def __gt__(self, other):
        return GT(left_arg=self.name, right_arg=other)

    # overload <= operator
    def __le__(self, other):
        return LE(left_arg=self.name, right_arg=other)

    # overload >= operator
    def __ge__(self, other):
        return GE(left_arg=self.name, right_arg=other)

    def in_(self, *args):
        return In(arg_name=self.name, args=args)
