from clickhouse_query.functions import base

class Equals(base._Func2Args):
    function = "equals"

class NotEquals(base._Func2Args):
    function = "notEquals"

class Less(base._Func2Args):
    function = "less"

class Greater(base._Func2Args):
    function = "greater"

class LessOrEquals(base._Func2Args):
    function = "lessOrEquals"

class GreaterOrEquals(base._Func2Args):
    function = "greaterOrEquals"