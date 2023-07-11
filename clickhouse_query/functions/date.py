from clickhouse_query.functions import base

class ToYear(base._Func1Args):
    function = "toYear"

class ToQuarter(base._Func1Args):
    function = "toQuarter"

class ToMonth(base._Func1Args):
    function = "toMonth"

class ToDayOfYear(base._Func1Args):
    function = "toDayOfYear"

class ToDayOfWeek(base._Func1Args):
    function = "toDayOfWeek"

class ToHour(base._Func1Args):
    function = "toHour"

class ToMinute(base._Func1Args):
    function = "toMinute"

class ToSecond(base._Func1Args):
    function = "toSecond"

class ToTime(base._Func1Args):
    function = "toTime"

class ToWeek(base.Func):
    function = "toWeek"

    def __init__(self, arg, mode=0, timezone=None):
        if timezone is None:
            super().__init__(arg, mode)
        else:
            super().__init__(arg, mode, timezone)