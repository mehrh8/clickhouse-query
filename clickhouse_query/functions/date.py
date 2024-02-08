from clickhouse_query.functions import base


class ToYear(base._Function1Args):
    function = "toYear"


class ToQuarter(base._Function1Args):
    function = "toQuarter"


class ToMonth(base._Function1Args):
    function = "toMonth"


class ToDayOfYear(base._Function1Args):
    function = "toDayOfYear"


class ToDayOfWeek(base._Function1Args):
    function = "toDayOfWeek"


class ToHour(base._Function1Args):
    function = "toHour"


class ToMinute(base._Function1Args):
    function = "toMinute"


class ToSecond(base._Function1Args):
    function = "toSecond"


class ToTime(base._Function1Args):
    function = "toTime"


class ToWeek(base.Function):
    function = "toWeek"

    def __init__(self, arg, mode=None):
        if mode is None:
            super().__init__(arg)
        else:
            super().__init__(arg, mode)
