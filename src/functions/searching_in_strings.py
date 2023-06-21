from . import base

class Like(base._Func2Args):
    function = "like"

class ILike(base._Func2Args):
    function = "ilike"