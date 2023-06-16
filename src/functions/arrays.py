from . import base

class Empty(base._Func1Args):
    function = "empty"

class NotEmpty(base._Func1Args):
    function = "notEmpty"

class Length(base._Func1Args):
    function = "length"

class EmptyArrayUInt8(base._Func0Args):
    function = "emptyArrayUInt8"

class EmptyArrayUInt16(base._Func0Args):
    function = "emptyArrayUInt16"

class EmptyArrayUInt32(base._Func0Args):
    function = "emptyArrayUInt32"

class EmptyArrayUInt64(base._Func0Args):
    function = "emptyArrayUInt64"

class EmptyArrayInt8(base._Func0Args):
    function = "emptyArrayInt8"

class EmptyArrayInt16(base._Func0Args):
    function = "emptyArrayInt16"

class EmptyArrayInt32(base._Func0Args):
    function = "emptyArrayInt32"

class EmptyArrayInt64(base._Func0Args):
    function = "emptyArrayInt64"

class EmptyArrayFloat32(base._Func0Args):
    function = "emptyArrayFloat32"

class EmptyArrayFloat64(base._Func0Args):
    function = "emptyArrayFloat64"

class EmptyArrayDate(base._Func0Args):
    function = "emptyArrayDate"

class EmptyArrayDateTime(base._Func0Args):
    function = "emptyArrayDateTime"

class EmptyArrayString(base._Func0Args):
    function = "emptyArrayString"

class EmptyArrayToSingle(base._Func1Args):
    function = "emptyArrayToSingle"

class Range(base.Func):
    function = "range"

    def __init__(self, arg1, arg2=None, arg3=None):
        args = [arg for arg in [arg1, arg2, arg3] if arg is not None]
        super().__init__(*args)

class Array(base.Func):
    function = "array"

class ArrayWithConstant(base.Func):
    function = "arrayWithConstant"

    def __init__(self, length, elem):
        super().__init__(length, elem)

class ArrayConcat(base.Func):
    function = "arrayConcat"

class ArrayElement(base.Func):
    function = "arrayElement"

    def __init__(self, arr, n):
        super().__init__(arr, n)

class Has(base.Func):
    function = "has"

    def __init__(self, arr, elem):
        super().__init__(arr, elem)

class _Func2Arrs(base.Func):
    def __init__(self, arr1, arr2):
        super().__init__(arr1, arr2)

class HasAll(_Func2Arrs):
    function = "hasAll"


class HasAny(_Func2Arrs):
    function = "hasAny"

class HasSubstr(_Func2Arrs):
    function = "hasSubstr"

class IndexOf(base.Func):
    function = "indexOf"

    def __init__(self, arr, elem):
        super().__init__(arr, elem)

class ArrayCount(base.Func):
    pass # TODO

