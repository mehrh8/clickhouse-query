from clickhouse_query.functions import base

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

class _FuncArrays(base.Func):
    def __init__(self, *arrays):
        super().__init__(*arrays)


class _Func1Arrays(base.Func):
    def __init__(self, array):
        super().__init__(array)

class _Func2Arrays(base.Func):
    def __init__(self, array1, array2):
        super().__init__(array1, array2)

class _Func1Arrays1Args(base.Func):
    def __init__(self, array, arg):
        super().__init__(array, arg)

class _FuncArraysFunc(base.Func):
    def __init__(self, func, *arrays):
        if func is None:
            super().__init__(*arrays)
        else:
            super().__init__(func, *arrays)

class _FuncArraysOptionalFunc(base.Func):
    def __init__(self, *arrays, func=None):
        if func is None:
            super().__init__(*arrays)
        else:
            super().__init__(func, *arrays)

class Array(base.Func):
    function = "array"

class ArrayWithConstant(base.Func):
    function = "arrayWithConstant"

    def __init__(self, length, elem):
        super().__init__(length, elem)

class ArrayConcat(_FuncArrays):
    function = "arrayConcat"

class ArrayElement(_Func1Arrays1Args):
    function = "arrayElement"

class Has(_Func1Arrays1Args):
    function = "has"

class HasAll(_Func2Arrays):
    function = "hasAll"

class HasAny(_Func2Arrays):
    function = "hasAny"

class HasSubstr(_Func2Arrays):
    function = "hasSubstr"

class IndexOf(_Func1Arrays1Args):
    function = "indexOf"

class ArrayCount(_FuncArraysOptionalFunc):
    function = "arrayCount"

class CountEqual(_Func1Arrays1Args):
    function = "countEqual"

class ArrayEnumerate(_Func1Arrays):
    function = "arrayEnumerate"

class ArrayEnumerateUniq(_FuncArrays):
    function = "arrayEnumerateUniq"

class ArrayPopBack(_Func1Arrays):
    function = "arrayPopBack"

class ArrayPopFront(_Func1Arrays):
    function = "arrayPopFront"

class ArrayPushBack(_Func1Arrays1Args):
    function = "arrayPushBack"

class ArrayPushFront(_Func1Arrays1Args):
    function = "arrayPushFront"

class ArrayResize(base.Func):
    function = "arrayResize"

    def __init__(self, array, size, extender=None):
        if extender is None:
            super().__init__(array, size)
        else:
            super().__init__(array, size, extender)

class ArraySlice(base.Func):
    function = "arraySlice"

    def __init__(self, array, offset, length=None):
        if length is None:
            super().__init__(array, offset)
        else:
            super().__init__(array, offset, length)

class ArraySort(_FuncArraysOptionalFunc):
    function = "arraySort"

class ArrayPartialSort(base.Func):
    function = "arrayPartialSort"

    def __init__(self, *arrays, limit, func=None):
        if func is None:
            super().__init__(limit, *arrays)
        else:
            super().__init__(func, limit, *arrays)

class ArrayReverseSort(_FuncArraysOptionalFunc):
    function = "arrayReverseSort"

class ArrayPartialReverseSort(base.Func):
    function = "arrayPartialReverseSort"

    def __init__(self, *arrays, limit, func=None):
        if func is None:
            super().__init__(limit, *arrays)
        else:
            super().__init__(func, limit, *arrays)

class ArrayUniq(_FuncArrays):
    function = "arrayUniq"

class ArrayJoin(_Func1Arrays):
    function = "arrayJoin"

class ArrayDifference(_Func1Arrays):
    function = "arrayDifference"

class ArrayDistinct(_Func1Arrays):
    function = "arrayDistinct"

class ArrayEnumerateDense(_Func1Arrays):
    function = "arrayEnumerateDense"

class ArrayIntersect(_FuncArrays):
    function = "arrayIntersect"

class ArrayReduce(base.Func):
    function = "arrayReduce"

    def __init__(self, *args):
        raise NotImplementedError

class ArrayReduceInRanges(base.Func):
    function = "arrayReduceInRanges"

    def __init__(self, *args):
        raise NotImplementedError

class ArrayReverse(_Func1Arrays):
    function = "arrayReverse"

class Reverse(_Func1Arrays):
    function = "reverse"

class ArrayFlatten(_Func1Arrays):
    function = "arrayFlatten"

class ArrayCompact(_Func1Arrays):
    function = "arrayCompact"

class ArrayZip(_FuncArrays):
    function = "arrayZip"

class ArrayAUC(base.Func):
    function = "arrayAUC"

    def __init__(self, array_scores, array_labels):
        super().__init__(array_scores, array_labels)

class ArrayMap(_FuncArraysFunc):
    function = "arrayMap"

class ArrayFilter(_FuncArraysFunc):
    function = "arrayFilter"

class ArrayFill(_FuncArraysFunc):
    function = "arrayFill"

class ArrayReverseFill(_FuncArraysFunc):
    function = "arrayReverseFill"

class ArraySplit(_FuncArraysFunc):
    function = "arraySplit"

class ArrayReverseSplit(_FuncArraysFunc):
    function = "arrayReverseSplit"

class ArrayExists(_FuncArraysOptionalFunc):
    function = "arrayExists"

class ArrayAll(_FuncArraysOptionalFunc):
    function = "arrayAll"

class ArrayFirst(_FuncArraysFunc):
    function = "arrayFirst"

class ArrayLast(_FuncArraysFunc):
    function = "arrayLast"

class ArrayFirstIndex(_FuncArraysFunc):
    function = "arrayFirstIndex"

class ArrayLastIndex(_FuncArraysFunc):
    function = "arrayLastIndex"

class ArrayMin(_FuncArraysFunc):
    function = "arrayMin"

class ArrayMax(_FuncArraysFunc):
    function = "arrayMax"

class ArraySum(_FuncArraysFunc):
    function = "arraySum"

class ArrayAvg(_FuncArraysFunc):
    function = "arrayAvg"

class ArrayCumSum(_FuncArraysFunc):
    function = "arrayCumSum"

class ArrayCumSumNonNegative(_FuncArraysFunc):
    function = "arrayCumSumNonNegative"

class ArrayProduct(_FuncArraysFunc):
    function = "arrayProduct"


