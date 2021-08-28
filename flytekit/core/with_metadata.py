from typing import Any, Dict, Tuple, Type


class TypeWithMetadata:
    def __init__(self, type: Type, data: Dict[str, Any]):
        self._type = type
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def data(self):
        return self._data


class _WithMetadata:
    def __init__(self, getitem):
        self._getitem = getitem
        self._name = getitem.__name__
        self.__doc__ = getitem.__doc__

    def __getitem__(self, params: Tuple[Type, Dict[str, Any]]):
        type, data = params

        return self._getitem(self, type, data)


@_WithMetadata
def WithMetadata(self, type: Type, data: Dict[str, Any]):
    return TypeWithMetadata(type, data)
