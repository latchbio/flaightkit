from typing import Any, Dict, Tuple, Type


class TypeWithMetadata:
    ...


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
    class TTypeWithMetadata(type, TypeWithMetadata):
        __flyte_metadata__ = data

    return TTypeWithMetadata
