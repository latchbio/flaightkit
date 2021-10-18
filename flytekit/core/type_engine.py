from __future__ import annotations

import dataclasses
import datetime as _datetime
import enum
import inspect
import json as _json
import mimetypes
import os
import typing
from abc import ABC, abstractmethod
from typing import Type

from google.protobuf import json_format as _json_format
from google.protobuf import reflection as _proto_reflection
from google.protobuf import struct_pb2 as _struct
from google.protobuf.json_format import MessageToDict as _MessageToDict
from google.protobuf.json_format import ParseDict as _ParseDict
from google.protobuf.struct_pb2 import Struct

from flytekit.common.types import primitives as _primitives
from flytekit.core.context_manager import FlyteContext
from flytekit.core.with_metadata import FlyteMetadata
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import (
    Blob,
    BlobMetadata,
    Literal,
    LiteralCollection,
    LiteralMap,
    Primitive,
    Record,
    Scalar,
    Void,
)
from flytekit.models.types import LiteralType, SimpleType

T = typing.TypeVar("T")


class TypeTransformer(typing.Generic[T]):
    """
    Base transformer type that should be implemented for every python native type that can be handled by flytekit
    """

    def __init__(self, name: str, t: Type[T], enable_type_assertions: bool = True):
        self._t = t
        self._name = name
        self._type_assertions_enabled = enable_type_assertions

    @property
    def name(self):
        return self._name

    @property
    def python_type(self) -> Type[T]:
        """
        This returns the python type
        """
        return self._t

    @property
    def type_assertions_enabled(self) -> bool:
        """
        Indicates if the transformer wants type assertions to be enabled at the core type engine layer
        """
        return self._type_assertions_enabled

    @abstractmethod
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Converts the python type to a Flyte LiteralType
        """
        raise NotImplementedError("Conversion to LiteralType should be implemented")

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        """
        Converts the Flyte LiteralType to a python object type.
        """
        raise ValueError("By default, transformers do not translate from Flyte types back to Python types")

    @abstractmethod
    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """
        Converts a given python_val to a Flyte Literal, assuming the given python_val matches the declared python_type.
        Implementers should refrain from using type(python_val) instead rely on the passed in python_type. If these
        do not match (or are not allowed) the Transformer implementer should raise an AssertionError, clearly stating
        what was the mismatch
        :param ctx: A FlyteContext, useful in accessing the filesystem and other attributes
        :param python_val: The actual value to be transformed
        :param python_type: The assumed type of the value (this matches the declared type on the function)
        :param expected: Expected Literal Type
        """
        raise NotImplementedError(f"Conversion to Literal for python type {python_type} not implemented")

    @abstractmethod
    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        """
        Converts the given Literal to a Python Type. If the conversion cannot be done an AssertionError should be raised
        :param ctx: FlyteContext
        :param lv: The received literal Value
        :param expected_python_type: Expected native python type that should be returned
        """
        raise NotImplementedError(
            f"Conversion to python value expected type {expected_python_type} from literal not implemented"
        )

    def __repr__(self):
        return f"{self._name} Transforms ({self._t}) to Flyte native"

    def __str__(self):
        return str(self.__repr__())


class SimpleTransformer(TypeTransformer[T]):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(
        self,
        name: str,
        t: Type[T],
        lt: LiteralType,
        to_literal_transformer: typing.Callable[[T], Literal],
        from_literal_transformer: typing.Callable[[Literal], T],
    ):
        super().__init__(name, t)
        self._lt = lt
        self._to_literal_transformer = to_literal_transformer
        self._from_literal_transformer = from_literal_transformer

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        return self._lt

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if hasattr(python_type, "__metadata__"):
            # typing.Annotated
            return self.to_literal(ctx, python_val, python_type.__origin__, expected)
        if type(python_val) != python_type:
            raise AssertionError(
                f"Could not convert value using simple transformer: '{type(python_val)}' != expected '{python_type}'"
            )
        return self._to_literal_transformer(python_val)

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        try:
            return self._from_literal_transformer(lv)
        except AttributeError:
            raise AssertionError(
                f"Could not convert value using simple transformer: '{lv}' to '{expected_python_type}'"
            )

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.simple is not None and literal_type.simple == self._lt.simple:
            return self.python_type
        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class RestrictedTypeError(Exception):
    pass


class RestrictedType(TypeTransformer[T], ABC):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: Type[T]):
        super().__init__(name, t)

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type {self.python_type} is restricted currently")


class DataclassTransformer(TypeTransformer[object]):
    """
    The Dataclass Transformer, provides a type transformer for arbitrary Python dataclasses, that have
    @dataclass decorators.
    """

    def __init__(self):
        super().__init__("Object-Dataclass-Transformer", object)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Extracts the Literal type definition for a Dataclass and returns a type Struct.
        If possible also extracts the JSONSchema for the dataclass.
        """
        fields = {}
        for f in dataclasses.fields(t):
            fields[f.name] = TypeEngine.to_literal_type(f.type)

        return LiteralType(record=_type_models.RecordType(field_types=fields))

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if not dataclasses.is_dataclass(python_val):
            raise AssertionError(
                f"{type(python_val)} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )

        if dataclasses.fields(python_val) != dataclasses.fields(python_type):
            raise AssertionError(f"Dataclass does not matched requested type: '{type(python_val)}' != '{python_type}'")

        if expected.record is None:
            raise AssertionError(f"Expected type is not a record: '{expected}'")

        res = {}
        for f in dataclasses.fields(python_type):
            v = getattr(python_val, f.name)
            res[f.name] = TypeEngine.to_literal(ctx, v, f.type, expected.record.field_types[f.name])

        return Literal(record=Record(res))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not dataclasses.is_dataclass(expected_python_type):
            raise AssertionError(
                f"{expected_python_type} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )

        if lv.record is None:
            raise AssertionError(f"Provided literal is not a record: '{lv}'")

        res = {}
        for f in dataclasses.fields(expected_python_type):
            if f.name not in lv.record.fields:
                raise AssertionError(f"Literal is missing a field: '{f.name}'")
            res[f.name] = TypeEngine.to_python_value(ctx, lv.record.fields[f.name], f.type)

        return expected_python_type(**res)


class UnionTransformer(TypeTransformer[typing.Union[typing.Any]]):
    def __init__(self):
        super().__init__("Union-Transformer", typing.Union)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        summands = [TypeEngine.to_literal_type(x) for x in t.__args__]
        return LiteralType(sum=_type_models.SumType(summands=summands))

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if expected.sum is None:
            raise AssertionError(f"Expected type is not a sum type: '{expected}'")

        typ = None
        val = None
        for t in python_type.__args__:
            try:
                ltyp = TypeEngine.to_literal_type(t)
                for s in expected.sum.summands:  # todo(maximsmol): O(n^2) algo, not sure if Type is hashable
                    if ltyp != s:
                        continue
                    typ = s

                if typ is None:
                    continue

                if t == type(None) and python_val is not None:
                    continue

                val = TypeEngine.to_literal(ctx, python_val, t, typ)
                break
            except Exception:
                continue

        if val is None or typ is None:
            raise ValueError(f"Could not find suitable union instantiation: '{python_val}' ('{python_type}')")

        return val

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        for x in expected_python_type.__args__:

            void_literal = Literal(scalar=Scalar(none_type=Void()))

            try:
                res = TypeEngine.to_python_value(ctx, lv, x)

                if (res is None and lv == void_literal and x == type(None)) or (res is not None and x != type(None)):
                    return res

            except Exception:
                pass

        raise AssertionError(
            f"Provided literal could not be cast to variant type: '{lv}' not one of '{expected_python_type}'"
        )

    def guess_python_type(self, literal_type: LiteralType) -> typing.Union[typing.Any]:
        if literal_type.sum is not None:
            return typing.Union[tuple(TypeEngine.guess_python_type(x) for x in literal_type.sum.summands)]
        raise ValueError(f"Enum transformer cannot reverse {literal_type}")


class ProtobufTransformer(TypeTransformer[_proto_reflection.GeneratedProtocolMessageType]):
    PB_FIELD_KEY = "pb_type"

    def __init__(self):
        super().__init__("Protobuf-Transformer", _proto_reflection.GeneratedProtocolMessageType)

    @staticmethod
    def tag(expected_python_type: Type[T]) -> str:
        return f"{expected_python_type.__module__}.{expected_python_type.__name__}"

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(simple=SimpleType.STRUCT, metadata={ProtobufTransformer.PB_FIELD_KEY: self.tag(t)})

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        struct = Struct()
        struct.update(_MessageToDict(python_val))
        return Literal(scalar=Scalar(generic=struct))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not (lv and lv.scalar and lv.scalar.generic is not None):
            raise AssertionError("Can only convert a generic literal to a Protobuf")

        pb_obj = expected_python_type()
        dictionary = _MessageToDict(lv.scalar.generic)
        pb_obj = _ParseDict(dictionary, pb_obj)
        return pb_obj


class TypeEngine(typing.Generic[T]):
    """
    Core Extensible TypeEngine of Flytekit. This should be used to extend the capabilities of FlyteKits type system.
    Users can implement their own TypeTransformers and register them with the TypeEngine. This will allow special handling
    of user objects
    """

    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}
    _DATACLASS_TRANSFORMER: TypeTransformer = DataclassTransformer()

    @classmethod
    def register(cls, transformer: TypeTransformer):
        """
        This should be used for all types that respond with the right type annotation when you use type(...) function
        """
        if transformer.python_type in cls._REGISTRY:
            existing = cls._REGISTRY[transformer.python_type]
            raise ValueError(
                f"Transformer {existing.name} for type {transformer.python_type} is already registered."
                f" Cannot override with {transformer.name}"
            )
        cls._REGISTRY[transformer.python_type] = transformer

    @classmethod
    def get_transformer(cls, python_type: Type) -> TypeTransformer[T]:
        """
        The TypeEngine hierarchy for flyteKit. This method looksup and selects the type transformer. The algorithm is
        as follows

          d = dictionary of registered transformers, where is a python `type`
          v = lookup type
        Step 1:
            find a transformer that matches v exactly

        Step 2:
            find a transformer that matches the generic type of v. e.g List[int], Dict[str, int] etc

        Step 3:
            if v is of type data class, use the dataclass transformer

        Step 4:
            Walk the inheritance hierarchy of v and  find a transformer that matches the first base class.
            This is potentially non-deterministic - will depend on the registration pattern.

            TODO lets make this deterministic by using an ordered dict

        """
        # Step 1
        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]

        # Step 2
        if hasattr(python_type, "__origin__"):
            if hasattr(python_type.__origin__, "__origin__"):
                # special handling for typing.Annotated that holds generic types
                return cls.get_transformer(python_type.__origin__)
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]
            raise ValueError(f"Generic Type {python_type.__origin__} not supported currently in Flytekit.")

        # Step 3
        if dataclasses.is_dataclass(python_type):
            return cls._DATACLASS_TRANSFORMER

        # To facilitate cases where users may specify one transformer for multiple types that all inherit from one
        # parent.
        for base_type in cls._REGISTRY.keys():
            if base_type is None:
                continue  # None is actually one of the keys, but isinstance/issubclass doesn't work on it
            if base_type is typing.Union or base_type is typing.NamedTuple:
                # cannot be used with isinstance
                continue
            if isinstance(python_type, base_type) or (
                inspect.isclass(python_type) and issubclass(python_type, base_type)
            ):
                return cls._REGISTRY[base_type]
        raise ValueError(f"Type {python_type} not supported currently in Flytekit. Please register a new transformer")

    @classmethod
    def to_literal_type(cls, python_type: Type) -> LiteralType:
        """
        Converts a python type into a flyte specific ``LiteralType``
        """
        transformer = cls.get_transformer(python_type)
        res = transformer.get_literal_type(python_type)
        meta = None
        if hasattr(python_type, "__metadata__"):
            for x in python_type.__metadata__:
                if not isinstance(x, FlyteMetadata):
                    continue
                if x.data.get("__consumed", False):
                    continue
                meta = x.data
                x.data["__consumed"] = True

        if meta is not None:
            return LiteralType(
                simple=res.simple,
                schema=res.schema,
                collection_type=res.collection_type,
                map_value_type=res.map_value_type,
                blob=res.blob,
                enum_type=res.enum_type,
                metadata=meta,
            )
        return res

    @classmethod
    def to_literal(cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type, expected: LiteralType) -> Literal:
        """
        Converts a python value of a given type and expected ``LiteralType`` into a resolved ``Literal`` value.
        """
        transformer = cls.get_transformer(python_type)
        lv = transformer.to_literal(ctx, python_val, python_type, expected)
        # TODO Perform assertion here
        return lv

    @classmethod
    def to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        """
        Converts a Literal value with an expected python type into a python value.
        """
        transformer = cls.get_transformer(expected_python_type)

        return transformer.to_python_value(ctx, lv, expected_python_type)

    @classmethod
    def named_tuple_to_variable_map(cls, t: typing.NamedTuple) -> _interface_models.VariableMap:
        """
        Converts a python-native ``NamedTuple`` to a flyte-specific VariableMap of named literals.
        """
        variables = {}
        for idx, (var_name, var_type) in enumerate(t.__annotations__.items()):
            literal_type = cls.to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)

    @classmethod
    def literal_map_to_kwargs(
        cls, ctx: FlyteContext, lm: LiteralMap, python_types: typing.Dict[str, type]
    ) -> typing.Dict[str, typing.Any]:
        """
        Given a ``LiteralMap`` (usually an input into a task - intermediate), convert to kwargs for the task
        """
        if len(lm.literals) != len(python_types):
            raise ValueError(
                f"Received more input values {len(lm.literals)}" f" than allowed by the input spec {len(python_types)}"
            )

        return {k: TypeEngine.to_python_value(ctx, lm.literals[k], v) for k, v in python_types.items()}

    @classmethod
    def dict_to_literal_map(cls, ctx: FlyteContext, d: typing.Dict[str, typing.Any]) -> LiteralMap:
        """
        Given a dictionary mapping string keys to python values, convert to a LiteralMap.
        """
        literal_map = {}
        for k, v in d.items():
            literal_map[k] = TypeEngine.to_literal(
                ctx=ctx,
                python_val=v,
                python_type=type(v),
                expected=TypeEngine.to_literal_type(type(v)),
            )
        return LiteralMap(literal_map)

    @classmethod
    def get_available_transformers(cls) -> typing.KeysView[Type]:
        """
        Returns all python types for which transformers are available
        """
        return cls._REGISTRY.keys()

    @classmethod
    def guess_python_types(
        cls, flyte_variable_dict: typing.Dict[str, _interface_models.Variable]
    ) -> typing.Dict[str, type]:
        """
        Transforms a dictionary of flyte-specific ``Variable`` objects to a dictionary of regular python values.
        """
        python_types = {}
        for k, v in flyte_variable_dict.items():
            python_types[k] = cls.guess_python_type(v.type)
        return python_types

    @classmethod
    def guess_python_type(cls, flyte_type: LiteralType) -> type:
        """
        Transforms a flyte-specific ``LiteralType`` to a regular python value.
        """
        for _, transformer in cls._REGISTRY.items():
            try:
                return transformer.guess_python_type(flyte_type)
            except ValueError:
                logger.debug(f"Skipping transformer {transformer.name} for {flyte_type}")
        raise ValueError(f"No transformers could reverse Flyte literal type {flyte_type}")


class ListTransformer(TypeTransformer[T]):
    """
    Transformer that handles a univariate typing.List[T]
    """

    def __init__(self):
        super().__init__("Typed List", list)

    @staticmethod
    def get_sub_type(t: Type[T]) -> Type[T]:
        """
        Return the generic Type T of the List
        """

        if hasattr(t, "__origin__"):
            if hasattr(t.__origin__, "__origin__"):
                # special handling for typing.Annotated
                return ListTransformer.get_sub_type(t.__origin__)
            if t.__origin__ is list and hasattr(t, "__args__"):
                return t.__args__[0]
        raise ValueError("Only generic univariate typing.List[T] type is supported.")

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Only univariate Lists are supported in Flyte
        """
        try:
            sub_type = TypeEngine.to_literal_type(self.get_sub_type(t))
            return _type_models.LiteralType(collection_type=sub_type)
        except Exception as e:
            raise ValueError(f"Type of Generic List type is not supported, {e}")

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        t = self.get_sub_type(python_type)
        lit_list = [TypeEngine.to_literal(ctx, x, t, expected.collection_type) for x in python_val]
        return Literal(collection=LiteralCollection(literals=lit_list))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if lv.collection is None:
            raise AssertionError(f"Provided literal is not a list: {lv}")

        st = self.get_sub_type(expected_python_type)
        return [TypeEngine.to_python_value(ctx, x, st) for x in lv.collection.literals]

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.collection_type:
            ct = TypeEngine.guess_python_type(literal_type.collection_type)
            return typing.List[ct]
        raise ValueError(f"List transformer cannot reverse {literal_type}")


class DictTransformer(TypeTransformer[dict]):
    """
    Transformer that transforms a univariate dictionary Dict[str, T] to a Literal Map or
    transforms a untyped dictionary to a JSON (struct/Generic)
    """

    def __init__(self):
        super().__init__("Typed Dict", dict)

    @staticmethod
    def get_dict_types(t: Type[dict]) -> (type, type):
        """
        Return the generic Type T of the Dict
        """
        if hasattr(t, "__origin__"):
            if hasattr(t, "__origin__"):
                if hasattr(t.__origin__, "__origin__"):
                    # special handling for typing.Annotated
                    return DictTransformer.get_sub_type(t.__origin__)
            if t.__origin__ is dict and hasattr(t, "__args__"):
                return t.__args__
        return None, None

    @staticmethod
    def dict_to_generic_literal(v: dict) -> Literal:
        """
        Creates a flyte-specific ``Literal`` value from a native python dictionary.
        """
        return Literal(scalar=Scalar(generic=_json_format.Parse(_json.dumps(v), _struct.Struct())))

    def get_literal_type(self, t: Type[dict]) -> LiteralType:
        """
        Transforms a native python dictionary to a flyte-specific ``LiteralType``
        """
        tp = self.get_dict_types(t)
        if tp:
            if tp[0] == str:
                try:
                    sub_type = TypeEngine.to_literal_type(tp[1])
                    return _type_models.LiteralType(map_value_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        return _primitives.Generic.to_flyte_literal_type()

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[dict], expected: LiteralType
    ) -> Literal:
        if expected and expected.simple and expected.simple == SimpleType.STRUCT:
            return self.dict_to_generic_literal(python_val)

        lit_map = {}
        for k, v in python_val.items():
            if type(k) != str:
                raise ValueError("Flyte MapType expects all keys to be strings")
            k_type, v_type = self.get_dict_types(python_type)
            lit_map[k] = TypeEngine.to_literal(ctx, v, v_type, expected.map_value_type)
        return Literal(map=LiteralMap(literals=lit_map))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[dict]) -> dict:
        if lv and lv.map and lv.map.literals is not None:
            tp = self.get_dict_types(expected_python_type)
            if tp is None or tp[0] is None:
                raise TypeError(
                    "TypeMismatch: Cannot convert to python dictionary from Flyte Literal Dictionary as the given "
                    "dictionary does not have sub-type hints or they do not match with the originating dictionary "
                    "source. Flytekit does not currently support implicit conversions"
                )
            if tp[0] != str:
                raise TypeError("TypeMismatch. Destination dictionary does not accept 'str' key")
            py_map = {}
            for k, v in lv.map.literals.items():
                py_map[k] = TypeEngine.to_python_value(ctx, v, tp[1])
            return py_map

        # for empty generic we have to explicitly test for lv.scalar.generic is not None as empty dict
        # evaluates to false
        if lv and lv.scalar and lv.scalar.generic is not None:
            return _json.loads(_json_format.MessageToJson(lv.scalar.generic))
        raise TypeError(f"Cannot convert from {lv} to {expected_python_type}")

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.map_value_type:
            mt = TypeEngine.guess_python_type(literal_type.map_value_type)
            return typing.Dict[str, mt]
        raise ValueError(f"Dictionary transformer cannot reverse {literal_type}")


class TextIOTransformer(TypeTransformer[typing.TextIO]):
    """
    Handler for TextIO
    """

    def __init__(self):
        super().__init__(name="TextIO", t=typing.TextIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".txt"],
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: typing.TextIO) -> LiteralType:
        return _type_models.LiteralType(
            blob=self._blob_type(),
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.TextIO, python_type: Type[typing.TextIO], expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.TextIO]
    ) -> typing.TextIO:
        # TODO rename to get_auto_local_path()
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probably the responsibility of the framework to close() this
        return open(local_path, "r")


class BinaryIOTransformer(TypeTransformer[typing.BinaryIO]):
    """
    Handler for BinaryIO
    """

    def __init__(self):
        super().__init__(name="BinaryIO", t=typing.BinaryIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"],
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[typing.BinaryIO]) -> LiteralType:
        return _type_models.LiteralType(
            blob=self._blob_type(),
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.BinaryIO, python_type: Type[typing.BinaryIO], expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.BinaryIO]
    ) -> typing.BinaryIO:
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probability the responsibility of the framework to close this
        return open(local_path, "rb")


class PathLikeTransformer(TypeTransformer[os.PathLike]):
    """
    Handler for os.PathLike
    """

    def __init__(self):
        super().__init__(name="os.PathLike", t=os.PathLike)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"],
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[os.PathLike]) -> LiteralType:
        return _type_models.LiteralType(
            blob=self._blob_type(),
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: os.PathLike, python_type: Type[os.PathLike], expected: LiteralType
    ) -> Literal:
        # TODO we could guess the mimetype and allow the format to be changed at runtime. thus a non existent format
        #      could be replaced with a guess format?

        rpath = ctx.file_access.get_random_remote_path()

        # For remote values, say https://raw.github.com/demo_data.csv, we will not upload to Flyte's store (S3/GCS)
        # and just return a literal with a uri equal to the path given
        if ctx.file_access.is_remote(python_val):
            return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(expected.blob), uri=python_val)))

        # For local files, we'll upload for the user.
        ctx.file_access.put_data(python_val, rpath, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(expected.blob), uri=rpath)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[os.PathLike]) -> os.PathLike:
        # TODO rename to get_auto_local_path()
        local_destination_path = ctx.file_access.get_random_local_path()
        uri = lv.scalar.blob.uri
        # If the uri is just a local path like /tmp/file_name, we just return
        if not ctx.file_access.is_remote(uri):
            return uri

        # Since no delayed downloading is possible with strings, always download immediately.
        ctx.file_access.get_data(lv.scalar.blob.uri, local_destination_path, is_multipart=False)
        return local_destination_path


class EnumTransformer(TypeTransformer[enum.Enum]):
    """
    Enables converting a python type enum.Enum to LiteralType.EnumType
    """

    def __init__(self):
        super().__init__(name="DefaultEnumTransformer", t=enum.Enum)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        values = [v.value for v in t]
        if not isinstance(values[0], str):
            raise AssertionError("Only EnumTypes with value of string are supported")
        return LiteralType(enum_type=_core_types.EnumType(values=values))

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        return Literal(scalar=Scalar(primitive=Primitive(string_value=python_val.value)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        return expected_python_type(lv.scalar.primitive.string_value)


def _check_and_covert_float(lv: Literal) -> float:
    if lv.scalar.primitive.float_value is not None:
        return lv.scalar.primitive.float_value
    elif lv.scalar.primitive.integer is not None:
        return float(lv.scalar.primitive.integer)
    raise RuntimeError(f"Cannot convert literal {lv} to float")


def _register_default_type_transformers():
    TypeEngine.register(
        SimpleTransformer(
            "int",
            int,
            _primitives.Integer.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x))),
            lambda x: x.scalar.primitive.integer,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "float",
            float,
            _primitives.Float.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(float_value=x))),
            _check_and_covert_float,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "bool",
            bool,
            _primitives.Boolean.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(boolean=x))),
            lambda x: x.scalar.primitive.boolean,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "str",
            str,
            _primitives.String.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(string_value=x))),
            lambda x: x.scalar.primitive.string_value,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "datetime",
            _datetime.datetime,
            _primitives.Datetime.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(datetime=x))),
            lambda x: x.scalar.primitive.datetime,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "timedelta",
            _datetime.timedelta,
            _primitives.Timedelta.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(duration=x))),
            lambda x: x.scalar.primitive.duration,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "none",
            None,
            _type_models.LiteralType(simple=_type_models.SimpleType.NONE),
            lambda x: Literal(scalar=Scalar(none_type=Void())),
            lambda x: None,
        )
    )
    TypeEngine.register(
        SimpleTransformer(
            "none",
            type(None),
            _type_models.LiteralType(simple=_type_models.SimpleType.NONE),
            lambda x: Literal(scalar=Scalar(none_type=Void())),
            lambda x: None,
        )
    )
    TypeEngine.register(ListTransformer())
    TypeEngine.register(DictTransformer())
    TypeEngine.register(TextIOTransformer())
    TypeEngine.register(PathLikeTransformer())
    TypeEngine.register(BinaryIOTransformer())
    TypeEngine.register(EnumTransformer())
    TypeEngine.register(ProtobufTransformer())
    TypeEngine.register(UnionTransformer())

    # inner type is. Also unsupported are typing's Tuples. Even though you can look inside them, Flyte's type system
    # doesn't support these currently.
    # Confusing note: typing.NamedTuple is in here even though task functions themselves can return them. We just mean
    # that the return signature of a task can be a NamedTuple that contains another NamedTuple inside it.
    # Also, it's not entirely true that Flyte IDL doesn't support tuples. We can always fake them as structs, but we'll
    # hold off on doing that for now, as we may amend the IDL formally to support tuples.
    TypeEngine.register(RestrictedType("non typed tuple", tuple))
    TypeEngine.register(RestrictedType("non typed tuple", typing.Tuple))
    TypeEngine.register(RestrictedType("named tuple", typing.NamedTuple))


_register_default_type_transformers()
