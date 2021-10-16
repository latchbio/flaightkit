from __future__ import annotations

import collections
import copy
import inspect
import json
import typing
from collections import OrderedDict
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, TypeVar, Union

from docstring_parser.common import Docstring

from flytekit.common.exceptions.user import FlyteValidationException
from flytekit.core import context_manager
from flytekit.core.docstring import param_metadata_from_docstring, returns_metadata_from_docstring
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models


class Interface(object):
    """
    A Python native interface object, like inspect.signature but simpler.
    """

    def __init__(
        self,
        inputs: typing.Optional[typing.Dict[str, Union[Type, Tuple[Type, Any]]]] = None,
        outputs: typing.Optional[typing.Dict[str, Type]] = None,
        output_tuple_name: Optional[str] = None,
        docstring: Optional[Docstring] = None,
    ):
        """
        :param outputs: Output variables and their types as a dictionary
        :param inputs: Map of input name to either a tuple where the first element is the python type, and the second
            value is the default, or just a single value which is the python type. The latter case is used by tasks
            for which perhaps a default value does not make sense. For consistency, we turn it into a tuple.
        :param output_tuple_name: This is used to store the name of a typing.NamedTuple when the task or workflow
            returns one. This is also used as a proxy for better or for worse for the presence of a tuple return type,
            primarily used when handling one-element NamedTuples.
        :param docstring: Docstring of the annotated @task or @workflow from which the interface derives from.
        """
        self._inputs = {}
        if inputs:
            for k, v in inputs.items():
                if isinstance(v, Tuple) and len(v) > 1:
                    self._inputs[k] = v
                else:
                    self._inputs[k] = (v, None)
        self._outputs = outputs if outputs else {}
        self._output_tuple_name = output_tuple_name

        if outputs:
            variables = [k for k in outputs.keys()]

            # TODO: This class is a duplicate of the one in create_task_outputs. Over time, we should move to this one.
            class Output(collections.namedtuple(output_tuple_name or "DefaultNamedTupleOutput", variables)):
                """
                This class can be used in two different places. For multivariate-return entities this class is used
                to rewrap the outputs so that our with_overrides function can work.
                For manual node creation, it's used during local execution as something that can be dereferenced.
                See the create_node funciton for more information.
                """

                def with_overrides(self, *args, **kwargs):
                    val = self.__getattribute__(self._fields[0])
                    val.with_overrides(*args, **kwargs)
                    return self

                @property
                def ref(self):
                    for var_name in variables:
                        if self.__getattribute__(var_name).ref:
                            return self.__getattribute__(var_name).ref
                    return None

                def runs_before(self, *args, **kwargs):
                    """
                    This is a placeholder and should do nothing. It is only here to enable local execution of workflows
                    where runs_before is manually called.
                    """

                def __rshift__(self, *args, **kwargs):
                    ...  # See runs_before

            self._output_tuple_class = Output
        self._docstring = docstring

    @property
    def output_tuple(self) -> Optional[Type[collections.namedtuple]]:
        return self._output_tuple_class

    @property
    def output_tuple_name(self) -> Optional[str]:
        return self._output_tuple_name

    @property
    def inputs(self) -> typing.Dict[str, Type]:
        r = {}
        for k, v in self._inputs.items():
            r[k] = v[0]
        return r

    @property
    def output_names(self) -> Optional[List[str]]:
        if self.outputs:
            return [k for k in self.outputs.keys()]
        return None

    @property
    def inputs_with_defaults(self) -> typing.Dict[str, Tuple[Type, Any]]:
        return self._inputs

    @property
    def default_inputs_as_kwargs(self) -> Dict[str, Any]:
        return {k: v[1] for k, v in self._inputs.items()}

    @property
    def outputs(self) -> typing.Dict[str, type]:
        return self._outputs

    @property
    def docstring(self) -> Optional[Docstring]:
        return self._docstring

    def remove_inputs(self, vars: List[str]) -> Interface:
        """
        This method is useful in removing some variables from the Flyte backend inputs specification, as these are
        implicit local only inputs or will be supplied by the library at runtime. For example, spark-session etc
        It creates a new instance of interface with the requested variables removed
        """
        if vars is None:
            return self
        new_inputs = copy.copy(self._inputs)
        for v in vars:
            if v in new_inputs:
                del new_inputs[v]
        return Interface(new_inputs, self._outputs, docstring=self.docstring)

    def with_inputs(self, extra_inputs: Dict[str, Type]) -> Interface:
        """
        Use this to add additional inputs to the interface. This is useful for adding additional implicit inputs that
        are added without the user requesting for them
        """
        if not extra_inputs:
            return self
        new_inputs = copy.copy(self._inputs)
        for k, v in extra_inputs.items():
            if k in new_inputs:
                raise ValueError(f"Input {k} cannot be added as it already exists in the interface")
            new_inputs[k] = v
        return Interface(new_inputs, self._outputs, docstring=self.docstring)

    def with_outputs(self, extra_outputs: Dict[str, Type]) -> Interface:
        """
        This method allows addition of extra outputs are expected from a task specification
        """
        if not extra_outputs:
            return self
        new_outputs = copy.copy(self._outputs)
        for k, v in extra_outputs.items():
            if k in new_outputs:
                raise ValueError(f"Output {k} cannot be added as it already exists in the interface")
            new_outputs[k] = v
        return Interface(self._inputs, new_outputs)


def transform_inputs_to_parameters(
    ctx: context_manager.FlyteContext, interface: Interface
) -> _interface_models.ParameterMap:
    """
    Transforms the given interface (with inputs) to a Parameter Map with defaults set
    :param interface: the interface object
    """
    if interface is None or interface.inputs_with_defaults is None:
        return _interface_models.ParameterMap({})
    if interface.docstring is None:
        inputs_vars = transform_variable_map(interface.inputs)
    else:
        inputs_vars = transform_variable_map(interface.inputs, param_metadata_from_docstring(interface.docstring))
    params = {}
    inputs_with_def = interface.inputs_with_defaults
    for k, v in inputs_vars.items():
        default = inputs_with_def[k][1]
        required = default == inspect.Parameter.empty

        default_lv = None
        if not required:
            default_lv = TypeEngine.to_literal(ctx, default, python_type=interface.inputs[k], expected=v.type)
        params[k] = _interface_models.Parameter(var=v, default=default_lv, required=required)
    return _interface_models.ParameterMap(params)


def transform_interface_to_typed_interface(
    interface: typing.Optional[Interface],
) -> typing.Optional[_interface_models.TypedInterface]:
    """
    Transform the given simple python native interface to FlyteIDL's interface
    """
    if interface is None:
        return None

    if interface.docstring is None:
        input_descriptions = output_descriptions = {}
    else:
        input_descriptions = param_metadata_from_docstring(interface.docstring)
        output_descriptions = returns_metadata_from_docstring(interface.docstring)

    if len(output_descriptions) == 1:
        name, desc = next(iter(output_descriptions.items()))

        # single unnamed return value docstring is shared by all return values:
        if name is None:
            output_descriptions = {k: output_descriptions.get(k, desc) for k in interface.outputs}

    inputs_map = transform_variable_map(interface.inputs, input_descriptions)
    outputs_map = transform_variable_map(interface.outputs, output_descriptions)
    return _interface_models.TypedInterface(inputs_map, outputs_map)


def transform_types_to_list_of_type(m: Dict[str, type]) -> Dict[str, type]:
    """
    Converts a given variables to be collections of their type. This is useful for array jobs / map style code.
    It will create a collection of types even if any one these types is not a collection type
    """
    if m is None:
        return {}

    all_types_are_collection = True
    for k, v in m.items():
        v_type = type(v)
        if v_type != typing.List and v_type != list:
            all_types_are_collection = False
            break

    if all_types_are_collection:
        return m

    om = {}
    for k, v in m.items():
        om[k] = typing.List[v]
    return om


def transform_interface_to_list_interface(interface: Interface) -> Interface:
    """
    Takes a single task interface and interpolates it to an array interface - to allow performing distributed python map
    like functions
    """
    map_inputs = transform_types_to_list_of_type(interface.inputs)
    map_outputs = transform_types_to_list_of_type(interface.outputs)

    return Interface(inputs=map_inputs, outputs=map_outputs)


def transform_signature_to_interface(signature: inspect.Signature, docstring: Optional[Docstring] = None) -> Interface:
    """
    From the annotations on a task function that the user should have provided, and the output names they want to use
    for each output parameter, construct the TypedInterface object

    For now the fancy object, maybe in the future a dumb object.

    """
    outputs = extract_return_annotation(signature.return_annotation)

    inputs = OrderedDict()
    for k, v in signature.parameters.items():
        # Inputs with default values are currently ignored, we may want to look into that in the future
        inputs[k] = (v.annotation, v.default)

    # This is just for typing.NamedTuples - in those cases, the user can select a name to call the NamedTuple. We
    # would like to preserve that name in our custom collections.namedtuple.
    custom_name = None
    return_annotation = signature.return_annotation
    if hasattr(return_annotation, "__bases__"):
        bases = return_annotation.__bases__
        if len(bases) == 1 and bases[0] == tuple and hasattr(return_annotation, "_fields"):
            if hasattr(return_annotation, "__name__") and return_annotation.__name__ != "":
                custom_name = return_annotation.__name__

    return Interface(inputs, outputs, output_tuple_name=custom_name, docstring=docstring)


def transform_variable_map(
    variable_map: Dict[str, type], descriptions: Dict[str, Any] = {}
) -> Dict[str, _interface_models.Variable]:
    """
    Given a map of str (names of inputs for instance) to their Python native types, return a map of the name to a
    Flyte Variable object with that type.
    """
    res = OrderedDict()
    if variable_map:
        for k, v in variable_map.items():
            res[k] = transform_type(v, descriptions.get(k, {"name": k}))

    return res


def transform_type(x: type, description: Optional[Any] = None) -> _interface_models.Variable:
    return _interface_models.Variable(type=TypeEngine.to_literal_type(x), description=json.dumps(description))


def default_output_name(index: int = 0) -> str:
    return f"o{index}"


def output_name_generator(length: int) -> Generator[str, None, None]:
    for x in range(0, length):
        yield default_output_name(x)


def extract_return_annotation(return_annotation: Union[Type, Tuple]) -> Dict[str, Type]:
    """
    The purpose of this function is to sort out whether a function is returning one thing, or multiple things, and to
    name the outputs accordingly, either by using our default name function, or from a typing.NamedTuple.

        # Option 1
        nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)
        def t(a: int, b: str) -> nt1: ...

        # Option 2
        def t(a: int, b: str) -> typing.NamedTuple("NT1", x_str=str, y_int=int): ...

        # Option 3
        def t(a: int, b: str) -> typing.Tuple[int, str]: ...

        # Option 4
        def t(a: int, b: str) -> (int, str): ...

        # Option 5
        def t(a: int, b: str) -> str: ...

        # Option 6
        def t(a: int, b: str) -> None: ...

        # Options 7/8
        def t(a: int, b: str) -> List[int]: ...
        def t(a: int, b: str) -> Dict[str, int]: ...

    Note that Options 1 and 2 are identical, just syntactic sugar. In the NamedTuple case, we'll use the names in the
    definition. In all other cases, we'll automatically generate output names, indexed starting at 0.
    """

    # Handle Option 6
    # We can think about whether we should add a default output name with type None in the future.
    if return_annotation is None or return_annotation is inspect.Signature.empty:
        return {}

    # This statement results in true for typing.Namedtuple, single and void return types, so this
    # handles Options 1, 2. Even though NamedTuple for us is multi-valued, it's a single value for Python
    if isinstance(return_annotation, Type) or isinstance(return_annotation, TypeVar):
        # isinstance / issubclass does not work for Namedtuple.
        # Options 1 and 2
        bases = return_annotation.__bases__
        if len(bases) == 1 and bases[0] == tuple and hasattr(return_annotation, "_fields"):
            logger.debug(f"Task returns named tuple {return_annotation}")
            return return_annotation.__annotations__

    if hasattr(return_annotation, "__origin__") and return_annotation.__origin__ is tuple:
        # Handle option 3
        logger.debug(f"Task returns unnamed typing.Tuple {return_annotation}")
        if len(return_annotation.__args__) == 1:
            raise FlyteValidationException(
                "Tuples should be used to indicate multiple return values, found only one return variable."
            )
        return OrderedDict(
            zip(list(output_name_generator(len(return_annotation.__args__))), return_annotation.__args__)
        )
    elif isinstance(return_annotation, tuple):
        if len(return_annotation) == 1:
            raise FlyteValidationException("Please don't use a tuple if you're just returning one thing.")
        return OrderedDict(zip(list(output_name_generator(len(return_annotation))), return_annotation))

    else:
        # Handle all other single return types
        logger.debug(f"Task returns unnamed native tuple {return_annotation}")
        return {default_output_name(): return_annotation}
