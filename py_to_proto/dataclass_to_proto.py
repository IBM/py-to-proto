# Standard
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import dataclasses

# Third Party
from google.protobuf import any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import timestamp_pb2

# First Party
import alog

# Local
from .compat_annotated import Annotated, get_args, get_origin
from .converter_base import ConverterBase

log = alog.use_channel("DCLS2P")


## Globals #####################################################################

PY_TO_PROTO_TYPES = {
    Any: any_pb2.Any,
    bool: _descriptor.FieldDescriptor.TYPE_BOOL,
    str: _descriptor.FieldDescriptor.TYPE_STRING,
    bytes: _descriptor.FieldDescriptor.TYPE_BYTES,
    datetime: timestamp_pb2.Timestamp,
    float: _descriptor.FieldDescriptor.TYPE_DOUBLE,
    # TODO: support more integer types with numpy dtypes
    int: _descriptor.FieldDescriptor.TYPE_INT64,
}

## Interface ###################################################################


class FieldNumber(int):
    """A positive number used to identify a field"""

    def __new__(cls, *args, **kwargs):
        inst = super().__new__(cls, *args, **kwargs)
        if inst <= 0:
            raise ValueError("A field number must be a positive integer")
        return inst


class OneofField(str):
    """A field name for an element of a oneof"""


def dataclass_to_proto(
    package: str,
    dataclass_: type,
    *,
    name: Optional[str] = None,
    validate: bool = False,
    type_mapping: Optional[Dict[str, Union[int, _descriptor.Descriptor]]] = None,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> _descriptor.Descriptor:
    """Convert a dataclass into a set of proto DESCRIPTOR objects.

    Reference: https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass

    Args:
        name:  str
            The name for the top-level message object
        package:  str
            The proto package name to use for this object
        dataclass_:  type
            The dataclass class

    Kwargs:
        validate:  bool
            Whether or not to validate the class proactively
        type_mapping:  Optional[Dict[str, Union[int, _descriptor.Descriptor]]]
            A non-default mapping from JTD type names to proto types
        descriptor_pool:  Optional[descriptor_pool.DescriptorPool]
            If given, this DescriptorPool will be used to aggregate the set of
            message descriptors

    Returns:
        descriptor:  descriptor.Descriptor
            The top-level MessageDescriptor corresponding to this jtd definition
    """
    return DataclassConverter(
        dataclass_=dataclass_,
        package=package,
        name=name,
        validate=validate,
        type_mapping=type_mapping,
        descriptor_pool=descriptor_pool,
    ).descriptor


## Impl ########################################################################


class DataclassConverter(ConverterBase):
    """Converter implementation for dataclasses as the source"""

    def __init__(
        self,
        dataclass_: type,
        package: str,
        *,
        name: Optional[str] = None,
        type_mapping: Optional[Dict[str, Union[int, _descriptor.Descriptor]]] = None,
        validate: bool = False,
        descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
    ):
        """Fill in the default type mapping and additional default vals, then
        initialize the parent
        """
        type_mapping = type_mapping or PY_TO_PROTO_TYPES
        name = name or getattr(dataclass_, "__name__", "")
        super().__init__(
            name=name,
            package=package,
            source_schema=dataclass_,
            type_mapping=type_mapping,
            validate=validate,
            descriptor_pool=descriptor_pool,
        )

    ## Abstract Interface ######################################################

    def validate(self, source_schema: type) -> bool:
        """Perform preprocess validation of the input"""
        if not dataclasses.is_dataclass(source_schema) and not (
            isinstance(source_schema, type) and issubclass(source_schema, Enum)
        ):
            return False
        # TODO: More validation!
        return True

    ## Types ##

    def get_concrete_type(self, entry: Any) -> Any:
        """If this is a concrete type, get the type map key for it"""
        # Unwrap any Annotations
        entry_type = self._resolve_wrapped_type(entry)

        # If it's a known type, just return it
        if entry_type in self.type_mapping or isinstance(
            entry_type, (_descriptor.Descriptor, _descriptor.EnumDescriptor)
        ):
            return entry_type

        # If it's a type with a descriptor, return that descriptor
        descriptor_attr = getattr(entry_type, "DESCRIPTOR", None)
        if descriptor_attr is not None:
            return descriptor_attr

    ## Maps ##

    def get_map_key_val_types(
        self,
        entry: Any,
    ) -> Optional[Tuple[int, ConverterBase.ConvertOutputTypes]]:
        """Get the key and value types for a given map type"""
        if get_origin(entry) is dict:
            key_type, val_type = get_args(entry)
            return (
                self._convert(key_type, name="key"),
                self._convert(val_type, name="value"),
            )

    ## Enums ##

    def get_enum_vals(self, entry: Any) -> Optional[Iterable[Tuple[str, int]]]:
        """Get the ordered list of enum name -> number mappings if this entry is
        an enum

        NOTE: If any values appear multiple times, this implies an alias

        NOTE 2: All names must be unique
        """
        if isinstance(entry, type) and issubclass(entry, Enum):
            values = [(name, val.value) for name, val in entry.__members__.items()]
            # NOTE: proto3 _requires_ a placeholder 0-value for every enum that
            #   is the equivalent of unset. Some enums may do this intentionally
            #   while others won't, so we add one in here if not in the python
            #   version.
            if 0 not in [entry[1] for entry in values]:
                log.debug3("Adding placeholder 0-val for enum %s", entry)
                values = [("PLACEHOLDER_UNSET", 0)] + values
            return values

    ## Messages ##

    def get_message_fields(self, entry: Any) -> Optional[Iterable[Tuple[str, Any]]]:
        """Get the mapping of names to type-specific field descriptors if this
        entry is a message
        """
        if dataclasses.is_dataclass(entry):
            return entry.__dataclass_fields__.items()

    def has_additional_fields(self, entry: Any) -> bool:
        """Check whether the given entry expects to support arbitrary key/val
        additional properties
        """
        # There's no way to do additional keys with a dataclass
        return False

    def get_optional_field_names(self, entry: Any) -> List[str]:
        """Get the names of any fields which are explicitly marked 'optional'.

        For a dataclass this means looking at the types of the members for ones
        that either have default values. Fields marked as Optional that do not
        have default values are NOT considered optional since they are required
        in the __init__.
        """
        return [
            field_name
            for field_name, field in entry.__dataclass_fields__.items()
            if (
                field.default is not dataclasses.MISSING
                or field.default_factory is not dataclasses.MISSING
            )
        ]

    ## Fields ##

    def get_field_number(
        self,
        num_fields: int,
        field_def: Union[dataclasses.Field, type],
    ) -> int:
        """From the given field definition and index, get the proto field number
        from any metadata in the field definition and fall back to the next
        sequential value
        """
        field_type = (
            field_def.type if isinstance(field_def, dataclasses.Field) else field_def
        )
        field_num = self._get_unique_annotation(field_type, FieldNumber)
        if field_num is not None:
            return field_num
        return num_fields + 1

    def get_oneof_fields(
        self, field_def: dataclasses.Field
    ) -> Optional[Iterable[Tuple[str, Any]]]:
        """If the given field is a Union, return an iterable of the sub-field
        definitions for its
        """
        field_type = self._resolve_wrapped_type(field_def.type)
        oneof_fields = []
        if get_origin(field_type) is Union:
            for arg in get_args(field_type):
                oneof_field_name = self._get_unique_annotation(arg, OneofField)
                if oneof_field_name is None:
                    res_type = self._resolve_wrapped_type(arg)
                    oneof_field_name = (
                        f"{field_def.name}_{str(res_type.__name__)}".lower()
                    )
                    log.debug3("Using default oneof field name: %s", oneof_field_name)
                oneof_fields.append((oneof_field_name, arg))
        return oneof_fields

    def get_oneof_name(self, field_def: dataclasses.Field) -> str:
        """For an identified oneof field def, get the name"""
        return field_def.name

    def get_field_type(self, field_def: dataclasses.Field) -> Any:
        """Get the type of the field. The definition of type here will be
        specific to the converter (e.g. string for JTD, py type for dataclass)
        """
        field_type = self._resolve_wrapped_type(field_def.type)
        if get_origin(field_type) is list:
            args = get_args(field_type)
            if len(args) == 1:
                return args[0]
        return field_type

    def is_repeated_field(self, field_def: dataclasses.Field) -> bool:
        """Determine if the given field def is repeated"""
        return get_origin(self._resolve_wrapped_type(field_def.type)) is list

    ## Implementation Details ##################################################

    @classmethod
    def _resolve_wrapped_type(cls, field_type: type) -> type:
        """Unwrap the type inside an Annotated or Optional, or just return the
        type if not wrapped
        """
        origin = get_origin(field_type)
        args = get_args(field_type)

        # Unwrap Annotated and recurse in case it's an Annotated[Optional]
        if origin is Annotated:
            return cls._resolve_wrapped_type(args[0])

        # Unwrap Optional and recurse in case it's an Optional[Annotated]
        if origin is Union and type(None) in args:
            non_none_args = [arg for arg in args if arg is not type(None)]
            assert non_none_args, f"Cannot have a union with only one NoneType arg"
            if len(non_none_args) > 1:
                res_type = Union.__getitem__(tuple(non_none_args))
            else:
                res_type = non_none_args[0]
            return cls._resolve_wrapped_type(res_type)

        # If not Annotated or Optional, return as is
        return field_type

    @staticmethod
    def _get_annotations(field_type: type, annotation_type: type) -> List:
        """Get all annotations of the given annotation type from the given field
        type if it's annotated
        """
        if get_origin(field_type) is Annotated:
            return [
                arg
                for arg in get_args(field_type)[1:]
                if isinstance(arg, annotation_type)
            ]
        return []

    @classmethod
    def _get_unique_annotation(
        cls, field_type: type, annotation_type: type
    ) -> Optional[Any]:
        """Get any annotations of the given annotation type and ensure they're
        unique
        """
        annos = cls._get_annotations(field_type, annotation_type)
        if annos:
            if len(annos) > 1:
                raise ValueError(f"Multiple {annotation_type} annotations found")
            return annos[0]
