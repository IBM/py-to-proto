# Standard
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# Third Party
from google.protobuf import any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import timestamp_pb2

# First Party
import alog

# Local
from .converter_base import ConverterBase
from .validation import is_valid_jtd

log = alog.use_channel("JTD2P")


## Globals #####################################################################

JTD_TO_PROTO_TYPES = {
    "any": any_pb2.Any,
    "boolean": _descriptor.FieldDescriptor.TYPE_BOOL,
    "string": _descriptor.FieldDescriptor.TYPE_STRING,
    "timestamp": timestamp_pb2.Timestamp,
    "float32": _descriptor.FieldDescriptor.TYPE_FLOAT,
    "float64": _descriptor.FieldDescriptor.TYPE_DOUBLE,
    # NOTE: All number types except fixed, double, and float are stored as
    #   varints meaning as long as your numbers stay in about the int8 or int16
    #   range, they are only 1 or 2 bytes, even though it says int32.
    #
    # CITE: https://groups.google.com/g/protobuf/c/Er39mNGnRWU/m/x6Srz_GrZPgJ
    "int8": _descriptor.FieldDescriptor.TYPE_INT32,
    "uint8": _descriptor.FieldDescriptor.TYPE_UINT32,
    "int16": _descriptor.FieldDescriptor.TYPE_INT32,
    "uint16": _descriptor.FieldDescriptor.TYPE_UINT32,
    "int32": _descriptor.FieldDescriptor.TYPE_INT32,
    "uint32": _descriptor.FieldDescriptor.TYPE_UINT32,
    "int64": _descriptor.FieldDescriptor.TYPE_INT64,
    "uint64": _descriptor.FieldDescriptor.TYPE_UINT64,
    # Not strictly part of the JTD spec, but important for protobuf messages
    "bytes": _descriptor.FieldDescriptor.TYPE_BYTES,
}

# Common type used everywhere for a JTD dict
_JtdDefType = Dict[str, Union[dict, str]]


## Interface ###################################################################


def jtd_to_proto(
    name: str,
    package: str,
    jtd_def: _JtdDefType,
    *,
    validate_jtd: bool = False,
    type_mapping: Optional[Dict[str, Union[int, _descriptor.Descriptor]]] = None,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> _descriptor.Descriptor:
    """Convert a JTD schema into a set of proto DESCRIPTOR objects.

    Reference: https://jsontypedef.com/docs/jtd-in-5-minutes/

    Args:
        name:  str
            The name for the top-level message object
        package:  str
            The proto package name to use for this object
        jtd_def:  Dict[str, Union[dict, str]]
            The full JTD schema dict

    Kwargs:
        validate_jtd:  bool
            Whether or not to validate the JTD schema
        type_mapping:  Optional[Dict[str, Union[int, _descriptor.Descriptor]]]
            A non-default mapping from JTD type names to proto types
        descriptor_pool:  Optional[descriptor_pool.DescriptorPool]
            If given, this DescriptorPool will be used to aggregate the set of
            message descriptors

    Returns:
        descriptor:  descriptor.Descriptor
            The top-level MessageDescriptor corresponding to this jtd definition
    """
    return JTDConverter(
        name=name,
        package=package,
        jtd_def=jtd_def,
        validate=validate_jtd,
        type_mapping=type_mapping,
        descriptor_pool=descriptor_pool,
    ).descriptor


## Impl ########################################################################


class JTDConverter(ConverterBase):
    """Converter implementation for JTD source schemas"""

    def __init__(
        self,
        name: str,
        package: str,
        jtd_def: _JtdDefType,
        *,
        type_mapping: Optional[Dict[str, Union[int, _descriptor.Descriptor]]] = None,
        validate: bool = False,
        descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
    ):
        """Fill in the default type mapping and additional default vals, then
        initialize the parent
        """
        type_mapping = type_mapping or JTD_TO_PROTO_TYPES
        super().__init__(
            name=name,
            package=package,
            source_schema=jtd_def,
            type_mapping=type_mapping,
            validate=validate,
            descriptor_pool=descriptor_pool,
        )

    ## Abstract Interface ######################################################

    def validate(self, source_schema: _JtdDefType) -> bool:
        """Perform preprocess validation of the input"""
        log.debug2("Validating JTD")
        valid_types = self.type_mapping.keys()
        return is_valid_jtd(source_schema, valid_types=valid_types)

    ## Types ##

    def get_concrete_type(self, entry: _JtdDefType) -> Any:
        """If this is a concrete type, get the JTD key for it"""
        return entry.get("type")

    ## Maps ##

    def get_map_key_val_types(
        self,
        entry: _JtdDefType,
    ) -> Optional[Tuple[int, ConverterBase.ConvertOutputTypes]]:
        """Get the key and value types for a given map type"""
        values = entry.get("values")
        if values is not None:
            string_type = self.type_mapping.get("string")
            if string_type is None:
                raise ValueError(
                    "Provided type mapping has no key for 'string', so values maps cannot be used"
                )
            val_type = self._convert(entry=values, name="value")
            return (string_type, val_type)

    ## Enums ##

    def get_enum_vals(self, entry: _JtdDefType) -> Optional[List[Tuple[str, int]]]:
        """Get the ordered list of enum name -> number mappings if this entry is
        an enum

        NOTE: If any values appear multiple times, this implies an alias

        NOTE 2: All names must be unique
        """
        enum = entry.get("enum")
        if enum is not None:
            return [
                (entry_name, entry_idx) for entry_idx, entry_name in enumerate(enum)
            ]

    ## Messages ##

    def get_message_fields(
        self,
        entry: _JtdDefType,
    ) -> Optional[Iterable[Tuple[str, Any]]]:
        """Get the mapping of names to type-specific field descriptors"""
        properties = entry.get("properties", {})
        optional_properties = entry.get("optionalProperties", {})
        all_properties = {**properties, **optional_properties}
        if all_properties:
            return all_properties.items()

    def has_additional_fields(self, entry: _JtdDefType) -> bool:
        """Check whether the given entry expects to support arbitrary key/val
        additional properties
        """
        return entry.get("additionalProperties", False)

    def get_optional_field_names(self, entry: _JtdDefType) -> List[str]:
        """Get the names of any fields which are explicitly marked 'optional'"""
        return entry.get("optionalProperties", {}).keys()

    ## Fields ##

    def get_field_number(self, num_fields: int, field_def: _JtdDefType) -> int:
        """If the field has a metadata field "field_number" use that, otherwise,
        use the next field number sequentially
        """
        return field_def.get("metadata", {}).get("field_number", num_fields + 1)

    def get_oneof_fields(
        self, field_def: _JtdDefType
    ) -> Optional[Iterable[Tuple[str, Any]]]:
        """If the given field def is a discriminator, it's a oneof"""
        discriminator = field_def.get("discriminator")
        if discriminator is not None:
            mapping = field_def.get("mapping")
            assert isinstance(mapping, dict), "Invalid discriminator without mapping"
            return mapping.items()

    def get_oneof_name(self, field_def: _JtdDefType) -> str:
        """For an identified oneof field def, get the name"""
        return field_def.get("discriminator")

    def get_field_type(self, field_def: _JtdDefType) -> Any:
        """Get the type of the field. The definition of type here will be
        specific to the converter (e.g. string for JTD, py type for dataclass)
        """
        elements = field_def.get("elements")
        if elements is not None:
            return elements
        return field_def

    def is_repeated_field(self, field_def: _JtdDefType) -> bool:
        """Determine if the given field def is repeated"""
        return "elements" in field_def
