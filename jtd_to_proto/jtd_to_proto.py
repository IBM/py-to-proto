# Standard
from typing import Any, Dict, List, Optional, Tuple, Union
import copy
import re

# Third Party
from google.protobuf import any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import struct_pb2, timestamp_pb2

# First Party
import alog

# Local
from .utils import safe_add_fd_to_pool, to_upper_camel
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


## Interface ###################################################################


def jtd_to_proto(
    name: str,
    package: str,
    jtd_def: Dict[str, Union[dict, str]],
    *,
    validate_jtd: bool = False,
    valid_types: Optional[List[str]] = None,
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
        descriptor_pool:  Optional[descriptor_pool.DescriptorPool]
            If given, this DescriptorPool will be used to aggregate the set of
            message descriptors

    Returns:
        descriptor:  descriptor.Descriptor
            The top-level MessageDescriptor corresponding to this jtd definition
    """
    # If performing validation, attempt to parse schema with jtd and throw away
    # the results
    if validate_jtd:
        log.debug2("Validating JTD")
        valid_types = valid_types or JTD_TO_PROTO_TYPES.keys()
        if not is_valid_jtd(jtd_def, valid_types=valid_types):
            raise ValueError(f"Invalid JTD Schema: {jtd_def}")

    # This list will be used to aggregate the list of message DescriptorProtos
    # for any nested message objects defined inline
    imports = []

    # Perform the recursive conversion to update the descriptors and enums in
    # place
    log.debug("Performing conversion")
    descriptor_proto = _jtd_to_proto_impl(
        jtd_def=jtd_def,
        name=name,
        imports=imports,
    )
    proto_kwargs = {}
    is_enum = False
    if isinstance(descriptor_proto, descriptor_pb2.DescriptorProto):
        proto_kwargs["message_type"] = [descriptor_proto]
    elif isinstance(descriptor_proto, descriptor_pb2.EnumDescriptorProto):
        is_enum = True
        proto_kwargs["enum_type"] = [descriptor_proto]
    else:
        raise ValueError("Only messages and enums are supported")

    # Create the FileDescriptorProto with all messages
    log.debug("Creating FileDescriptorProto")
    fd_proto = descriptor_pb2.FileDescriptorProto(
        name=f"{name.lower()}.proto",
        package=package,
        syntax="proto3",
        dependency=sorted(list(set(imports))),
        **proto_kwargs,
    )
    log.debug4("Full FileDescriptorProto:\n%s", fd_proto)

    # Add the FileDescriptorProto to the Descriptor Pool
    log.debug("Adding Descriptors to DescriptorPool")
    if descriptor_pool is None:
        log.debug2("Using default descriptor pool")
        descriptor_pool = _descriptor_pool.Default()

    safe_add_fd_to_pool(fd_proto, descriptor_pool)

    # Return the descriptor for the top-level message
    fullname = name if not package else ".".join([package, name])
    if is_enum:
        return descriptor_pool.FindEnumTypeByName(fullname)
    return descriptor_pool.FindMessageTypeByName(fullname)


## Impl ########################################################################


def _jtd_to_proto_impl(
    *,
    jtd_def: Dict[str, Union[dict, str]],
    name: Optional[str],
    imports: List[str],
) -> Union[
    descriptor_pb2.DescriptorProto,
    descriptor_pb2.EnumDescriptorProto,
    int,
    Tuple[str, int],
]:
    """Recursive implementation of converting messages, fields, enums, arrays,
    and maps from JTD to their respective *DescriptorProto representations.
    """

    # Common logic for determining the name to use if a name is needed
    message_name = to_upper_camel(name)
    log.debug("Message name: %s", message_name)

    # If the jtd definition is a single "type": "name", perform the base-case
    # and look up the proto type
    type_name = jtd_def.get("type")
    if type_name is not None:
        # If the type name is itself a descriptor, use it as the value directly
        proto_type_descriptor = None
        is_enum = False
        if isinstance(type_name, _descriptor.Descriptor):
            proto_type_descriptor = type_name
        elif isinstance(type_name, _descriptor.EnumDescriptor):
            is_enum = True
            proto_type_descriptor = type_name
        else:
            proto_type_val = JTD_TO_PROTO_TYPES.get(type_name)
            proto_type_descriptor = getattr(proto_type_val, "DESCRIPTOR", None)
            if proto_type_val is None:
                raise ValueError(f"No proto mapping for type '{type_name}'")
            elif proto_type_descriptor is None:
                assert isinstance(
                    proto_type_val, int
                ), f"PROGRAMMING ERROR: Bad proto value type for {type_name}"
                return proto_type_val

        assert (
            proto_type_descriptor is not None
        ), "PROGRAMMING ERROR: proto_type_descriptor not defined"
        type_name = proto_type_descriptor.full_name
        import_file = proto_type_descriptor.file.name
        log.debug3(
            "Adding import file %s for known nested type %s",
            import_file,
            type_name,
        )
        imports.append(import_file)
        return (
            type_name,
            (
                _descriptor.FieldDescriptor.TYPE_ENUM
                if is_enum
                else _descriptor.FieldDescriptor.TYPE_MESSAGE
            ),
        )

    # If the definition has "enum" it's an enum
    enum = jtd_def.get("enum")
    if enum is not None:
        enum_proto = descriptor_pb2.EnumDescriptorProto(
            name=message_name,
            value=[
                descriptor_pb2.EnumValueDescriptorProto(
                    name=entry_name,
                    number=i,
                )
                for i, entry_name in enumerate(enum)
            ],
        )
        return enum_proto

    # If the definition has "values" it's a map
    #
    # Maps in descriptors are implemented in a _funky_ way. The map syntax=
    #     map<KeyType, ValType> the_map = 1;
    #
    # gets converted to a repeated message as follows:
    #     option map_entry = true;
    #     optional KeyType key = 1;
    #     optional ValType value = 2;
    #
    # CITE: https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.cc#L7102
    ##
    values = jtd_def.get("values")
    if values is not None:
        # Construct the JTD representation of the message
        entry_msg_type = {
            "properties": {
                "key": {"type": "string"},
                "value": values,
            }
        }
        entry_msg_name = "{}Entry".format(message_name)
        log.debug3("Map entry message for %s: %s", message_name, entry_msg_name)

        # Perform the recursive conversion
        nested = _jtd_to_proto_impl(
            jtd_def=entry_msg_type,
            name=entry_msg_name,
            imports=imports,
        )

        # Set the map_entry option
        nested.MergeFrom(
            descriptor_pb2.DescriptorProto(
                options=descriptor_pb2.MessageOptions(map_entry=True)
            )
        )
        return nested

    # If the object has "properties", it's a message
    properties = jtd_def.get("properties", {})
    optional_properties = jtd_def.get("optionalProperties", {})
    all_properties = dict(**properties, **optional_properties)
    additional_properties = jtd_def.get("additionalProperties")
    if all_properties or additional_properties:
        field_descriptors: List[descriptor_pb2.FieldDescriptorProto] = []
        nested_enums: List[descriptor_pb2.EnumDescriptorProto] = []
        nested_messages: List[descriptor_pb2.DescriptorProto] = []
        nested_oneofs: List[descriptor_pb2.OneofDescriptorProto] = []

        # Iterate each field and perform the recursive conversion
        for field_index, (field_name, field_def) in enumerate(all_properties.items()):
            log.debug2(
                "Handling property [%s.%s] (%d)", message_name, field_name, field_index
            )
            log.debug3("%s", field_def)

            field_kwargs = {
                "name": field_name,
                "number": len(field_descriptors) + 1,
                "label": _descriptor.FieldDescriptor.LABEL_OPTIONAL,
            }
            field_type_def = field_def

            # If the definition is nested with "elements" it's a repeated field
            elements = field_def.get("elements")
            if elements is not None:
                field_kwargs["label"] = _descriptor.FieldDescriptor.LABEL_REPEATED
                field_type_def = elements

            # If the definition is a "discriminator" it's a oneof. This means
            # we need to recurse on all elements of the "mapping" and perform
            # the nested type logic for each result, then add the special
            # oneof field
            discriminator = field_def.get("discriminator")
            nested_results = []
            if discriminator is not None:
                mapping = field_def.get("mapping")
                assert isinstance(
                    mapping, dict
                ), "Invalid discriminator without mapping"

                # Make all the sub-fields within the oneof
                for mapping_idx, (mapping_name, mapping_def) in enumerate(
                    mapping.items()
                ):
                    nested_results.append(
                        (
                            _jtd_to_proto_impl(
                                jtd_def=mapping_def,
                                name=mapping_name,
                                imports=imports,
                            ),
                            {
                                "oneof_index": len(nested_oneofs),
                                "number": field_kwargs["number"] + mapping_idx,
                                "name": mapping_name.lower(),
                            },
                        )
                    )

                # Add the name for this oneof
                nested_oneofs.append(
                    descriptor_pb2.OneofDescriptorProto(name=discriminator)
                )

            # If not a oneof, just recurse once
            else:
                nested_results = [
                    (
                        _jtd_to_proto_impl(
                            jtd_def=field_type_def,
                            name=field_name,
                            imports=imports,
                        ),
                        {},
                    )
                ]

            for nested, extra_kwargs in nested_results:
                nested_field_kwargs = copy.copy(field_kwargs)
                nested_field_kwargs.update(extra_kwargs)

                # If the result is an int, it's a type value
                if isinstance(nested, int):
                    nested_field_kwargs["type"] = nested

                # If the result is a tuple, it's an imported message or name
                elif isinstance(nested, tuple):
                    (
                        nested_field_kwargs["type_name"],
                        nested_field_kwargs["type"],
                    ) = nested

                # If the result is an enum, add it as a nested enum
                elif isinstance(nested, descriptor_pb2.EnumDescriptorProto):
                    nested_field_kwargs["type"] = _descriptor.FieldDescriptor.TYPE_ENUM
                    nested_field_kwargs["type_name"] = nested.name
                    nested_enums.append(nested)

                # If the result is a message, add it as a nested message
                elif isinstance(nested, descriptor_pb2.DescriptorProto):
                    nested_field_kwargs[
                        "type"
                    ] = _descriptor.FieldDescriptor.TYPE_MESSAGE
                    nested_field_kwargs["type_name"] = nested.name
                    nested_messages.append(nested)

                    # If the message has map_entry set, we need to indicate that
                    # it's repeated
                    if nested.options.map_entry:
                        nested_field_kwargs[
                            "label"
                        ] = _descriptor.FieldDescriptor.LABEL_REPEATED

                        # If the nested map entry itself has nested types or enums,
                        # they need to be moved up to this message
                        while nested.nested_type:
                            nested_type = nested.nested_type.pop()
                            plain_name = nested_type.name
                            nested_name = to_upper_camel(
                                "_".join([field_name, plain_name])
                            )
                            nested_type.MergeFrom(
                                descriptor_pb2.DescriptorProto(name=nested_name)
                            )
                            for field in nested.field:
                                if field.type_name == plain_name:
                                    field.MergeFrom(
                                        descriptor_pb2.FieldDescriptorProto(
                                            type_name=nested_name
                                        )
                                    )
                            nested_messages.append(nested_type)
                        while nested.enum_type:
                            nested_enum = nested.enum_type.pop()
                            plain_name = nested_enum.name
                            nested_name = to_upper_camel(
                                "_".join([field_name, plain_name])
                            )
                            nested_enum.MergeFrom(
                                descriptor_pb2.EnumDescriptorProto(name=nested_name)
                            )
                            for field in nested.field:
                                if field.type_name == plain_name:
                                    field.MergeFrom(
                                        descriptor_pb2.FieldDescriptorProto(
                                            type_name=nested_name
                                        )
                                    )
                            nested_enums.append(nested_enum)

                # Create the field descriptor
                field_descriptors.append(
                    descriptor_pb2.FieldDescriptorProto(**nested_field_kwargs)
                )

        # If additionalProperties specified, add a 'special' field for this.
        # This is one place where there's not a good mapping between JTD and
        # proto since proto does not allow for arbitrary mappings _in addition_
        # to specific keys. Instead, there needs to be a special Struct field to
        # hold these additional propreties.
        if additional_properties:
            if "additionalProperties" in all_properties:
                raise ValueError(
                    "Cannot specify 'additionalProperties' as a field and use it in the JTD definition"
                )
            field_descriptors.append(
                descriptor_pb2.FieldDescriptorProto(
                    name="additionalProperties",
                    number=len(field_descriptors) + 1,
                    type=_descriptor.FieldDescriptor.TYPE_MESSAGE,
                    label=_descriptor.FieldDescriptor.LABEL_OPTIONAL,
                    type_name=struct_pb2.Struct.DESCRIPTOR.full_name,
                )
            )
            imports.append(struct_pb2.Struct.DESCRIPTOR.file.name)

        # Support optional properties as oneofs i.e. optional int32 foo = 1;
        # becomes interpreted as oneof _foo { int32 foo = 1; }
        optional_oneofs: List[descriptor_pb2.OneofDescriptorProto] = []
        for field in field_descriptors:
            if (
                field.name in optional_properties.keys()
                and field.label == _descriptor.FieldDescriptor.LABEL_OPTIONAL
            ):
                # OneofDescriptorProto do not contain fields themselves. Instead the
                # FieldDescriptorProto must contain the index of the oneof inside the
                # DescriptorProto
                optional_oneofs.append(
                    descriptor_pb2.OneofDescriptorProto(name=f"_{field.name}")
                )
                field.oneof_index = len(nested_oneofs) + len(optional_oneofs) - 1

        # Construct the message descriptor
        log.debug3(
            "All field descriptors for [%s]:\n%s", message_name, field_descriptors
        )
        descriptor_proto = descriptor_pb2.DescriptorProto(
            name=message_name,
            field=field_descriptors,
            enum_type=nested_enums,
            nested_type=nested_messages,
            oneof_decl=nested_oneofs + optional_oneofs,
        )
        return descriptor_proto
