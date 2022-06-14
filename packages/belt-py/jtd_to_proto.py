# Standard
from typing import Dict, List, Optional, Tuple, Union
import copy
import os
import re

# Third Party
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import struct_pb2, timestamp_pb2
import jtd

# First Party
import alog

log = alog.use_channel("JTD2P")

## Utils #######################################################################


def _to_upper_camel(snake_str: str) -> str:
    """Convert a snake_case string to UpperCamelCase"""
    if not snake_str:
        return snake_str
    return (
        snake_str[0].upper()
        + re.sub("_([a-zA-Z])", lambda pat: pat.group(1).upper(), snake_str)[1:]
    )


def _descriptor_proto_from_descriptor(descriptor, msg_name):
    """Extract a DescriptorProto for a named message from a FileDescriptor. This
    is needed so that those messages can be used within fields below.
    """
    file_proto = descriptor_pb2.FileDescriptorProto()
    descriptor.CopyToProto(file_proto)
    return list(filter(lambda msg: msg.name == msg_name, file_proto.message_type))[0]


## Globals #####################################################################

# Extract DescriptorProtos for struct_pb2.Proto and timestamp_pb2.Timestamp
STRUCT_PROTO = _descriptor_proto_from_descriptor(struct_pb2.DESCRIPTOR, "Struct")
TIMESTAMP_PROTO = _descriptor_proto_from_descriptor(
    timestamp_pb2.DESCRIPTOR, "Timestamp"
)

JTD_TO_PROTO_TYPES = {
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
    "int8": _descriptor.FieldDescriptor.TYPE_INT32,
    "uint8": _descriptor.FieldDescriptor.TYPE_UINT32,
    "int32": _descriptor.FieldDescriptor.TYPE_INT32,
    "uint32": _descriptor.FieldDescriptor.TYPE_UINT32,
}

PROTO_FILE_PRIMITIVE_TYPE_NAMES = {
    type_val: type_name[5:].lower()
    for type_name, type_val in vars(_descriptor.FieldDescriptor).items()
    if type_name.startswith("TYPE_")
}

PROTO_FILE_INDENT = "  "

PROTO_FILE_AUTOGEN_HEADER = """
/*------------------------------------------------------------------------------
 * AUTO GENERATED
 *----------------------------------------------------------------------------*/
"""

PROTO_FILE_ENUM_HEADER = """
/*-- ENUMS -------------------------------------------------------------------*/
"""

PROTO_FILE_MESSAGE_HEADER = """
/*-- MESSAGES ----------------------------------------------------------------*/
"""


## Interface ###################################################################


def jtd_to_proto(
    jtd_def: Dict[str, Union[dict, str]],
    name: str,
    package_name: str = "",
    *,
    validate_jtd: bool = False,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> _descriptor.Descriptor:
    """Convert a JTD schema into a set of proto DESCRIPTOR objects.

    Reference: https://jsontypedef.com/docs/jtd-in-5-minutes/

    Args:
        jtd_def:  Dict[str, Union[dict, str]]
            The full JTD schema dict
        name:  str
            The name for the top-level message object
        package_name:  str
            The proto package name to use for this object

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
        jtd.schema.Schema.from_dict(jtd_def)

    # This list will be used to aggregate the list of message DescriporProtos
    # for any nested message objects defined inline
    imports = []

    # Perform the recursive conversion to update the descriptors and enums in
    # place
    log.debug("Performing conversion")
    descriptor_proto = _jtd_to_proto_impl(
        jtd_def=jtd_def,
        name=name,
        package_name=package_name,
        imports=imports,
    )
    proto_kwargs = {}
    if isinstance(descriptor_proto, descriptor_pb2.DescriptorProto):
        proto_kwargs["message_type"] = [descriptor_proto]
    elif isinstance(descriptor_proto, descriptor_pb2.EnumDescriptorProto):
        proto_kwargs["enum_type"] = [descriptor_proto]
    else:
        raise ValueError("Cannot create top-level proto for 'type'")

    # Create the FileDescriptorProto with all messages
    log.debug("Creating FileDescriptorProto")
    fd_proto = descriptor_pb2.FileDescriptorProto(
        name=f"{name.lower()}.proto",
        package=package_name,
        syntax="proto3",
        dependency=imports,
        **proto_kwargs,
    )
    log.debug4("Full FileDescriptorProto:\n%s", fd_proto)

    # Add the FileDescriptorProto to the Descriptor Pool
    log.debug("Adding Descriptors to DescriptorPool")
    if descriptor_pool is None:
        log.debug2("Using default descriptor pool")
        descriptor_pool = _descriptor_pool.Default()
    descriptor_pool.Add(fd_proto)

    # Return the descriptor for the top-level message
    fullname = name if not package_name else ".".join([package_name, name])
    return descriptor_pool.FindMessageTypeByName(fullname)


def descriptor_to_file(
    descriptor: Union[_descriptor.FileDescriptor, _descriptor.Descriptor],
) -> str:
    """Serialize a .proto file from a FileDescriptor

    Args:
        descriptor:  Union[descriptor.FileDescriptor, descriptor.MessageDescriptor]
            The file or message descriptor to serialize

    Returns:
        proto_file_content:  str
            The serialized file content for the .proto file
    """

    # If this is a message descriptor, use its corresponding FileDescriptor
    if isinstance(descriptor, _descriptor.Descriptor):
        descriptor = descriptor.file
    if not isinstance(descriptor, _descriptor.FileDescriptor):
        raise ValueError(f"Invalid file descriptor of type {type(descriptor)}")
    proto_file_lines = []

    # Create the header
    proto_file_lines.append(PROTO_FILE_AUTOGEN_HEADER)

    # Add package, syntax, and imports
    proto_file_lines.append(f'syntax = "{descriptor.syntax}";')
    if descriptor.package:
        proto_file_lines.append(f"package {descriptor.package};")
    for dep in descriptor.dependencies:
        proto_file_lines.append(f'import "{dep.name}";')
    proto_file_lines.append("")

    # Add all enums
    if descriptor.enum_types_by_name:
        proto_file_lines.append(PROTO_FILE_ENUM_HEADER)
        for enum_descriptor in descriptor.enum_types_by_name.values():
            proto_file_lines.extend(_enum_descriptor_to_file(enum_descriptor))
            proto_file_lines.append("")

    # Add all messages
    if descriptor.message_types_by_name:
        proto_file_lines.append(PROTO_FILE_MESSAGE_HEADER)
        for message_descriptor in descriptor.message_types_by_name.values():
            proto_file_lines.extend(_message_descriptor_to_file(message_descriptor))
            proto_file_lines.append("")

    return "\n".join(proto_file_lines)


## Impl ########################################################################


def _jtd_to_proto_impl(
    *,
    jtd_def: Dict[str, Union[dict, str]],
    name: Optional[str],
    package_name: str,
    imports: List[str],
) -> Union[
    descriptor_pb2.DescriptorProto,
    descriptor_pb2.EnumDescriptorProto,
    int,
    str,
]:
    """Recursive implementation of converting messages, fields, enums, arrays,
    and maps from JTD to their respective *DescriptorProto representations.
    """

    # Common logic for determining the name to use if a name is needed
    message_name = _to_upper_camel(name)
    log.debug("Message name: %s", message_name)

    # If the jtd definition is a single "type": "name", perform the base-case
    # and look up the proto type
    type_name = jtd_def.get("type")
    if type_name is not None:
        proto_type_val = JTD_TO_PROTO_TYPES.get(type_name)
        if proto_type_val is None:
            raise ValueError(f"No proto mapping for type '{type_name}'")

        # If this is a primitive, just return it
        if isinstance(proto_type_val, int):
            return proto_type_val

        # Otherwise, assume it's a known DescriptorProto
        else:
            type_name = proto_type_val.DESCRIPTOR.full_name
            import_file = proto_type_val.DESCRIPTOR.file.name
            log.debug3(
                "Adding import file %s for known nested type %s",
                import_file,
                type_name,
            )
            imports.append(import_file)
            return type_name

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
            package_name=package_name,
            imports=imports,
        )

        # Set the map_entry option
        nested.MergeFrom(
            descriptor_pb2.DescriptorProto(
                options=descriptor_pb2.MessageOptions(map_entry=True)
            )
        )
        return nested

    # If the definition has "descriminator" in it, it's a oneof
    descriminator = jtd_def.get("descriminator")
    if descriminator is not None:
        mapping = jtd_def.get("mapping")
        if mapping is None:
            raise ValueError("No 'mapping' given with 'descriminator'")
        raise NotImplementedError("Descriminator not handled yet!")

    # If the object has "properties", it's a message
    properties = jtd_def.get("properties", {})
    optional_properties = jtd_def.get("optionalProperties", {})
    all_properties = dict(**properties, **optional_properties)
    additional_properties = jtd_def.get("additionalProperties")
    if all_properties or additional_properties:
        field_descriptors = []
        nested_enums = []
        nested_messages = []
        nested_oneofs = []

        # Iterate each field and perform the recursive conversion
        for field_index, (field_name, field_def) in enumerate(all_properties.items()):
            log.debug2(
                "Handling property [%s.%s] (%d)", message_name, field_name, field_index
            )
            log.debug3(field_def)

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
                                package_name=package_name,
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
                            package_name=package_name,
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

                # If the result is a tuple, it's an imported message name
                elif isinstance(nested, str):
                    nested_field_kwargs[
                        "type"
                    ] = _descriptor.FieldDescriptor.TYPE_MESSAGE
                    nested_field_kwargs["type_name"] = nested

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
                            nested_name = _to_upper_camel(
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
                            nested_name = _to_upper_camel(
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

                # Otherwise, it's an error!
                else:
                    assert (
                        False
                    ), f"Programming Error! Can't handle field of type {type(nested)}"

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

        # Construct the message descriptor
        log.debug3(
            "All field descriptors for [%s]:\n%s", message_name, field_descriptors
        )
        descriptor_proto = descriptor_pb2.DescriptorProto(
            name=message_name,
            field=field_descriptors,
            enum_type=nested_enums,
            nested_type=nested_messages,
            oneof_decl=nested_oneofs,
        )
        return descriptor_proto


def _indent_lines(indent: int, lines: List[str]) -> List[str]:
    """Add indentation to the given lines"""
    return [
        indent * PROTO_FILE_INDENT + line if line else line
        for line in "\n".join(lines).split("\n")
    ]


def _enum_descriptor_to_file(
    enum_descriptor: _descriptor.EnumDescriptor,
    indent: int = 0,
) -> List[str]:
    """Make the string representation of an enum"""
    lines = []
    lines.append(f"enum {enum_descriptor.name} {{")
    for val in enum_descriptor.values:
        lines.append(f"{PROTO_FILE_INDENT}{val.name} = {val.number};")
    lines.append("}")
    return _indent_lines(indent, lines)


def _message_descriptor_to_file(
    message_descriptor: _descriptor.Descriptor,
    indent: int = 0,
) -> List[str]:
    """Make the string representation of an enum"""
    lines = []
    lines.append(f"message {message_descriptor.name} {{")

    # Add nested enums
    for enum_descriptor in message_descriptor.enum_types:
        lines.extend(_enum_descriptor_to_file(enum_descriptor, indent=1))
        lines.append("")

    # Add nested messages
    for nested_msg_descriptor in message_descriptor.nested_types:
        if _is_map_entry(nested_msg_descriptor):
            continue
        lines.extend(_message_descriptor_to_file(nested_msg_descriptor, indent=1))
        lines.append("")

    # Add fields
    for field_descriptor in message_descriptor.fields:
        field_line = PROTO_FILE_INDENT

        # Add the repeated qualifier if needed
        if (
            not _is_map_entry(field_descriptor.message_type)
            and field_descriptor.label == field_descriptor.LABEL_REPEATED
        ):
            field_line += "repeated "

        # Add the type
        field_line += _get_field_type_str(field_descriptor)

        # Add the name and number
        field_line += f" {field_descriptor.name} = {field_descriptor.number};"

        lines.append(field_line)

    lines.append("}")
    return _indent_lines(indent, lines)


def _get_field_type_str(field_descriptor: _descriptor.FieldDescriptor) -> str:
    """Get the string version of a field's type"""

    # Add the type
    if field_descriptor.type == field_descriptor.TYPE_MESSAGE:
        if _is_map_entry(field_descriptor.message_type):
            key_type = _get_field_type_str(
                field_descriptor.message_type.fields_by_name["key"]
            )
            val_type = _get_field_type_str(
                field_descriptor.message_type.fields_by_name["value"]
            )
            return f"map<{key_type}, {val_type}>"
        else:
            return field_descriptor.message_type.full_name
    elif field_descriptor.type == field_descriptor.TYPE_ENUM:
        return field_descriptor.enum_type.full_name
    else:
        return PROTO_FILE_PRIMITIVE_TYPE_NAMES[field_descriptor.type]


def _is_map_entry(message_descriptor: _descriptor.Descriptor) -> bool:
    """Check whether this message is a map entry"""
    return message_descriptor is not None and getattr(
        message_descriptor.GetOptions(), "map_entry", False
    )


## Main ########################################################################

jtd_def = {
    "properties": {
        # bool field
        "foo": {
            "type": "boolean",
        },
        # Array of strings
        "bar": {
            "elements": {
                "type": "string",
            }
        },
        # Nested Object
        "buz": {
            "properties": {
                "bee": {
                    "type": "boolean",
                }
            },
            # Arbitrary map
            "additionalProperties": True,
        },
        # timestamp field
        "time": {
            "type": "timestamp",
        },
        # Array of objects
        "baz": {
            "elements": {
                "properties": {
                    "nested": {
                        "type": "int8",
                    }
                }
            }
        },
        # Enum
        "bat": {
            "enum": ["VAMPIRE", "DRACULA"],
        },
        # Array of enums
        "bif": {
            "elements": {
                "enum": ["NAME", "SOUND_EFFECT"],
            }
        },
        # Typed dict with primitive values
        "biz": {
            "values": {
                "type": "float32",
            }
        },
        # Dict with message values
        "bonk": {
            "values": {
                "properties": {
                    "how_hard": {"type": "float32"},
                }
            }
        },
        # Dict with enum values
        "bang": {
            "values": {
                "enum": ["BLAM", "KAPOW"],
            }
        },
        # # Descriminator (oneof)
        # "bit": {
        #     "discriminator": "bitType",
        #     "mapping": {
        #         "SCREW_DRIVER": {
        #             "properties": {
        #                 "isPhillips": {"type": "boolean"},
        #             }
        #         },
        #         "DRILL": {
        #             "properties": {
        #                 "size": {"type": "float32"},
        #             }
        #         },
        #     },
        # },
    },
    # Ensure that optionalProperties are also handled
    "optionalProperties": {
        "metoo": {
            "type": "string",
        }
    },
}

if __name__ == "__main__":
    alog.configure(os.environ.get("LOG_LEVEL", "info"))

    desc = jtd_to_proto(jtd_def, "Foo", validate_jtd=True)
    print(desc)
    proto_file_content = descriptor_to_file(desc)
    print(proto_file_content)
    with open("foo.proto", "w") as handle:
        handle.write(proto_file_content)

    # Standard
    import shlex
    import subprocess

    subprocess.run(
        shlex.split("python -m grpc_tools.protoc foo.proto --python_out='.' -I .")
    )
