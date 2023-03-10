"""
This module implements serialization of an in-memory Descriptor to a portable
.proto file
"""

# Standard
from typing import List, Union

# Third Party
from google.protobuf import descriptor as _descriptor

## Globals #####################################################################


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

PROTO_FILE_SERVICES_HEADER = """
/*-- SERVICES ----------------------------------------------------------------*/
"""

PROTO_FILE_NESTED_ENUM_HEADER = f"{PROTO_FILE_INDENT}/*-- nested enums --*/"
PROTO_FILE_NESTED_MESSAGE_HEADER = f"{PROTO_FILE_INDENT}/*-- nested messages --*/"
PROTO_FILE_FIELD_HEADER = f"{PROTO_FILE_INDENT}/*-- fields --*/"
PROTO_FILE_ONEOF_HEADER = f"{PROTO_FILE_INDENT}/*-- oneofs --*/"


## Interface ###################################################################


def descriptor_to_file(
    descriptor: Union[
        _descriptor.FileDescriptor,
        _descriptor.Descriptor,
        _descriptor.ServiceDescriptor,
    ],
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
    if isinstance(
        descriptor,
        (
            _descriptor.Descriptor,
            _descriptor.EnumDescriptor,
            _descriptor.ServiceDescriptor,
        ),
    ):
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

    if descriptor.services_by_name:
        proto_file_lines.append(PROTO_FILE_SERVICES_HEADER)
        for service_descriptor in descriptor.services_by_name.values():
            proto_file_lines.extend(_service_descriptor_to_file(service_descriptor))
            proto_file_lines.append("")

    return "\n".join(proto_file_lines)


## Impl ########################################################################


def _indent_lines(indent: int, lines: List[str]) -> List[str]:
    """Add indentation to the given lines"""
    if not indent:
        return lines
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
    if message_descriptor.enum_types:
        lines.append("")
        lines.append(PROTO_FILE_NESTED_ENUM_HEADER)
    for enum_descriptor in message_descriptor.enum_types:
        lines.extend(_enum_descriptor_to_file(enum_descriptor, indent=1))

    # Add nested messages
    if message_descriptor.nested_types:
        lines.append("")
        lines.append(PROTO_FILE_NESTED_MESSAGE_HEADER)
    for nested_msg_descriptor in message_descriptor.nested_types:
        if _is_map_entry(nested_msg_descriptor):
            continue
        lines.extend(_message_descriptor_to_file(nested_msg_descriptor, indent=1))

    # Add fields
    if message_descriptor.fields:
        lines.append("")
        lines.append(PROTO_FILE_FIELD_HEADER)
    for field_descriptor in message_descriptor.fields:
        # If the field is part of a oneof, defer it until adding oneofs
        if field_descriptor.containing_oneof:
            continue
        lines.extend(_field_descriptor_to_file(field_descriptor, indent=1))

    # Add oneofs
    if message_descriptor.oneofs:
        lines.append("")
        lines.append(PROTO_FILE_ONEOF_HEADER)
    for oneof_descriptor in message_descriptor.oneofs:
        lines.extend(_oneof_descriptor_to_file(oneof_descriptor, indent=1))

    lines.append("}")
    return _indent_lines(indent, lines)


def _service_descriptor_to_file(
    service_descriptor: _descriptor.ServiceDescriptor,
    indent: int = 0,
) -> List[str]:
    """Make the string representation of a service"""
    lines = []
    lines.append(f"service {service_descriptor.name} {{")
    for method in service_descriptor.methods:
        lines.append(
            f"{PROTO_FILE_INDENT}rpc {method.name}({method.input_type.full_name}) returns ({method.output_type.full_name});"
        )
    lines.append("}")
    return _indent_lines(indent, lines)


def _field_descriptor_to_file(
    field_descriptor: _descriptor.FieldDescriptor,
    indent: int = 0,
) -> List[str]:
    """Get the string version of a field"""

    # Add the repeated qualifier if needed
    field_line = ""
    if (
        not _is_map_entry(field_descriptor.message_type)
        and field_descriptor.label == field_descriptor.LABEL_REPEATED
    ):
        field_line += "repeated "

    # Add the type
    field_line += _get_field_type_str(field_descriptor)

    # Add the name and number
    field_line += f" {field_descriptor.name} = {field_descriptor.number};"
    return _indent_lines(indent, [field_line])


def _oneof_descriptor_to_file(
    oneof_descriptor: _descriptor.OneofDescriptor,
    indent: int = 0,
) -> List[str]:
    """Get the string version of a oneof"""
    lines = []
    lines.append(f"oneof {oneof_descriptor.name} {{")
    for field_descriptor in oneof_descriptor.fields:
        lines.extend(_field_descriptor_to_file(field_descriptor, indent=1))
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
