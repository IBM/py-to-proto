"""
This module implements a helper to create python classes from in-memory protobuf
Descriptor objects
"""

# Standard
from typing import Type, Union
import os

# Third Party
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper

# Local
from .descriptor_to_file import descriptor_to_file


def descriptor_to_message_class(
    descriptor: Union[_descriptor.Descriptor, _descriptor.EnumDescriptor],
) -> Union[Type[_message.Message], EnumTypeWrapper]:
    """Create the proto class from the given descriptor

    Args:
        descriptor:  Union[_descriptor.Descriptor, _descriptor.EnumDescriptor]
            The message or enum Descriptor

    Returns:
        generated:  Union[Type[_message.Message], EnumTypeWrapper]
            The generated message class or the enum wrapper
    """
    # Handle enum descriptors
    if isinstance(descriptor, _descriptor.EnumDescriptor):
        return EnumTypeWrapper(descriptor)

    # Handle message descriptors

    message_class = reflection.message_factory.MessageFactory().GetPrototype(descriptor)

    # Add to_proto_file
    if not hasattr(message_class, "to_proto_file"):

        @classmethod
        def to_proto_file(cls) -> str:
            f"Create the serialized .proto file content holding all definitions for {descriptor.name}"
            return descriptor_to_file(cls.DESCRIPTOR)

        setattr(
            message_class,
            "to_proto_file",
            to_proto_file,
        )

    # Add write_proto_file
    if not hasattr(message_class, "write_proto_file"):

        @classmethod
        def write_proto_file(cls, root_dir: str = "."):
            "Write out the proto file to the target directory"
            with open(os.path.join(root_dir, cls.DESCRIPTOR.file.name), "w") as handle:
                handle.write(cls.to_proto_file())

        setattr(
            message_class,
            "write_proto_file",
            write_proto_file,
        )

    # Recursively add nested messages
    for nested_message_descriptor in descriptor.nested_types:
        nested_message_class = descriptor_to_message_class(nested_message_descriptor)
        setattr(message_class, nested_message_descriptor.name, nested_message_class)

    # Recursively add nested enums
    for nested_enum_descriptor in descriptor.enum_types:
        setattr(
            message_class,
            nested_enum_descriptor.name,
            descriptor_to_message_class(nested_enum_descriptor),
        )

    return message_class
