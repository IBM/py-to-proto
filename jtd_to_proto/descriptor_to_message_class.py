"""
This module implements a helper to create python classes from in-memory protobuf
Descriptor objects
"""

# Standard
from functools import wraps
from types import MethodType
from typing import Any, Callable, Type, Union
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
        message_class = EnumTypeWrapper(descriptor)

    # Handle message descriptors
    else:
        message_class = reflection.message_factory.MessageFactory().GetPrototype(
            descriptor
        )

        # Recursively add nested messages
        for nested_message_descriptor in descriptor.nested_types:
            nested_message_class = descriptor_to_message_class(
                nested_message_descriptor
            )
            setattr(message_class, nested_message_descriptor.name, nested_message_class)

        # Recursively add nested enums
        for nested_enum_descriptor in descriptor.enum_types:
            setattr(
                message_class,
                nested_enum_descriptor.name,
                descriptor_to_message_class(nested_enum_descriptor),
            )

    # Add to_proto_file
    if not hasattr(message_class, "to_proto_file"):

        def to_proto_file(first_arg) -> str:
            f"Create the serialized .proto file content holding all definitions for {descriptor.name}"
            return descriptor_to_file(first_arg.DESCRIPTOR)

        _maybe_classmethod(to_proto_file, message_class)

    # Add write_proto_file
    if not hasattr(message_class, "write_proto_file"):

        def write_proto_file(first_arg, root_dir: str = "."):
            "Write out the proto file to the target directory"
            with open(
                os.path.join(root_dir, first_arg.DESCRIPTOR.file.name), "w"
            ) as handle:
                handle.write(first_arg.to_proto_file())

        _maybe_classmethod(write_proto_file, message_class)

    return message_class


## Implementation Details ######################################################


def _maybe_classmethod(func: Callable, parent: Any):
    """Helper to attach the given function to the parent as either a classmethod
    of an instance method
    """

    if isinstance(parent, type):

        @classmethod
        @wraps(func)
        def _wrapper(cls, *args, **kwargs):
            return func(cls, *args, **kwargs)

    else:

        @wraps(func)
        def _wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        _wrapper = MethodType(_wrapper, parent)

    setattr(parent, func.__name__, _wrapper)
