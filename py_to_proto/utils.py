"""
Common utilities that are shared across converters
"""

# Standard
from typing import Any, List
import re

# Third Party
from google.protobuf import descriptor_pb2
import google.protobuf.descriptor_pool

# First Party
import alog

log = alog.use_channel("2PUTL")


def to_upper_camel(snake_str: str) -> str:
    """Convert a snake_case string to UpperCamelCase"""
    if not snake_str:
        return snake_str
    return (
        snake_str[0].upper()
        + re.sub("_([a-zA-Z])", lambda pat: pat.group(1).upper(), snake_str)[1:]
    )


def safe_add_fd_to_pool(
    fd_proto: descriptor_pb2.FileDescriptorProto,
    descriptor_pool: google.protobuf.descriptor_pool.DescriptorPool,
):
    """Safely add a new file descriptor to a descriptor pool. This function will
    look for naming collisions and if one occurs, it will validate the inbound
    descriptor against the conflicting descriptor in the pool to see if they are
    the same. If they are, no further action is taken. If they are not, an error
    is raised.
    """
    try:
        existing_fd = descriptor_pool.FindFileByName(fd_proto.name)
        # Rebuild the file descriptor proto so that we can compare; there is
        # almost certainly a more efficient way to compare that avoids this.
        existing_proto = descriptor_pb2.FileDescriptorProto()
        existing_fd.CopyToProto(existing_proto)
        # Raise if the file exists already with different content
        # Otherwise, do not attempt to re-add the file
        if not _are_same_file_descriptors(fd_proto, existing_proto):
            # NOTE: This is a TypeError because that is what you get most of the time when you
            # have conflict issues in the descriptor pool arising from JTD to Proto followed by
            # importing differing defs for the same top level message type using different file
            # names (i.e., skipping this validation) compiled by protoc. Raising TypeError here
            # ensures that we at least usually raise the same error type regardless of
            # import / operation order.
            raise TypeError(
                f"Cannot add new file {fd_proto.name} to descriptor pool, file already exists with different content"
            )
    except KeyError:
        # It's okay for the file to not already exist, we'll add it!
        try:
            descriptor_pool.Add(fd_proto)
        except TypeError as e:
            # More likely than not, this is a duplicate symbol; the main case in which
            # this could occur is when you've compiled files with protoc, added them to your
            # descriptor pool, and ALSO added the defs in your py_to_proto schema, but the
            # lookup validation with fd_proto.name is skipped because the .proto file fed to
            # protoc had a different name!
            raise TypeError(
                f"Failed to add {fd_proto.name} to descriptor pool with error: [{e}]; Hint: if you previously used protoc to compile this definition, you must recompile it with the name {fd_proto.name} to avoid the conflict."
            )


## Implementation Details ######################################################


def _are_same_file_descriptors(
    d1: descriptor_pb2.FileDescriptorProto, d2: descriptor_pb2.FileDescriptorProto
) -> bool:
    """Validate that there are no consistency issues in the message descriptors of
    our proto file descriptors.

    Args:
        d1: descriptor_pb2.FileDescriptorProto
            First FileDescriptorProto we want to compare.
        d2: descriptor_pb2.FileDescriptorProto
            second FileDescriptorProto we want to compare.

    Returns:
        True if the provided file descriptor proto files are identical.
    """
    have_same_deps = d1.dependency == d2.dependency
    are_same_package = d1.package == d2.package
    have_aligned_enums = _are_same_enum_descriptor(d1.enum_type, d2.enum_type)
    have_aligned_messages = _check_message_descs_alignment(
        d1.message_type, d2.message_type
    )
    have_aligned_services = _check_service_desc_alignment(d1.service, d2.service)
    return (
        have_same_deps
        and are_same_package
        and have_aligned_enums
        and have_aligned_messages
        and have_aligned_services
    )


def _are_same_enum_descriptor(d1_enums: Any, d2_enums: Any) -> bool:
    """Determine if two iterables of EnumDescriptorProtos have the same enums.
    This means the following:

    1. They have the same names in their respective .enum_type properties.
    2. For every enum in enum_type, they have the same number of values & the same names.

    Args:
        d1_enums: Any
            First iterable of enum desc protos to compare, e.g., RepeatedCompositeContainer.
        d2_enums: Any
            Second iterable of enum desc protos to compare, e.g., RepeatedCompositeContainer.

    Returns:
        True if the provided iterable enum descriptors are identical.
    """
    d1_enum_map = {enum.name: enum for enum in d1_enums}
    d2_enum_map = {enum.name: enum for enum in d2_enums}
    if d1_enum_map.keys() != d2_enum_map.keys():
        return False

    for enum_name in d1_enum_map.keys():
        d1_enum_descriptor = d1_enum_map[enum_name]
        d2_enum_descriptor = d2_enum_map[enum_name]
        if len(d1_enum_descriptor.value) != len(d2_enum_descriptor.value):
            return False
        # Compare each entry in the repeated composite container,
        # i.e., all of our EnumValueDescriptorProto objects
        for first_enum_val, second_enum_val in zip(
            d1_enum_descriptor.value, d2_enum_descriptor.value
        ):
            if (
                first_enum_val.name != second_enum_val.name
                or first_enum_val.number != second_enum_val.number
            ):
                return False
    return True


def _check_message_descs_alignment(
    d1_msg_container: Any, d2_msg_container: Any
) -> bool:
    """Determine if two message descriptor proto containers, i.e., RepeatedCompositeContainers
    have the same message types. This means the following:

    1. The messages contained in each FileDescriptorProto are the same.
    2. For each of those respective messages, their respective fields are roughly the same.
       Note that this includes nested_types, which are verified recursively.

    Args:
        d1_msg_container: Any
            First container iterable of message descriptors protos to be verified.
        d2_msg_container: Any
            Second container iterable of message descriptors protos to be verified.

    Returns:
        bool
            True if the contained message descriptor protos are identical.
    """
    d1_msg_descs = {msg.name: msg for msg in d1_msg_container}
    d2_msg_descs = {msg.name: msg for msg in d2_msg_container}

    # Ensure that our descriptors have the same dependencies & top level message types
    if d1_msg_descs.keys() != d2_msg_descs.keys():
        return False
    # For every encapsulated message descriptor, ensure that every field has the same
    # name, number, label, type, and type name
    for msg_name in d1_msg_descs.keys():
        d1_message_descriptor = d1_msg_descs[msg_name]
        d2_message_descriptor = d2_msg_descs[msg_name]
        # Ensure that these messages are actually the same
        if not _are_same_message_descriptor(
            d1_message_descriptor, d2_message_descriptor
        ):
            return False
    return True


def _check_service_desc_alignment(
    d1_service_list: List[descriptor_pb2.ServiceDescriptorProto],
    d2_service_list: List[descriptor_pb2.ServiceDescriptorProto],
) -> bool:
    d1_service_descs = {svc.name: svc for svc in d1_service_list}
    d2_service_descs = {svc.name: svc for svc in d2_service_list}

    log.debug(
        "Checking service descriptors: [%s] and [%s]",
        d1_service_descs,
        d2_service_descs,
    )
    # Ensure that our service names are the same set
    if d1_service_descs.keys() != d2_service_descs.keys():
        # Excluding from code coverage: We can't actually generate file descriptors with multiple services in them.
        # But, this check seems pretty basic and worth leaving in if this ever gets extended in the future.
        return False  # pragma: no cover

    # For every service, ensure that every method is the same
    for svc_name in d1_service_descs.keys():
        d1_service = d1_service_descs[svc_name]
        d2_service = d2_service_descs[svc_name]

        if not _are_same_service_descriptor(d1_service, d2_service):
            return False
    return True


def _are_same_service_descriptor(
    d1_service: descriptor_pb2.ServiceDescriptorProto,
    d2_service: descriptor_pb2.ServiceDescriptorProto,
) -> bool:
    # Not checking service.name because we only compare services with the same name

    d1_methods = {method.name: method for method in d1_service.method}
    d2_methods = {method.name: method for method in d2_service.method}

    # Ensure that our service names are the same set
    if d1_methods.keys() != d2_methods.keys():
        return False

    # For every service, ensure that every method is the same
    for method_name in d1_methods.keys():
        d1_method = d1_methods[method_name]
        d2_method = d2_methods[method_name]

        if not _are_same_method_descriptor(d1_method, d2_method):
            return False

    return True


def _are_same_method_descriptor(
    d1_method: descriptor_pb2.MethodDescriptorProto,
    d2_method: descriptor_pb2.MethodDescriptorProto,
) -> bool:
    # Not checking method.name because we only compare services with the same name

    if not _are_types_similar(d1_method.input_type, d2_method.input_type):
        return False
    if not _are_types_similar(d1_method.output_type, d2_method.output_type):
        return False
    # TODO: Add the ability for `json_to_service` to set options
    # Then we can test this!
    if d1_method.options != d2_method.options:
        log.debug(  # pragma: no cover
            "Method options differ! [%s] vs. [%s]", d1_method.options, d2_method.options
        )
        return False  # pragma: no cover
    if d1_method.client_streaming != d2_method.client_streaming:
        return False
    if d1_method.server_streaming != d2_method.server_streaming:
        return False
    return True


def _are_types_similar(type_1: str, type_2: str) -> bool:
    """Returns true iff type names are the same or differ only by a leading `.`"""
    # TODO: figure out why when you `json_to_service` the same thing twice, on of the service descriptors ends up with
    # fully qualified names (.foo.bar.Foo) and the other does not (foo.bar.Foo)
    return type_1.lstrip(".") == type_2.lstrip(".")


def _are_same_message_descriptor(
    d1: descriptor_pb2.DescriptorProto, d2: descriptor_pb2.DescriptorProto
) -> bool:
    """Determine if two message descriptors proto are representing the same thing. We do this by
    ensuring that their fields all have the same fields, then inspecting each of their labels,
    names, etc, for alignment. We do the same for any nested fields.

    Args:
        d1: descriptor_pb2.DescriptorProto
            First message descriptor to be compared.
        d2: descriptor_pb2.DescriptorProto
            second message descriptor to be compared.

    Returns:
        bool
            True of messages are identical, False otherwise.
    """
    # Compare any nested enums in our message.
    if not _are_same_enum_descriptor(d1.enum_type, d2.enum_type):
        return False
    # Make sure all of our named fields align, then check them individually
    d1_field_descs = {field.name: field for field in d1.field}
    d2_field_descs = {field.name: field for field in d2.field}
    if d1_field_descs.keys() != d2_field_descs.keys():
        return False
    for field_name in d1_field_descs.keys():
        # We consider two fields equal if they have the same name, label
        d1_field_descriptor = d1_field_descs[field_name]
        d2_field_descriptor = d2_field_descs[field_name]
        if (
            d1_field_descriptor.label != d2_field_descriptor.label
            or d1_field_descriptor.type != d2_field_descriptor.type
        ):
            return False
    # For nested fields, we treat them similarly to how we've treated messages
    # and recurse into comparisons used for the top level messages.
    if d1.nested_type or d2.nested_type:
        return _check_message_descs_alignment(d1.nested_type, d2.nested_type)
    # Otherwise, we have no more nested layers to check; we're done!
    return True
