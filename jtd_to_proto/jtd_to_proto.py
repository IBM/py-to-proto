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
import google.protobuf.descriptor_pool

# First Party
import alog

# Local
from .validation import is_valid_jtd

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


def _safe_add_fd_to_pool(
    fd_proto: descriptor_pb2.FileDescriptorProto,
    descriptor_pool: google.protobuf.descriptor_pool.DescriptorPool,
):
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
            # descriptor pool, and ALSO added the defs in your jtd_to_proto schema, but the
            # lookup validation with fd_proto.name is skipped because the .proto file fed to
            # protoc had a different name!
            raise TypeError(
                f"Failed to add {fd_proto.name} to descriptor pool with error: [{e}]; Hint: if you previously used protoc to compile this definition, you must recompile it with the name {fd_proto.name} to avoid the conflict."
            )


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
    # TODO: Add the ability for `json_to_service` to set options + streaming settings.
    # Then we can test this!
    if d1_method.options != d2_method.options:
        log.debug(  # pragma: no cover
            "Method options differ! [%s] vs. [%s]", d1_method.options, d2_method.options
        )
        return False  # pragma: no cover
    if d1_method.client_streaming != d2_method.client_streaming:
        return False  # pragma: no cover
    if d1_method.server_streaming != d2_method.server_streaming:
        return False  # pragma: no cover
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
        package=package,
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

    _safe_add_fd_to_pool(fd_proto, descriptor_pool)

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
    package: str,
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
    message_name = _to_upper_camel(name)
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
            package=package,
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
                                package=package,
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
                            package=package,
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
            if field.name in optional_properties.keys():
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
