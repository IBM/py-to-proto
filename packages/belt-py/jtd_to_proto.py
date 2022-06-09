# Standard
from typing import Dict, List, Optional, Union
import os
import re

# Third Party
from google.protobuf import descriptor, descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import struct_pb2, timestamp_pb2
import jtd

# First Party
import alog

log = alog.use_channel("JTD2P")


def descriptor_proto_from_descriptor(descriptor, msg_name):
    """Extract a DescriptorProto for a named message from a FileDescriptor. This
    is needed so that those messages can be used within fields below.
    """
    file_proto = descriptor_pb2.FileDescriptorProto()
    descriptor.CopyToProto(file_proto)
    return list(filter(lambda msg: msg.name == msg_name, file_proto.message_type))[0]


# Extract DescriptorProtos for struct_pb2.Proto and timestamp_pb2.Timestamp
STRUCT_PROTO = descriptor_proto_from_descriptor(struct_pb2.DESCRIPTOR, "Struct")
TIMESTAMP_PROTO = descriptor_proto_from_descriptor(
    timestamp_pb2.DESCRIPTOR, "Timestamp"
)

jtd_to_proto_types = {
    "boolean": descriptor.FieldDescriptor.TYPE_BOOL,
    "string": descriptor.FieldDescriptor.TYPE_STRING,
    "timestamp": timestamp_pb2.Timestamp,
    "float32": descriptor.FieldDescriptor.TYPE_FLOAT,
    "float64": descriptor.FieldDescriptor.TYPE_DOUBLE,
    # NOTE: All number types except fixed, double, and float are stored as
    #   varints meaning as long as your numbers stay in about the int8 or int16
    #   range, they are only 1 or 2 bytes, even though it says int32.
    #
    # CITE: https://groups.google.com/g/protobuf/c/Er39mNGnRWU/m/x6Srz_GrZPgJ
    "int8": descriptor.FieldDescriptor.TYPE_INT32,
    "uint8": descriptor.FieldDescriptor.TYPE_UINT32,
    "int16": descriptor.FieldDescriptor.TYPE_INT32,
    "uint16": descriptor.FieldDescriptor.TYPE_UINT32,
    "int8": descriptor.FieldDescriptor.TYPE_INT32,
    "uint8": descriptor.FieldDescriptor.TYPE_UINT32,
    "int32": descriptor.FieldDescriptor.TYPE_INT32,
    "uint32": descriptor.FieldDescriptor.TYPE_UINT32,
}

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
        # # Enum
        # "bat": {
        #     "enum": ["VAMPIRE", "DRACULA"],
        # },
        #
        # # Typed dict
        # "biz": {
        #     "values": {
        #         "type": "float32",
        #     }
        # },
        #
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
        #         }
        #     }
        # }
    },
    # Ensure that optionalProperties are also handled
    "optionalProperties": {
        "metoo": {
            "type": "string",
        }
    },
}


def jtd_to_proto(
    jtd_def: Dict[str, Union[dict, str]],
    name: str,
    package_name: str = "",
    *,
    validate_jtd: bool = False,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
):
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
        descriptor:  descriptor.FileDescriptor
    """
    # If performing validation, attempt to parse schema with jtd and throw away
    # the results
    if validate_jtd:
        log.debug2("Validating JTD")
        jtd.schema.Schema.from_dict(jtd_def)

    # This list will be used to aggregate the list of message DescriporProtos
    # for any nested message objects defined inline
    descriptor_protos = []
    enum_protos = []
    imports = []

    # Perform the recursive conversion to update the descriptors and enums in
    # place
    log.debug("Performing conversion")
    _jtd_to_proto_impl(
        jtd_def=jtd_def,
        name=name,
        package_name=package_name,
        path_elements=[],
        descriptor_protos=descriptor_protos,
        enum_protos=enum_protos,
        imports=imports,
    )

    # Create the FileDescriptorProto with all messages
    log.debug("Creating FileDescriptorProto")
    fd_proto = descriptor_pb2.FileDescriptorProto(
        name=f"{name.lower()}.proto",
        package=package_name,
        syntax="proto3",
        dependency=imports,
        public_dependency=list(range(len(imports))),
        message_type=descriptor_protos,
        enum_type=enum_protos,
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


def _jtd_to_proto_impl(
    *,
    jtd_def: Dict[str, Union[dict, str]],
    name: Optional[str],
    package_name: str,
    path_elements: List[str],
    descriptor_protos: List[descriptor_pb2.DescriptorProto],
    enum_protos: List[descriptor_pb2.EnumDescriptorProto],
    imports: List[str],
    is_repeated: bool = False,
) -> Union[
    descriptor_pb2.DescriptorProto,
    descriptor_pb2.EnumDescriptorProto,
    descriptor_pb2.FieldDescriptorProto,
]:
    """Recursive implementation of converting messages, fields, enums, arrays,
    and maps from JTD to their respective *DescriptorProto representations.

    Any DescriptorProto (message) and EnumDescriptorProto will be added to the
    given lists in-line and also returned so that it can be referenced as
    needed if it is a nested field type.
    """

    # If the jtd definition is a single "type": "name", perform the base-case
    # and look up the proto type
    type_name = jtd_def.get("type")
    if type_name is not None:
        if type_name is None:
            raise ValueError(f"No proto mapping for type '{type_name}'")

        # If this is a primitive, set up the type arguments accordingly
        proto_type_val = jtd_to_proto_types.get(type_name)
        if isinstance(proto_type_val, int):
            type_val = proto_type_val
            type_name = None

        # Otherwise, assume it's a known DescriptorProto
        else:
            type_val = descriptor.FieldDescriptor.TYPE_MESSAGE
            type_name = proto_type_val.DESCRIPTOR.full_name
            import_file = proto_type_val.DESCRIPTOR.file.name
            log.debug3(
                "Adding import file %s for known nested type %s",
                import_file,
                type_name,
            )
            imports.append(import_file)

        # Create the FieldDescriptor for the non-repeated field
        return descriptor_pb2.FieldDescriptorProto(
            name=name or path_elements[-1],
            type=type_val,
            # In proto3, everything is optional
            label=(
                descriptor.FieldDescriptor.LABEL_REPEATED
                if is_repeated
                else descriptor.FieldDescriptor.LABEL_OPTIONAL
            ),
            type_name=type_name,
        )

    # If the definition has "elements" it's a repeated field
    elements = jtd_def.get("elements")
    if elements is not None:
        nested = _jtd_to_proto_impl(
            jtd_def=elements,
            name=name,
            path_elements=path_elements,
            package_name=package_name,
            descriptor_protos=descriptor_protos,
            enum_protos=enum_protos,
            imports=imports,
            is_repeated=True,
        )
        if isinstance(nested, descriptor_pb2.DescriptorProto):
            return descriptor_pb2.FieldDescriptorProto(
                name=name or path_elements[-1],
                type=descriptor.FieldDescriptor.TYPE_MESSAGE,
                label=descriptor.FieldDescriptor.LABEL_REPEATED,
                type_name=nested.name,
            )
        return nested

    # If the definition has "enum" it's an enum
    enum = jtd_def.get("enum")
    if enum is not None:
        raise NotImplementedError("Enum not handled yet!")

    # If the definition has "values" it's a map
    values = jtd_def.get("values")
    if values is not None:
        raise NotImplementedError("Enum not handled yet!")

    # If the definition has "descriminator" in it, it's a oneof
    descriminator = jtd_def.get("descriminator")
    if descriminator is not None:
        mapping = jtd_def.get("mapping")
        if mapping is None:
            raise ValueError("No 'mapping' given with 'descriminator'")
        raise NotImplementedError("Descriminator not handled yet!")

    # If the object has "properties", it's going to create a net-new DESCRIPTOR
    properties = jtd_def.get("properties", {})
    optional_properties = jtd_def.get("optionalProperties", {})
    all_properties = dict(**properties, **optional_properties)
    additional_properties = jtd_def.get("additionalProperties")
    if all_properties or additional_properties:
        field_descriptors = []

        # Determine the name for this message based on the provided name and/or
        # path elements
        message_name = name or _to_upper_camel("_".join(path_elements))
        log.debug2("Message name: %s", message_name)

        # Iterate each field and perform the recursive conversion
        for field_index, (field_name, field_def) in enumerate(all_properties.items()):
            log.debug2(
                "Handling property [%s.%s] (%d)", message_name, field_name, field_index
            )
            log.debug3(field_def)

            # If the field is itself a message, determine a name for it
            if "properties" in field_def:
                nested = _jtd_to_proto_impl(
                    jtd_def=field_def,
                    name=None,
                    path_elements=list(
                        filter(
                            lambda x: x is not None,
                            path_elements + [name, field_name],
                        )
                    ),
                    package_name=package_name,
                    descriptor_protos=descriptor_protos,
                    enum_protos=enum_protos,
                    imports=imports,
                )
                field_descriptors.append(
                    descriptor_pb2.FieldDescriptorProto(
                        name=field_name,
                        type=descriptor.FieldDescriptor.TYPE_MESSAGE,
                        label=descriptor.FieldDescriptor.LABEL_OPTIONAL,
                        type_name=nested.name,
                        number=field_index + 1,
                    )
                )
            else:
                field_descriptor = _jtd_to_proto_impl(
                    jtd_def=field_def,
                    name=None,
                    package_name=package_name,
                    path_elements=path_elements + [message_name, field_name],
                    descriptor_protos=descriptor_protos,
                    enum_protos=enum_protos,
                    imports=imports,
                )
                field_descriptor.number = field_index + 1
                field_descriptors.append(field_descriptor)

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
                    number=len(all_properties) + 1,
                    type=descriptor.FieldDescriptor.TYPE_MESSAGE,
                    label=descriptor.FieldDescriptor.LABEL_OPTIONAL,
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
            # enum_types=None, # TODO: Fix with enum support
            # enum_types_by_name=None,
            # enum_values_by_name=None,
            # oneofs=None, # TODO: Fix with oneof support
            # oneofs_by_name=None,
        )
        descriptor_protos.append(descriptor_proto)
        return descriptor_proto


def _to_upper_camel(snake_str: str) -> str:
    """Convert a snake_case string to UpperCamelCase"""
    if not snake_str:
        return snake_str
    return (
        snake_str[0].upper()
        + re.sub("_([a-zA-Z])", lambda pat: pat.group(1).upper(), snake_str)[1:]
    )


if __name__ == "__main__":
    alog.configure(os.environ.get("LOG_LEVEL", "info"))

    print(jtd_to_proto(jtd_def, "Foo", validate_jtd=True))
