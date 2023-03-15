"""
Tests for the jtd_to_proto logic
"""

# Third Party
from google.protobuf import any_pb2, descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf.descriptor import EnumDescriptor, FieldDescriptor
import pytest

# Local
from jtd_to_proto.json_to_service import json_to_service
from jtd_to_proto.jtd_to_proto import _to_upper_camel, jtd_to_proto

## Happy Path ##################################################################


def test_jtd_to_proto_primitives(temp_dpool):
    """Ensure that primitives in JTD can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "foo": {
                    "type": "boolean",
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["foo"]
    assert fields["foo"].type == fields["foo"].TYPE_BOOL
    assert fields["foo"].label == fields["foo"].LABEL_OPTIONAL


def test_jtd_to_proto_objects(temp_dpool):
    """Ensure that nested objects can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "buz": {
                    "properties": {
                        "bee": {
                            "type": "boolean",
                        }
                    },
                },
            },
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert len(descriptor.nested_types) == 1
    assert descriptor.nested_types[0].name == "Buz"
    assert descriptor.nested_types[0].full_name == ".".join([package, msg_name, "Buz"])
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["buz"]
    assert fields["buz"].type == fields["buz"].TYPE_MESSAGE
    assert fields["buz"].label == fields["buz"].LABEL_OPTIONAL


def test_jtd_to_proto_additonal_properties(temp_dpool):
    """Ensure that an object can use 'additionalProperties'"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "buz": {
                    "properties": {
                        "blat": {"type": "int8"},
                    },
                    "additionalProperties": True,
                },
            },
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    nested_types = list(descriptor.nested_types)
    assert len(nested_types) == 1
    assert nested_types[0].name == "Buz"
    assert nested_types[0].full_name == ".".join([package, msg_name, "Buz"])
    nested_fields = dict(nested_types[0].fields_by_name)
    assert set(nested_fields.keys()) == {"blat", "additionalProperties"}
    assert (
        nested_fields["additionalProperties"].type
        == nested_fields["additionalProperties"].TYPE_MESSAGE
    )
    assert (
        nested_fields["additionalProperties"].message_type.full_name
        == "google.protobuf.Struct"
    )
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["buz"]
    assert fields["buz"].type == fields["buz"].TYPE_MESSAGE
    assert fields["buz"].label == fields["buz"].LABEL_OPTIONAL


def test_jtd_to_proto_timestamp(temp_dpool):
    """Ensure that the timestamp type can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "time": {
                    "type": "timestamp",
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["time"]
    assert fields["time"].type == fields["time"].TYPE_MESSAGE
    assert fields["time"].message_type.full_name == "google.protobuf.Timestamp"
    assert fields["time"].label == fields["time"].LABEL_OPTIONAL


def test_jtd_to_proto_enum(temp_dpool):
    """Ensure that enums can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "bat": {
                    "enum": ["VAMPIRE", "DRACULA"],
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    enum_types = list(descriptor.enum_types)
    assert len(enum_types) == 1
    assert enum_types[0].name == "Bat"
    assert enum_types[0].full_name == ".".join([package, msg_name, "Bat"])
    assert {
        val_name: val.number for val_name, val in enum_types[0].values_by_name.items()
    } == {
        "VAMPIRE": 0,
        "DRACULA": 1,
    }
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["bat"]
    assert fields["bat"].type == fields["bat"].TYPE_ENUM
    assert fields["bat"].label == fields["bat"].LABEL_OPTIONAL


def test_jtd_to_proto_arrays_of_primitives(temp_dpool):
    """Ensure that arrays of primitives can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "foo": {
                    "elements": {
                        "type": "boolean",
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["foo"]
    assert fields["foo"].type == fields["foo"].TYPE_BOOL
    assert fields["foo"].label == fields["foo"].LABEL_REPEATED


def test_jtd_to_proto_arrays_of_objects(temp_dpool):
    """Ensure that arrays of objects can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "buz": {
                    "elements": {
                        "properties": {
                            "bee": {
                                "type": "boolean",
                            }
                        },
                    },
                },
            },
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    nested_types = list(descriptor.nested_types)
    assert len(nested_types) == 1
    assert nested_types[0].name == "Buz"
    assert nested_types[0].full_name == ".".join([package, msg_name, "Buz"])
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["buz"]
    assert fields["buz"].type == fields["buz"].TYPE_MESSAGE
    assert fields["buz"].label == fields["buz"].LABEL_REPEATED


def test_jtd_to_proto_arrays_of_enums(temp_dpool):
    """Ensure that arrays of enums can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "bat": {
                    "elements": {
                        "enum": ["VAMPIRE", "DRACULA"],
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    enum_types = list(descriptor.enum_types)
    assert len(enum_types) == 1
    assert enum_types[0].name == "Bat"
    assert enum_types[0].full_name == ".".join([package, msg_name, "Bat"])
    assert {
        val_name: val.number for val_name, val in enum_types[0].values_by_name.items()
    } == {
        "VAMPIRE": 0,
        "DRACULA": 1,
    }
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["bat"]
    assert fields["bat"].type == fields["bat"].TYPE_ENUM
    assert fields["bat"].label == fields["bat"].LABEL_REPEATED


def test_jtd_to_proto_maps_to_primitives(temp_dpool):
    """Ensure that maps with primitive values can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "biz": {
                    "values": {
                        "type": "float32",
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert len(descriptor.nested_types) == 1
    assert descriptor.nested_types[0].name == "BizEntry"
    assert {field.name: field.type for field in descriptor.nested_types[0].fields} == {
        "key": FieldDescriptor.TYPE_STRING,
        "value": FieldDescriptor.TYPE_FLOAT,
    }
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["biz"]
    assert fields["biz"].type == fields["biz"].TYPE_MESSAGE
    assert fields["biz"].label == fields["biz"].LABEL_REPEATED


def test_jtd_to_proto_maps_to_objects(temp_dpool):
    """Ensure that maps with object values can be converted"""
    msg_name = "SomethingElse"
    package = "something.else"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "bonk": {
                    "values": {
                        "properties": {
                            "how_hard": {"type": "float32"},
                        },
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert len(descriptor.nested_types) == 2
    nested_types = {typ.name: typ for typ in descriptor.nested_types}
    assert list(nested_types.keys()) == ["BonkEntry", "BonkValue"]
    assert {field.name: field.type for field in nested_types["BonkEntry"].fields} == {
        "key": FieldDescriptor.TYPE_STRING,
        "value": FieldDescriptor.TYPE_MESSAGE,
    }
    assert {field.name: field.type for field in nested_types["BonkValue"].fields} == {
        "how_hard": FieldDescriptor.TYPE_FLOAT,
    }
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["bonk"]
    assert fields["bonk"].type == fields["bonk"].TYPE_MESSAGE
    assert fields["bonk"].label == fields["bonk"].LABEL_REPEATED


def test_jtd_to_proto_maps_to_enums(temp_dpool):
    """Ensure that maps with enum values can be converted"""
    msg_name = "SomethingElse"
    package = "something.else"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "bang": {
                    "values": {
                        "enum": ["BLAM", "KAPOW"],
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert len(descriptor.nested_types) == 1
    assert descriptor.nested_types[0].name == "BangEntry"
    assert {field.name: field.type for field in descriptor.nested_types[0].fields} == {
        "key": FieldDescriptor.TYPE_STRING,
        "value": FieldDescriptor.TYPE_ENUM,
    }
    assert len(descriptor.enum_types) == 1
    assert descriptor.enum_types[0].name == "BangValue"
    assert {
        val_name: val.number
        for val_name, val in descriptor.enum_types[0].values_by_name.items()
    } == {
        "BLAM": 0,
        "KAPOW": 1,
    }
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["bang"]
    assert fields["bang"].type == fields["bang"].TYPE_MESSAGE
    assert fields["bang"].label == fields["bang"].LABEL_REPEATED


def test_jtd_to_proto_oneofs(temp_dpool):
    """Ensure that oneofs can be converted"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "bit": {
                    "discriminator": "bitType",
                    "mapping": {
                        "SCREW_DRIVER": {
                            "properties": {
                                "isPhillips": {"type": "boolean"},
                            }
                        },
                        "DRILL": {
                            "properties": {
                                "size": {"type": "float32"},
                            }
                        },
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert len(descriptor.nested_types) == 2
    nested_types = {typ.name: typ for typ in descriptor.nested_types}
    assert list(nested_types.keys()) == ["SCREWDRIVER", "DRILL"]
    assert not descriptor.enum_types
    assert len(descriptor.oneofs) == 1
    assert descriptor.oneofs[0].name == "bitType"

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["screw_driver", "drill"]
    assert fields["screw_driver"].type == fields["screw_driver"].TYPE_MESSAGE
    assert fields["screw_driver"].containing_oneof.name == "bitType"
    assert fields["screw_driver"].label == fields["screw_driver"].LABEL_OPTIONAL
    assert fields["drill"].type == fields["drill"].TYPE_MESSAGE
    assert fields["screw_driver"].containing_oneof.name == "bitType"
    assert fields["drill"].label == fields["drill"].LABEL_OPTIONAL


def test_jtd_to_proto_optional_properties(temp_dpool):
    """Ensure that entries in 'optionalProperties' are handled"""
    msg_name = "Foo"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "foo": {
                    "type": "boolean",
                },
            },
            "optionalProperties": {
                "metoo": {
                    "type": "string",
                }
            },
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate nested descriptors
    assert not descriptor.nested_types
    assert not descriptor.enum_types
    assert not descriptor.oneofs

    # Validate fields
    fields = dict(descriptor.fields_by_name)
    assert list(fields.keys()) == ["foo", "metoo"]
    assert fields["foo"].type == fields["foo"].TYPE_BOOL
    assert fields["foo"].label == fields["foo"].LABEL_OPTIONAL
    assert fields["metoo"].type == fields["metoo"].TYPE_STRING
    assert fields["metoo"].label == fields["metoo"].LABEL_OPTIONAL


def test_jtd_to_proto_top_level_enum(temp_dpool):
    """Make sure that a top-level enum can be converted"""
    msg_name = "SomeEnum"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {"enum": ["FOO", "BAR"]},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    assert isinstance(descriptor, EnumDescriptor)
    # Validate message naming
    assert descriptor.name == msg_name
    assert descriptor.full_name == ".".join([package, msg_name])

    # Validate enum values
    assert {
        val_name: val.number for val_name, val in descriptor.values_by_name.items()
    } == {
        "FOO": 0,
        "BAR": 1,
    }


def test_jtd_to_proto_reference_external_descriptor(temp_dpool):
    """Test that values in the JTD schema can be references to other in-memory
    descriptors
    """

    nested_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {"properties": {"foo": {"type": "string"}}},
        descriptor_pool=temp_dpool,
    )
    wrapper_descriptor = jtd_to_proto(
        "Bar",
        "foo.bar",
        {"properties": {"bar": {"type": nested_descriptor}}},
        descriptor_pool=temp_dpool,
    )
    assert wrapper_descriptor.fields_by_name["bar"].message_type is nested_descriptor


def test_jtd_to_proto_reference_external_enum_descriptor(temp_dpool):
    """Test that values in the JTD schema can be references to other in-memory
    enum descriptors
    """

    enum_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {"enum": ["FOO", "BAR"]},
        descriptor_pool=temp_dpool,
    )
    wrapper_descriptor = jtd_to_proto(
        "Bar",
        "foo.bar",
        {"properties": {"bar": {"type": enum_descriptor}}},
        descriptor_pool=temp_dpool,
    )
    assert wrapper_descriptor.fields_by_name["bar"].enum_type is enum_descriptor


def test_jtd_to_proto_bytes(temp_dpool):
    """Make sure that fields can have type bytes and that the messages can be
    validated even with bytes which is not in the JTD spec
    """
    bytes_descriptor = jtd_to_proto(
        "HasBytes",
        "foo.bar",
        {"properties": {"foo": {"type": "bytes"}}},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    bytes_field = bytes_descriptor.fields_by_name["foo"]
    assert bytes_field.type == bytes_field.TYPE_BYTES


def test_jtd_to_proto_any(temp_dpool):
    """Make sure that fields can have type Any and that the messages can be
    validated even with any which is not in the JTD spec
    """
    temp_dpool.AddSerializedFile(any_pb2.DESCRIPTOR.serialized_pb)
    bytes_descriptor = jtd_to_proto(
        "HasAny",
        "foo.bar",
        {"properties": {"foo": {"type": "any"}}},
        validate_jtd=True,
        descriptor_pool=temp_dpool,
    )
    bytes_field = bytes_descriptor.fields_by_name["foo"]
    assert bytes_field.type == bytes_field.TYPE_MESSAGE
    assert bytes_field.message_type.full_name == "google.protobuf.Any"


def test_jtd_to_proto_int64(temp_dpool):
    """Make sure that fields can have type int64 and that the messages can be
    validated.
    """
    int64_descriptor = jtd_to_proto(
        "HasInt64",
        "foo.bar",
        {"properties": {"foo": {"type": "int64"}}},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    int64_field = int64_descriptor.fields_by_name["foo"]
    assert int64_field.type == int64_field.TYPE_INT64


def test_jtd_to_proto_uint64(temp_dpool):
    """Make sure that fields can have type uint64 and that the messages can be
    validated.
    """
    uint64_descriptor = jtd_to_proto(
        "HasUInt64",
        "foo.bar",
        {"properties": {"foo": {"type": "uint64"}}},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    uint64_field = uint64_descriptor.fields_by_name["foo"]
    assert uint64_field.type == uint64_field.TYPE_UINT64


def test_jtd_to_proto_default_dpool():
    """This test ensures that without an explicitly passed descriptor pool, the
    default is used. THIS SHOULD BE THE ONLY TEST THAT DOESN'T USE `temp_dpool`!
    """
    jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {
                    "type": "boolean",
                },
            }
        },
    )

    # Tacking on a `jtd_to_service` test here as well so that we don't have
    # two tests each using the default descriptor pool
    json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def={
            "service": {
                "rpcs": [
                    {
                        "name": "FooPredict",
                        "input_type": "foo.bar.Foo",
                        "output_type": "foo.bar.Foo",
                    }
                ]
            }
        },
    )
    _descriptor_pool.Default().FindMessageTypeByName("foo.bar.Foo")


def test_jtd_to_proto_duplicate_message(temp_dpool):
    """Check that we can register the same message twice"""
    msg_name = "Message"
    package = "package"
    schema = {
        "properties": {
            "fooz": {"properties": {"bar": {"type": "boolean"}}},
        }
    }
    descriptor = jtd_to_proto(
        msg_name,
        package,
        schema,
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    descriptor2 = jtd_to_proto(
        msg_name,
        package,
        schema,
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )

    assert descriptor is descriptor2


def test_type_names_are_fully_qualified_with_nested_messages(temp_dpool):
    """Make sure that type_names are fully qualified with nested messages."""
    descriptor = jtd_to_proto(
        "First",
        "foo.bar",
        {
            "properties": {
                "second": {
                    "properties": {
                        "third": {"properties": {"fourth": {"type": "string"}}}
                    },
                },
            }
        },
        descriptor_pool=temp_dpool,
    )
    # Copy our descriptor over to a proto & make sure it's type_name look correct
    dproto = descriptor_pb2.DescriptorProto()
    descriptor.CopyToProto(dproto)
    package_mapping = {
        "Second": ".foo.bar.First.Second",
        "Third": ".foo.bar.First.Second.Third",
    }
    # Validate the direct field, i.e., Second, and make sure it has a fully qualified type name
    assert dproto.field[0].type_name == package_mapping["Second"]
    # Validate the nested message, i.e., Third, and make sure it has a fully qualified type name
    assert dproto.nested_type[0].field[0].type_name == package_mapping["Third"]


def test_type_names_are_fully_qualified_with_multiple_packages(temp_dpool):
    """Make sure that type_names are fully qualified with multiple packages."""
    bar_descriptor = jtd_to_proto(
        "Bar",
        "barpackage",
        {"properties": {"mydata": {"type": "string"}}},
        descriptor_pool=temp_dpool,
    )
    foo_descriptor = jtd_to_proto(
        "Foo",
        "foopackage",
        {
            "properties": {
                "external_friend": {"type": bar_descriptor},
            }
        },
        descriptor_pool=temp_dpool,
    )
    foo_dproto = descriptor_pb2.DescriptorProto()
    foo_descriptor.CopyToProto(foo_dproto)
    assert foo_dproto.field[0].type_name == ".barpackage.Bar"


def test_protoc_collision_same_def(temp_dpool):
    """Test that if we do jtd_to_proto -> protoc with the same underlying file name, it is okay."""
    # Happy because the file is named outermessage.proto, so we can find it!
    protoc_sample = b'\n\x12outermessage.proto\x12\x11test.jtd_to_proto"!\n\x0cOuterMessage\x12\x11\n\tprimitive\x18\x01 \x01(\tb\x06proto3'
    jtd_to_proto(
        name="OuterMessage",
        package="test.jtd_to_proto",
        jtd_def={"properties": {"primitive": {"type": "string"}}},
        validate_jtd=True,
        descriptor_pool=temp_dpool,
    )
    temp_dpool.AddSerializedFile(protoc_sample)


## Error Cases #################################################################


def test_jtd_to_proto_invalid_def():
    """Make sure that the validation catches an invalid JTD definition"""
    with pytest.raises(ValueError):
        jtd_to_proto("Foo", "foo.bar", {"foo": "bar"}, validate_jtd=True)


def test_jtd_to_proto_invalid_top_level():
    """Make sure that an error is raised if the top-level definition is a nested
    field specification
    """
    with pytest.raises(ValueError):
        jtd_to_proto("Foo", "foo.bar", {"type": "boolean"}, validate_jtd=True)


def test_jtd_to_proto_invalid_type_string():
    """Make sure that an error is raised if a type name is given that doesn't
    have a corresponding mapping
    """
    with pytest.raises(ValueError):
        jtd_to_proto(
            "Foo",
            "foo.bar",
            {
                "properties": {
                    "foo": {
                        "type": "widget",
                    },
                },
            },
            validate_jtd=False,
        )


def test_jtd_to_proto_explicit_additional_properties():
    """Make sure that an error is raised if a field is named
    'additionalProperties' and additionalProperties is set to True
    """
    with pytest.raises(ValueError):
        jtd_to_proto(
            "Foo",
            "foo.bar",
            {
                "properties": {
                    "additionalProperties": {
                        "type": "boolean",
                    },
                },
                "additionalProperties": True,
            },
            validate_jtd=False,
        )


def test_jtd_to_proto_duplicate_message_name(temp_dpool):
    """Check that we cannot register a different message with the same name"""
    msg_name = "Foo"
    package = "foo.bar"
    jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "foo": {
                    "type": "boolean",
                },
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {
                "properties": {
                    "bar": {
                        "type": "int32",
                    },
                }
            },
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_jtd_to_proto_duplicate_enum_name(temp_dpool):
    """Check that we cannot register a different message with the same name with wrong enum vals"""
    msg_name = "Foo"
    package = "foo.bar"
    # The respective values we are going to register
    first_enum_values = ["Hello", "World"]
    second_enum_values = ["Hi", "Planet"]
    jtd_to_proto(
        msg_name,
        package,
        {"enum": first_enum_values},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {"enum": second_enum_values},
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_jtd_to_proto_duplicate_enum_name_different_length(temp_dpool):
    """Check that we cannot register a different message with the same name & different enum len"""
    msg_name = "Foo"
    package = "foo.bar"
    # The respective values we are going to register
    first_enum_values = ["Hello", "World"]
    second_enum_values = ["Hello", "World", "And an extra value that we don't expect!"]
    jtd_to_proto(
        msg_name,
        package,
        {"enum": first_enum_values},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {"enum": second_enum_values},
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_jtd_to_proto_duplicate_nested_enums(temp_dpool):
    """Check that we cannot register a different message of the same name with sad nested enums"""
    msg_name = "Foo"
    package = "foo.bar"
    # Conflicting values for a nested enum in a message that otherwise aligns
    first_schema = {
        "properties": {
            "baz": {
                "enum": ["Hello", "World"],
            },
        },
    }
    second_schema = {
        "properties": {
            "baz": {
                "enum": ["Hello", "World", "And an extra value that we don't expect!"],
            },
        },
    }
    jtd_to_proto(
        msg_name,
        package,
        first_schema,
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            second_schema,
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_jtd_to_proto_sad_labels(temp_dpool):
    """Check that we cannot register a different message with field properties, e.g., label."""
    msg_name = "Foo"
    package = "foo.bar"
    jtd_to_proto(
        msg_name,
        package,
        {"properties": {"foo": {"type": "int32"}}},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {"properties": {"foo": {"type": "string"}}},
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_jtd_to_proto_misaligned_keys(temp_dpool):
    """Check that we cannot register a different message with missing enums, properties."""
    msg_name = "Foo"
    package = "foo.bar"
    jtd_to_proto(
        msg_name,
        package,
        {"enum": ["Hello", "World"]},
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {"properties": {"foo": {"type": "string"}}},
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_nested_registration_conflict(temp_dpool):
    """Check that we cannot register a different message with a nested change."""
    msg_name = "Foo"
    package = "foo.bar"
    jtd_to_proto(
        msg_name,
        package,
        {
            "properties": {
                "foo": {"properties": {"bar": {"type": "boolean"}}},
            }
        },
        descriptor_pool=temp_dpool,
        validate_jtd=True,
    )
    with pytest.raises(TypeError):
        jtd_to_proto(
            msg_name,
            package,
            {
                "properties": {
                    "foo": {"properties": {"a_sad_nest": {"type": "int32"}}},
                }
            },
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )


def test_protoc_collision_different_file_names_with_import_compiled_first(temp_dpool):
    """Test that we get a TypeError if we add a protoc compiled object -> do a sad JTD to proto."""
    # This is what happens when you end up importing something compiled by recent
    # versions of protoc; this was generated via protoc version 3.21.12.
    protoc_sample = b'\n\x10sadmessage.proto\x12\x11test.jtd_to_proto"!\n\x0cOuterMessage\x12\x11\n\tprimitive\x18\x01 \x01(\tb\x06proto3'
    temp_dpool.AddSerializedFile(protoc_sample)

    # NOTE - Since the name of the message (OuterMessage) does not match the proto file name
    # we compiled (sadmessage.proto), our direct validation is skipped, because we use that
    # to look up the FileDescriptor. Since we can't find the FileDescriptor, we (currently)
    # can't validate the message types it contains.
    with pytest.raises(TypeError):
        jtd_to_proto(
            name="OuterMessage",
            package="test.jtd_to_proto",
            jtd_def={"properties": {"primitive": {"type": "string"}}},
            validate_jtd=True,
            descriptor_pool=temp_dpool,
        )


def test_protoc_collision_different_file_names_with_import_compiled_last(temp_dpool):
    """Test that we get a TypeError if we do JTD to proto -> add a protoc compiled object."""
    jtd_to_proto(
        name="OuterMessage",
        package="test.jtd_to_proto",
        jtd_def={"properties": {"primitive": {"type": "string"}}},
        validate_jtd=True,
        descriptor_pool=temp_dpool,
    )

    # This is what happens when you end up importing something compiled by recent
    # versions of protoc; this was generated via protoc version 3.21.12.
    protoc_sample = b'\n\x10sadmessage.proto\x12\x11test.jtd_to_proto"!\n\x0cOuterMessage\x12\x11\n\tprimitive\x18\x01 \x01(\tb\x06proto3'
    # Descriptor pool does not like this because it is a different file descriptor and the def
    # of Message type OuterMessage changed; TypeError with duplicate symbols.
    # NOTE - yes, this is a test for the descriptor pool API, but the reason is we test the
    # opposite operation order above, and these error types should be consistent to make them
    # easier for people to understand & handle.
    with pytest.raises(TypeError):
        temp_dpool.AddSerializedFile(protoc_sample)


def test_protoc_collision_different_def_jtd_to_proto_first(temp_dpool):
    """Test that we get a TypeError if we JTD to proto -> import serialized def that conflicts."""
    jtd_to_proto(
        name="OuterMessage",
        package="test.jtd_to_proto",
        jtd_def={"properties": {"foobar": {"type": "int32"}}},
        validate_jtd=True,
        descriptor_pool=temp_dpool,
    )
    # NOTE: This is essentially testing the behavior of protobufs descriptor pool when you have a
    # conflict, but we do this explicitly here since we have a similar test for jtd to proto last;
    # the behavior of these things should be the same, otherwise we may have different behavior
    # based on import order.
    #
    # This is what happens when you end up importing something compiled by recent
    # versions of protoc; this was generated via protoc version 3.21.12.
    protoc_sample = b'\n\x12outermessage.proto\x12\x11test.jtd_to_proto"!\n\x0cOuterMessage\x12\x11\n\tprimitive\x18\x01 \x01(\tb\x06proto3'
    with pytest.raises(TypeError):
        temp_dpool.AddSerializedFile(protoc_sample)


def test_protoc_collision_different_def_jtd_to_proto_last(temp_dpool):
    """Test that we get a TypeError if we import serialized def -> JTD to proto that conflicts."""
    # This is what happens when you end up importing something compiled by recent
    # versions of protoc; this was generated via protoc version 3.21.12.
    protoc_sample = b'\n\x12outermessage.proto\x12\x11test.jtd_to_proto"!\n\x0cOuterMessage\x12\x11\n\tprimitive\x18\x01 \x01(\tb\x06proto3'
    temp_dpool.AddSerializedFile(protoc_sample)
    # Now that it's in our descriptor pool, we are sad in future JTD to proto calls!
    with pytest.raises(TypeError):
        jtd_to_proto(
            name="OuterMessage",
            package="test.jtd_to_proto",
            jtd_def={"properties": {"foobar": {"type": "int32"}}},
            validate_jtd=True,
            descriptor_pool=temp_dpool,
        )


## Details #####################################################################


def test_to_upper_camel_empty():
    """Make sure _to_upper_camel is safe with an empty string"""
    assert _to_upper_camel("") == ""
