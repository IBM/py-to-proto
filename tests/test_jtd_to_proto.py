"""
Tests for the jtd_to_proto logic
"""

# Third Party
from google.protobuf.descriptor import FieldDescriptor
import pytest

# Local
from .helpers import temp_dpool
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
    assert list(nested_fields.keys()) == ["additionalProperties"]
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


def test_jtd_to_proto_top_level_enum():
    """Make sure that a top-level enum can be converted

    NOTE: This test also validates the use of the default descriptor pool
    """
    msg_name = "SomeEnum"
    package = "foo.bar"
    descriptor = jtd_to_proto(
        msg_name,
        package,
        {"enum": ["FOO", "BAR"]},
        validate_jtd=True,
    )
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


def test_jtd_to_proto_reference_external_descriptor():
    """Test that values in the JTD schema can be references to other in-memory
    descriptors
    """

    nested_descriptor = jtd_to_proto(
        "Foo", "foo.bar", {"properties": {"foo": {"type": "string"}}}
    )
    wrapper_descriptor = jtd_to_proto(
        "Bar", "foo.bar", {"properties": {"bar": {"type": nested_descriptor}}}
    )
    assert wrapper_descriptor.fields_by_name["bar"].message_type is nested_descriptor


def test_jtd_to_proto_bytes():
    """Make sure that fields can have type bytes and that the messages can be
    validated even with bytes which is not in the JTD spec
    """
    bytes_descriptor = jtd_to_proto(
        "HasBytes",
        "foo.bar",
        {"properties": {"foo": {"type": "bytes"}}},
        validate_jtd=True,
    )
    bytes_field = bytes_descriptor.fields_by_name["foo"]
    assert bytes_field.type == bytes_field.TYPE_BYTES

def test_jtd_to_proto_int64s():
    """Make sure that fields can have type int64 and uint64 and that the messages
    can be validated even with these not in the JTD spec
    """
    bytes_descriptor = jtd_to_proto(
        "HasInt64s",
        "foo.bar",
        {"properties": {"foo": {"type": "int64"}, "baz": {"type": "uint64"}}},
        validate_jtd=True,
    )
    bytes_field = bytes_descriptor.fields_by_name["foo"]
    assert bytes_field.type == bytes_field.TYPE_INT64
    bytes_field = bytes_descriptor.fields_by_name["baz"]
    assert bytes_field.type == bytes_field.TYPE_UINT64

def test_jtd_to_proto_any():
    """Make sure that fields can have type Any and that the messages
    can be validated even with these not in the JTD spec
    """
    bytes_descriptor = jtd_to_proto(
        "HasInt64s",
        "foo.bar",
        {"properties": {"foo": {"type": "int64"}, "baz": {"type": "uint64"}}},
        validate_jtd=True,
    )
    bytes_field = bytes_descriptor.fields_by_name["foo"]
    assert bytes_field.type == bytes_field.TYPE_INT64
    bytes_field = bytes_descriptor.fields_by_name["baz"]
    assert bytes_field.type == bytes_field.TYPE_UINT64

## Error Cases #################################################################


def test_jtd_to_proto_invalid_def():
    """Make sure that the validation catches an invalid JTD definition"""
    with pytest.raises(AttributeError):
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


## Details #####################################################################


def test_to_upper_camel_empty():
    """Make sure _to_upper_camel is safe with an empty string"""
    assert _to_upper_camel("") == ""
