"""
Unit tests for validation logic. These tests exercise all examples from the RFC
https://www.rfc-editor.org/rfc/rfc8927
"""

# Third Party
import pytest

# First Party
import alog

# Local
from py_to_proto.jtd_to_proto import jtd_to_proto
from py_to_proto.validation import _validate_jtd_impl, is_valid_jtd, validate_jtd

log = alog.use_channel("TEST")

## is_valid_jtd ################################################################

SampleDescriptor = jtd_to_proto(
    "Sample", "foo.bar", {"properties": {"foo": {"type": "string"}}}
)


VALID_SCHEMAS = [
    # Empty
    {},
    {"nullable": True},
    {"metadata": {"foo": 12345}},
    {"definitions": {}},
    # Ref
    {
        "definitions": {
            "coordinates": {
                "properties": {"lat": {"type": "float32"}, "lng": {"type": "float32"}}
            }
        },
        "properties": {
            "user_location": {"ref": "coordinates"},
            "server_location": {"ref": "coordinates"},
        },
    },
    # Type
    {"type": "uint8"},
    {"type": SampleDescriptor},
    # Enum
    {"enum": ["PENDING", "IN_PROGRESS", "DONE"]},
    # Elements
    {"elements": {"type": "uint8"}},
    # Properties
    {"optionalProperties": {"foo": {}}},
    {"optionalProperties": {"foo": {}}, "additionalProperties": True},
    {
        "properties": {
            "users": {
                "elements": {
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "create_time": {"type": "timestamp"},
                    },
                    "optionalProperties": {"delete_time": {"type": "timestamp"}},
                }
            },
            "next_page_token": {"type": "string"},
        }
    },
    # Values
    {"values": {"type": "uint8"}},
    # Discriminator
    {
        "discriminator": "event_type",
        "mapping": {
            "account_deleted": {"properties": {"account_id": {"type": "string"}}},
            "account_payment_plan_changed": {
                "properties": {
                    "account_id": {"type": "string"},
                    "payment_plan": {"enum": ["FREE", "PAID"]},
                },
                "optionalProperties": {"upgraded_by": {"type": "string"}},
            },
        },
    },
    {
        "discriminator": "event_type",
        "nullable": True,
        "mapping": {
            "account_deleted": {
                "nullable": True,
                "properties": {"account_id": {"type": "string"}},
            },
        },
    },
]

INVALID_SCHEMAS = [
    # Empty
    {"nullable": "foo"},
    {"metadata": "foo"},
    # Ref
    {"ref": "foo"},
    {"ref": 1234},
    {"definitions": {"foo": {}}, "ref": "bar"},
    {"definitions": 1234},
    {"definitions": {"foo": {"definitions": {}}}},
    # Type
    {"type": True},
    {"type": "foo"},
    # Enum
    {"enum": []},
    {"enum": 1234},
    {"enum": ["a\\b", "a\u005Cb"]},
    # Elements
    {"elements": True},
    {"elements": {"type": "foo"}},
    # Properties
    {
        "properties": {"confusing": {}},
        "optionalProperties": {"confusing": {}},
    },
    {"optionalProperties": {}},
    {"properties": {}},
    {"properties": {}, "optionalProperties": {}},
    {"properties": 1234},
    {"optionalProperties": {"foo": {}}, "additionalProperties": 12345},
    # Values
    {"values": True},
    {"values": {"type": "foo"}},
    # Discriminator
    {
        "discriminator": "event_type",
        "mapping": {
            "can_the_object_be_null_or_not?": {
                "nullable": True,
                "properties": {"foo": {"type": "string"}},
            }
        },
    },
    {
        "discriminator": "event_type",
        "mapping": {
            "is_event_type_a_string_or_a_float32?": {
                "properties": {"event_type": {"type": "float32"}}
            }
        },
    },
    {
        "discriminator": "event_type",
        "mapping": {
            "is_event_type_a_string_or_an_optional_float32?": {
                "optionalProperties": {"event_type": {"type": "float32"}}
            }
        },
    },
    {
        "discriminator": "key",
        "mapping": {"int": {"type": "int32"}, "str": {"type": "string"}},
    },
]


@pytest.mark.parametrize("schema", VALID_SCHEMAS)
def test_valid_schemas(schema):
    """Make sure all valid schemas return True as expected"""
    log.debug("Testing valid schema: %s", schema)
    assert is_valid_jtd(schema)


@pytest.mark.parametrize("schema", INVALID_SCHEMAS)
def test_invalid_schemas(schema):
    """Make sure all invalid schemas return False as expected"""
    log.debug("Testing invalid schema: %s", schema)
    assert not is_valid_jtd(schema)


## validate_jtd ################################################################


class CustomClass:
    pass


# (object, schema)
VALID_JTD = [
    # Empty
    ({"foo": 1234, "bar": CustomClass()}, {}),
    (None, {"nullable": True}),
    (CustomClass(), {"metadata": {"foo": "bar"}}),
    # Ref
    (123, {"definitions": {"a": {"type": "float32"}}, "ref": "a"}),
    (None, {"definitions": {"a": {"type": "float32"}}, "ref": "a", "nullable": True}),
    # Type
    (123, {"type": "int32"}),
    (123, {"type": "float64"}),
    (1.23, {"type": "float64"}),
    (None, {"type": "boolean", "nullable": True}),
    # Enum
    ("FOO", {"enum": ["FOO", "BAR"]}),
    (None, {"enum": ["FOO", "BAR"], "nullable": True}),
    # Elements
    ([1, 2], {"elements": {"type": "int32"}}),
    ([], {"elements": {"type": "int32"}}),
    (None, {"elements": {"type": "int32"}, "nullable": True}),
    (
        [{"foo": 1}, {"foo": 2}],
        {"elements": {"properties": {"foo": {"type": "int32"}}}},
    ),
    # Properties
    ({"foo": 123}, {"properties": {"foo": {"type": "int32"}}}),
    ({"foo": ["bar"]}, {"properties": {"foo": {"elements": {"type": "string"}}}}),
    (
        {"foo": 123, "bar": "baz"},
        {"properties": {"foo": {"type": "int32"}, "bar": {"type": "string"}}},
    ),
    (
        {"foo": 123, "bar": "baz"},
        {
            "properties": {"foo": {"type": "int32"}},
            "optionalProperties": {"bar": {"type": "string"}},
        },
    ),
    (
        {"foo": 123},
        {
            "properties": {"foo": {"type": "int32"}},
            "optionalProperties": {"bar": {"type": "string"}},
        },
    ),
    ({}, {"optionalProperties": {"bar": {"type": "string"}}}),
    (
        {"buz": 123},
        {
            "optionalProperties": {"bar": {"type": "string"}},
            "additionalProperties": True,
        },
    ),
    # Values
    ({"foo": 123, "bar": -2}, {"values": {"type": "int32"}}),
    ({"foo": {"bar": -2}}, {"values": {"properties": {"bar": {"type": "int32"}}}}),
    # Discriminator
    (
        {"key": "str", "val": "this is a test"},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "int", "val": 123},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "int", "val_int": 123},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val_int": {"type": "int32"}}},
                "str": {"properties": {"val_str": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "str", "val_str": "asdf"},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val_int": {"type": "int32"}}},
                "str": {"properties": {"val_str": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "str", "val": "this is a test", "something": "else"},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {
                    "properties": {"val": {"type": "string"}},
                    "additionalProperties": True,
                },
            },
        },
    ),
]

INVALID_JTD = [
    # Ref
    (None, {"definitions": {"a": {"type": "float32"}}, "ref": "a", "nullable": False}),
    ({"foo": "bar"}, {"definitions": {"a": {"type": "float32"}}, "ref": "a"}),
    # Type
    (1.23, {"type": "int8"}),
    (-2, {"type": "uint8"}),
    (None, {"type": "boolean"}),
    # Enum
    ("BAZ", {"enum": ["FOO", "BAR"]}),
    (0, {"enum": ["FOO", "BAR"]}),
    ({}, {"enum": ["FOO", "BAR"]}),
    (None, {"enum": ["FOO", "BAR"]}),
    # Elements
    ([1, 2, "foo"], {"elements": {"type": "int32"}}),
    (None, {"elements": {"type": "int32"}, "nullable": False}),
    (
        [{"foo": 1}, {"foo": 2}],
        {"elements": {"properties": {"foo": {"type": "string"}}}},
    ),
    # Properties
    ({"foo": 123}, {"properties": {"foo": {"type": "string"}}}),
    ({"foo": [123]}, {"properties": {"foo": {"elements": {"type": "string"}}}}),
    (
        {"bar": "baz"},
        {"properties": {"foo": {"type": "int32"}, "bar": {"type": "string"}}},
    ),
    (
        {"bar": "baz"},
        {
            "properties": {"foo": {"type": "int32"}},
            "optionalProperties": {"bar": {"type": "string"}},
        },
    ),
    (
        {},
        {
            "properties": {"foo": {"type": "int32"}},
            "optionalProperties": {"bar": {"type": "string"}},
        },
    ),
    ({"buz": 123}, {"optionalProperties": {"bar": {"type": "string"}}}),
    (
        {"buz": 123},
        {
            "optionalProperties": {"bar": {"type": "string"}},
            "additionalProperties": False,
        },
    ),
    ({"bar": 123}, {"optionalProperties": {"bar": {"type": "string"}}}),
    ([{"foo": 123}], {"properties": {"foo": {"type": "string"}}}),
    # Values
    ({"foo": 123, "bar": "asdf"}, {"values": {"type": "int32"}}),
    ({"foo": {"bar": "test"}}, {"values": {"properties": {"bar": {"type": "int32"}}}}),
    # Discriminator
    (
        {"key": "str", "val": 123},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "int", "val": "asdf"},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
    (
        {"key": "str", "val": "this is a test", "something": "else"},
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
    (
        123,
        {
            "discriminator": "key",
            "mapping": {
                "int": {"properties": {"val": {"type": "int32"}}},
                "str": {"properties": {"val": {"type": "string"}}},
            },
        },
    ),
]


@pytest.mark.parametrize("obj,schema", VALID_JTD)
def test_valid_jtd(obj, schema):
    """Test all valid object validations"""
    log.debug("Comparing %s to %s", obj, schema)
    assert validate_jtd(obj, schema)


@pytest.mark.parametrize("obj,schema", INVALID_JTD)
def test_invalid_jtd(obj, schema):
    """Test all invalid object validations"""
    log.debug("Comparing %s to %s", obj, schema)
    assert not validate_jtd(obj, schema)


def test_custom_type_validator():
    """Make sure that a custom type validator works as expected"""
    assert validate_jtd(
        CustomClass(),
        {"type": "CustomClass"},
        {"CustomClass": lambda x: isinstance(x, CustomClass)},
    )
    assert not validate_jtd(
        123,
        {"type": "CustomClass"},
        {"CustomClass": lambda x: isinstance(x, CustomClass)},
    )


def test_validate_jtd_invalid_schema():
    """Make sure that an invalid schema causes an error in validate_jtd"""
    with pytest.raises(ValueError):
        validate_jtd({}, {"not": "a valid schema"})


def test_validate_jtd_impl_invalid_schema():
    """COV! Make sure an error is raised if somehow the schema isn't valid"""
    with pytest.raises(ValueError):
        _validate_jtd_impl({}, {"invalid": "schema"}, {})
