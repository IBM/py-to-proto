"""
Unit tests for validation logic. These tests exercise all examples from the RFC
https://www.rfc-editor.org/rfc/rfc8927
"""

# Third Party
import pytest

# First Party
import alog

# Local
from jtd_to_proto.validation import is_valid_jtd

log = alog.use_channel("TEST")

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
