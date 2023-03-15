"""
This module implements recursive JTD schema validation

https://www.rfc-editor.org/rfc/rfc8927
"""
# Standard
from typing import Any, Dict, List, Optional

# First Party
import alog

log = alog.use_channel("JTD2P")

# List of standard type names
JTD_TYPES = [
    "boolean",
    "float32",
    "float64",
    "int8",
    "uint8",
    "int16",
    "uint16",
    "int32",
    "uint32",
    "string",
    "timestamp",
]


def is_valid_jtd(
    schema: Dict[str, Any], valid_types: Optional[List[str]] = None
) -> bool:
    """Determine whether the given dict represents a valid JTD schema

    Args:
        schema (Dict[str, Any])
            The candidate schema for validation
        valid_types (Optional[List[str]])
            List of valid type name strings. This defaults to the standard JTD
            types, but can be changed/extended to support additional types

    Returns:
        is_valid (bool)
            True if the schema is valid, False otherwise
    """
    valid_types = valid_types or JTD_TYPES
    return _is_valid_jtd_impl(schema, valid_types, is_root_schema=True)


## Implementation ##############################################################

_SHARED_KEYS = {"nullable", "metadata", "definitions"}


def _is_valid_jtd_impl(
    schema: Dict[str, Any],
    valid_types: List[str],
    definitions: Optional[Dict[str, Any]] = None,
    *,
    is_root_schema: bool = False,
) -> bool:
    """Recursive implementation of schema validation"""

    # Make sure it is a dict with string keys
    if not _is_string_key_dict(schema):
        log.debug4("Invalid jtd: Not a dict with string keys")
        return False

    # Check for metadata and/or nullable keywords which any form can contain
    if not isinstance(schema.get("nullable", False), bool):
        log.debug4("Invalid jtd: Found non-bool 'nullable'")
        return False
    if not _is_string_key_dict(schema.get("metadata", {})):
        log.debug4("Invalid jtd: Found 'metadata' that is not a dict of strings")
        return False

    # Definitions (2.1)
    definitions = definitions or {}
    if is_root_schema:
        definitions = schema.get("definitions", {})
        if not _is_string_key_dict(definitions):
            log.debug4("Invalid jtd: Found 'definitions' that is not a dict of strings")
            return False
        # TODO: Can definitions refer to _other_ definitions? The RFC is
        #   ambiguous here, so I think it should _technically_ be possible, but
        #   for our sake, we won't allow it for now.
        if any(
            not _is_valid_jtd_impl(val, valid_types) for val in definitions.values()
        ):
            log.debug4("Invalid jtd: Found 'definitions' value that is not valid jtd")
            return False
    elif "definitions" in schema:
        log.debug4("Found 'definitions' in non-root schema")
        return False

    # Get the set of keys in this schema with universal keys removed
    schema_keys = set(schema.keys()) - _SHARED_KEYS

    # Empty (2.2.1)
    if schema_keys == set():
        return True

    # Ref (2.2.2)
    if schema_keys == {"ref"}:
        ref_val = schema["ref"]
        if not isinstance(ref_val, str) or ref_val not in definitions:
            log.debug4("Invalid jtd: Bad reference <%s>", ref_val)
            return False
        return True

    # Type (2.2.3)
    if schema_keys == {"type"}:
        type_val = schema["type"]
        if not isinstance(type_val, str) or (type_val not in valid_types):
            log.debug4("Invalid jtd: Bad type <%s>", type_val)
            return False
        return True

    # Enum (2.2.4)
    if schema_keys == {"enum"}:
        enum_val = schema["enum"]
        if (
            not isinstance(enum_val, list)  # Must be a list
            or not enum_val  # Must be non-empty
            or len(set(enum_val)) != len(enum_val)  # Must have no duplicate entries
        ):
            log.debug4("Invalid jtd: Bad enum <%s>", enum_val)
            return False
        return True

    # Elements (2.2.5)
    if schema_keys == {"elements"}:
        elements_val = schema["elements"]
        if not _is_valid_jtd_impl(elements_val, valid_types, definitions):
            log.debug4("Invalid jtd: Bad elements <%s>", elements_val)
            return False
        return True

    # Properties (2.2.6)
    if "properties" in schema_keys or "optionalProperties" in schema_keys:
        properties_val = schema.get("properties", {})
        opt_properties_val = schema.get("optionalProperties", {})
        if (
            # No extra keys beyond additionalProperties
            schema_keys - {"properties", "optionalProperties", "additionalProperties"}
            # additionalProperties must be a bool
            or not isinstance(schema.get("additionalProperties", False), bool)
            # String dict properties
            or not _is_string_key_dict(properties_val)
            # String dict optionalProperties
            or not _is_string_key_dict(opt_properties_val)
            # Non-empty
            or (not properties_val and not opt_properties_val)
            # No overlapping keys
            or set(properties_val.keys()).intersection(opt_properties_val.keys())
            # Valid properties definitions
            or any(
                not _is_valid_jtd_impl(val, valid_types, definitions)
                for val in properties_val.values()
            )
            # Valid optionalProperties definitions
            or any(
                not _is_valid_jtd_impl(val, valid_types, definitions)
                for val in opt_properties_val.values()
            )
        ):
            log.debug4(
                "Invalid jtd: Bad properties <%s> / optionalProperties <%s>",
                properties_val,
                opt_properties_val,
            )
            return False
        return True

    # Values (2.2.7)
    if schema_keys == {"values"}:
        values_val = schema["values"]
        if not _is_valid_jtd_impl(values_val, valid_types, definitions):
            log.debug4("Invalid jtd: Bad 'values' <%s>", values_val)
            return False
        return True

    # Discriminator (2.2.8)
    if schema_keys == {"discriminator", "mapping"}:
        discriminator_val = schema["discriminator"]
        mapping_val = schema["mapping"]
        nullable = schema.get("nullable", False)
        if (
            # Discriminator is a string
            not isinstance(discriminator_val, str)
            # Mapping is a string dict
            or not _is_string_key_dict(mapping_val)
            # Mapping entries are valid JTD
            or any(
                not _is_valid_jtd_impl(val, valid_types, definitions)
                for val in mapping_val.values()
            )
            # Mapping entry "nullable" matches discriminator "nullable"
            or any(
                val.get("nullable", False) != nullable for val in mapping_val.values()
            )
            # Discriminator must not shadow properties in mapping elements
            or discriminator_val
            in set.union(
                *[
                    set(entry.get("properties", {}).keys())
                    for entry in mapping_val.values()
                ],
                *[
                    set(entry.get("optionalProperties", {}).keys())
                    for entry in mapping_val.values()
                ],
            )
        ):
            log.debug4(
                "Invalid jtd: Bad discriminator <%s> / mapping <%s>",
                discriminator_val,
                mapping_val,
            )
            return False
        return True

    # All other sets of keys are invalid
    log.debug4("Invalid jtd: Bad key set <%s>", schema_keys)
    return False


def _is_string_key_dict(value: Any) -> bool:
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)
