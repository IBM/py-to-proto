"""
This module implements recursive JTD schema validation

https://www.rfc-editor.org/rfc/rfc8927
"""
# Standard
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

# Third Party
from google.protobuf import descriptor as _descriptor

# First Party
import alog

log = alog.use_channel("JTD2P")

# Map of type validators for standard JTD types
JTD_TYPE_VALIDATORS = {
    "boolean": lambda x: isinstance(x, bool),
    "float32": lambda x: isinstance(x, (int, float)),
    "float64": lambda x: isinstance(x, (int, float)),
    "int8": lambda x: isinstance(x, int),
    "uint8": lambda x: isinstance(x, int) and x >= 0,
    "int16": lambda x: isinstance(x, int),
    "uint16": lambda x: isinstance(x, int) and x >= 0,
    "int32": lambda x: isinstance(x, int),
    "uint32": lambda x: isinstance(x, int) and x >= 0,
    "string": lambda x: isinstance(x, str),
    "timestamp": lambda x: isinstance(x, datetime),
}

# List of standard type names
JTD_TYPES = list(JTD_TYPE_VALIDATORS.keys())


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


def validate_jtd(
    obj: Any,
    schema: Dict[str, Any],
    type_validators: Optional[Dict[str, Callable[[Any], bool]]] = None,
) -> bool:
    """Validate the given object against the given schema

    Args:
        obj (Any)
            The candidate object to validate
        schema (Dict[str, Any])
            The schema to validate against
        type_validators (Optional[Dict[str, Callable[[Any], bool]]])
            Mapping from types string names to validation functions

    Returns:
        is_valid (bool)
            True if the object matches the schema, False otherwise
    """
    type_validators = type_validators or JTD_TYPE_VALIDATORS
    if not is_valid_jtd(schema, type_validators.keys()):
        raise ValueError(f"Invalid schema: {schema}")
    return _validate_jtd_impl(obj, schema, type_validators, is_root_schema=True)


## Implementation ##############################################################

_SHARED_KEYS = {"nullable", "metadata", "definitions"}


def _is_string_key_dict(value: Any) -> bool:
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


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
        if (
            # All protobuf descriptors are "special" cases
            not isinstance(type_val, _descriptor.Descriptor)
            and
            # All non-descriptor types must be valid types
            (not isinstance(type_val, str) or (type_val not in valid_types))
        ):
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
            # Mapping entries are of the "properties" form
            or any(
                "properties" not in val and "optionalProperties" not in val
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


def _validate_jtd_impl(
    obj: Any,
    schema: Dict[str, Any],
    type_validators: Dict[str, Callable[[Any], bool]],
    definitions: Optional[Dict[str, Any]] = None,
    *,
    is_root_schema: bool = False,
):
    """Recursive validation implementation"""

    # Pull out common definitions from the root that will be passed along
    # everywhere
    definitions = definitions or {}
    if is_root_schema:
        definitions = schema.get("definitions", {})

    # Check to see if this schema is null and nullable
    if obj is None and schema.get("nullable", False):
        return True

    # Get the set of keys in this schema with universal keys removed
    schema_keys = set(schema.keys()) - _SHARED_KEYS

    # Empty (3.3.1)
    if not schema_keys:
        return True

    # Ref (3.3.2)
    if schema_keys == {"ref"}:
        ref_val = schema["ref"]
        if not _validate_jtd_impl(
            obj, definitions[ref_val], type_validators, definitions
        ):
            log.debug4("Invalid value <%s> or ref <%s>", obj, ref_val)
            return False
        return True

    # Type (3.3.3)
    if schema_keys == {"type"}:
        type_val = schema["type"]
        validator = type_validators.get(type_val)
        if not (validator is not None and validator(obj)):
            log.debug4("Invalid value <%s> for type <%s>", obj, type_val)
            return False
        return True

    # Enum (3.3.4)
    if schema_keys == {"enum"}:
        enum_vals = schema["enum"]
        if obj not in enum_vals:
            log.debug4("Invalid enum value <%s> for enum <%s>", obj, enum_vals)
            return False
        return True

    # Elements (3.3.5)
    if schema_keys == {"elements"}:
        element_schema = schema["elements"]
        if not isinstance(obj, list) or any(
            not _validate_jtd_impl(entry, element_schema, type_validators, definitions)
            for entry in obj
        ):
            log.debug4(
                "Invalid elements value <%s> for element schema <%s>",
                obj,
                element_schema,
            )
            return False
        return True

    # Properties (3.3.6)
    if "properties" in schema_keys or "optionalProperties" in schema_keys:
        if not _is_string_key_dict(obj):
            log.debug4("Invalid properties <%s> is not a string key dict", obj)
            return False
        schema_properties = schema.get("properties", {})
        schema_opt_properties = schema.get("optionalProperties", {})
        if any(
            prop not in obj
            or not _validate_jtd_impl(
                obj[prop],
                prop_schema,
                type_validators,
                definitions,
            )
            for prop, prop_schema in schema_properties.items()
        ):
            log.debug4(
                "Invalid properties <%s> for properties %s", obj, schema_properties
            )
            return False
        if any(
            prop in obj
            and not _validate_jtd_impl(
                obj[prop], prop_schema, type_validators, definitions
            )
            for prop, prop_schema in schema_opt_properties.items()
        ):
            log.debug4(
                "Invalid optional properties <%s> for optional properties %s",
                obj,
                schema_opt_properties,
            )
            return False
        all_props = set.union(
            set(schema_properties.keys()), schema_opt_properties.keys()
        )
        if (
            not schema.get("additionalProperties", False)
            and set(obj.keys()) - all_props - _SHARED_KEYS
        ):
            log.debug4("Invalid additional properties in <%s> for %s", obj, schema)
            return False
        return True

    # Values (3.3.7)
    if schema_keys == {"values"}:
        value_schema = schema["values"]
        if not _is_string_key_dict(obj) or any(
            not _validate_jtd_impl(entry, value_schema, type_validators, definitions)
            for entry in obj.values()
        ):
            log.debug4("Invalid values <%s> for values schema <%s>", obj, value_schema)
            return False
        return True

    # Discriminator (3.3.8)
    if schema_keys == {"discriminator", "mapping"}:
        if not _is_string_key_dict(obj):
            log.debug4("Invalid discriminator <%s> which is not a string key dict", obj)
            return False
        schema_discriminator = schema["discriminator"]
        schema_mapping = schema["mapping"]
        discriminator_val = obj.get(schema_discriminator)
        if discriminator_val not in schema_mapping or not _validate_jtd_impl(
            {key: val for key, val in obj.items() if key != schema_discriminator},
            schema_mapping[discriminator_val],
            type_validators,
            definitions,
        ):
            log.debug4("Invalid discriminator <%s> for schema %s", obj, schema)
            return False
        return True

    # Since the schema must be valid, we should never get here!
    raise ValueError(f"Programming Error: unhandled schema {schema}")
