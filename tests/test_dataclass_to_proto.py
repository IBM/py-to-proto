"""
Tests for dataclass_to_proto
"""

# Standard
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

# Third Party
from google.protobuf import any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2, timestamp_pb2
import pytest

# Local
from py_to_proto.dataclass_to_proto import (
    Annotated,
    FieldNumber,
    OneofField,
    dataclass_to_proto,
)
from py_to_proto.descriptor_to_message_class import descriptor_to_message_class
from py_to_proto.utils import _are_same_message_descriptor, to_upper_camel

## Helpers #####################################################################


def message_descriptors_match(
    d1: _descriptor.Descriptor, d2: _descriptor.Descriptor
) -> bool:
    """Helper to compare two in-memory descriptors that may not be the same
    instance, but may match. This is needed to compare external protos that have
    been copied to a non-default descriptor pool.
    """
    d1proto = descriptor_pb2.DescriptorProto()
    d2proto = descriptor_pb2.DescriptorProto()
    d1.CopyToProto(d1proto)
    d2.CopyToProto(d2proto)
    return _are_same_message_descriptor(d1proto, d2proto)


## Happy Path ##################################################################


def test_dataclass_to_proto_primitives(temp_dpool):
    """Make sure a dataclass with primitives works as expected"""

    @dataclass
    class Foo:
        foo: int
        bar: str

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool, validate=True)
    assert desc.fields_by_name["foo"].type == desc.fields_by_name["foo"].TYPE_INT64
    assert desc.fields_by_name["bar"].type == desc.fields_by_name["bar"].TYPE_STRING


def test_dataclass_to_proto_proto_nested_message(temp_dpool):
    """Make sure that a dataclass with a reference to another dataclass has the
    upstream as a nested message
    """

    @dataclass
    class Foo:
        foo: int

    @dataclass
    class Bar:
        bar: Foo

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.nested_types_by_name["Foo"]
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    assert bar_fld.message_type is foo_desc


def test_dataclass_to_proto_python_nested_message(temp_dpool):
    """Make sure that a dataclass with a reference to another dataclass declared
    inside the class itself has the upstream as a nested message
    """

    @dataclass
    class Bar:
        @dataclass
        class Foo:
            foo: int

        bar: Foo

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.nested_types_by_name["Foo"]
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    assert bar_fld.message_type is foo_desc


def test_dataclass_to_proto_message_reference(temp_dpool):
    """Make sure that a dataclass with a reference to another message descriptor
    references the other message without it being a nested message
    """

    @dataclass
    class Foo:
        foo: int

    foo_desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)

    @dataclass
    class Bar:
        bar: foo_desc

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    assert not bar_desc.nested_types_by_name
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    assert bar_fld.message_type is foo_desc


def test_dataclass_to_proto_nested_enum(temp_dpool):
    """Make sure that a dataclass with a reference to a Enum has the upstream as
    a nested enum
    """

    class FooEnum(Enum):
        FOO = 1
        BAR = 2

    @dataclass
    class Bar:
        bar: FooEnum

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.enum_types_by_name["FooEnum"]
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_ENUM
    assert bar_fld.enum_type is foo_desc

    # Make sure that the implicit zero-value was added to FooEnum
    assert 0 in foo_desc.values_by_number


def test_dataclass_to_proto_enum_reference(temp_dpool):
    """Make sure that a dataclass with a reference to an Enum descriptor
    references the other message without it being a nested enum
    """

    class FooEnum(Enum):
        FOO = 1
        BAR = 2

    foo_desc = dataclass_to_proto("foo.bar", FooEnum, descriptor_pool=temp_dpool)

    @dataclass
    class Bar:
        bar: foo_desc

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    assert not bar_desc.enum_types_by_name
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_ENUM
    assert bar_fld.enum_type is foo_desc


def test_dataclass_to_proto_any(temp_dpool):
    """Make sure that Any is converted to a protobuf Any correctly"""

    @dataclass
    class Foo:
        foo: Any

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_MESSAGE
    assert message_descriptors_match(foo_fld.message_type, any_pb2.Any.DESCRIPTOR)


def test_dataclass_to_proto_datetime(temp_dpool):
    """Make sure that a datetime is converted to a protobuf Timestamp correctly"""

    @dataclass
    class Foo:
        foo: datetime

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_MESSAGE
    assert message_descriptors_match(
        foo_fld.message_type, timestamp_pb2.Timestamp.DESCRIPTOR
    )


def test_dataclass_to_proto_map_to_primitive(temp_dpool):
    """Make sure that key/val maps with primitive values are handled correctly"""

    @dataclass
    class Foo:
        foo: Dict[int, str]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_MESSAGE
    map_desc = desc.nested_types_by_name["FooEntry"]
    assert foo_fld.message_type == map_desc
    assert map_desc.GetOptions().map_entry
    assert map_desc.fields_by_name["key"].type == foo_fld.TYPE_INT64
    assert map_desc.fields_by_name["value"].type == foo_fld.TYPE_STRING


def test_dataclass_to_proto_map_to_message(temp_dpool):
    """Make sure that key/val maps with message values are handled correctly"""

    @dataclass
    class Foo:
        foo: int

    foo_desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_msg_class = descriptor_to_message_class(foo_desc)

    @dataclass
    class Bar:
        bar: Dict[str, foo_msg_class]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)

    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    map_desc = bar_desc.nested_types_by_name["BarEntry"]
    assert bar_fld.message_type == map_desc
    assert map_desc.GetOptions().map_entry
    assert map_desc.fields_by_name["key"].type == bar_fld.TYPE_STRING
    assert map_desc.fields_by_name["value"].type == bar_fld.TYPE_MESSAGE
    assert map_desc.fields_by_name["value"].message_type == foo_desc


def test_dataclass_to_proto_map_to_enum(temp_dpool):
    """Make sure that key/val maps with enum values are handled correctly

    NOTE: Currently, the usage of `Dict[x, y]` restricts that `y` must be a type
        which means we cannot use an EnumDescriptorWrapper since it's an
        instance rather than a type. This gets at the awkwardness of how proto
        manages enums as instances versus messages as classes.
    """

    class FooEnum(Enum):
        FOO = 1
        BAR = 2

    @dataclass
    class Bar:
        bar: Dict[str, FooEnum]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.enum_types_by_name["BarFooEnum"]

    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    map_desc = bar_desc.nested_types_by_name["BarEntry"]
    assert bar_fld.message_type == map_desc
    assert map_desc.GetOptions().map_entry
    assert map_desc.fields_by_name["key"].type == bar_fld.TYPE_STRING
    assert map_desc.fields_by_name["value"].type == bar_fld.TYPE_ENUM
    assert map_desc.fields_by_name["value"].enum_type == foo_desc


def test_dataclass_to_proto_repeated_primitive(temp_dpool):
    """Make sure that a repeated field with primitive values is handled
    correctly
    """

    @dataclass
    class Foo:
        foo: List[bool]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_BOOL
    assert foo_fld.label == foo_fld.LABEL_REPEATED


def test_dataclass_to_proto_repeated_message(temp_dpool):
    """Make sure that a repeated field with message values is handled correctly"""

    @dataclass
    class Foo:
        foo: float

    @dataclass
    class Bar:
        bar: Annotated[List[Foo], "some", "other", "annotations"]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.nested_types_by_name["Foo"]
    foo_fld = foo_desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_DOUBLE
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.label == bar_fld.LABEL_REPEATED
    assert bar_fld.type == bar_fld.TYPE_MESSAGE
    assert bar_fld.message_type == foo_desc


def test_dataclass_to_proto_repeated_enum(temp_dpool):
    """Make sure that a repeated field with enum values is handled correctly"""

    class FooEnum(Enum):
        FOO = 1
        BAR = 2

    @dataclass
    class Bar:
        bar: List[FooEnum]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foo_desc = bar_desc.enum_types_by_name["FooEnum"]
    bar_fld = bar_desc.fields_by_name["bar"]
    assert bar_fld.label == bar_fld.LABEL_REPEATED
    assert bar_fld.type == bar_fld.TYPE_ENUM
    assert bar_fld.enum_type == foo_desc


def test_dataclass_to_proto_oneof_primitives(temp_dpool):
    """Make sure that a oneof with primitive fields works correctly"""

    @dataclass
    class Foo:
        foo: Union[bool, str]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    assert len(desc.oneofs) == 1
    oneof_desc = desc.oneofs_by_name["foo"]
    foobool_fld = desc.fields_by_name["foo_bool"]
    foostr_fld = desc.fields_by_name["foo_str"]
    assert foobool_fld.type == foobool_fld.TYPE_BOOL
    assert foobool_fld.containing_oneof is oneof_desc
    assert foostr_fld.type == foostr_fld.TYPE_STRING
    assert foostr_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_oneof_messages(temp_dpool):
    """Make sure that a oneof with message fields works correctly"""

    @dataclass
    class FooInt:
        foo: int

    @dataclass
    class FooStr:
        foo: str

    @dataclass
    class Bar:
        bar: Union[FooInt, FooStr]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    fooint_desc = bar_desc.nested_types_by_name["FooInt"]
    foostr_desc = bar_desc.nested_types_by_name["FooStr"]
    assert len(bar_desc.oneofs) == 1
    oneof_desc = bar_desc.oneofs_by_name["bar"]
    fooint_fld = bar_desc.fields_by_name["bar_fooint"]
    foostr_fld = bar_desc.fields_by_name["bar_foostr"]
    assert fooint_fld.type == fooint_fld.TYPE_MESSAGE
    assert fooint_fld.message_type == fooint_desc
    assert fooint_fld.containing_oneof is oneof_desc
    assert foostr_fld.type == foostr_fld.TYPE_MESSAGE
    assert foostr_fld.message_type == foostr_desc
    assert foostr_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_oneof_mixed(temp_dpool):
    """Make sure that a oneof with both primitive and message fields works
    correctly
    """

    @dataclass
    class FooStr:
        foo: str

    @dataclass
    class Bar:
        bar: Union[int, FooStr]

    bar_desc = dataclass_to_proto("foo.bar", Bar, descriptor_pool=temp_dpool)
    foostr_desc = bar_desc.nested_types_by_name["FooStr"]
    assert len(bar_desc.oneofs) == 1
    oneof_desc = bar_desc.oneofs_by_name["bar"]
    fooint_fld = bar_desc.fields_by_name["bar_int"]
    foostr_fld = bar_desc.fields_by_name["bar_foostr"]
    assert fooint_fld.type == fooint_fld.TYPE_INT64
    assert fooint_fld.containing_oneof is oneof_desc
    assert foostr_fld.type == foostr_fld.TYPE_MESSAGE
    assert foostr_fld.message_type == foostr_desc
    assert foostr_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_oneof_named_fields(temp_dpool):
    """Make sure that oneof fields can be named using Annotated types"""

    @dataclass
    class Foo:
        foo: Union[
            Annotated[bool, OneofField("foo_bool")],
            Annotated[str, OneofField("foo_str")],
        ]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    assert len(desc.oneofs) == 1
    oneof_desc = desc.oneofs_by_name["foo"]
    foobool_fld = desc.fields_by_name["foo_bool"]
    foostr_fld = desc.fields_by_name["foo_str"]
    assert foobool_fld.type == foobool_fld.TYPE_BOOL
    assert foobool_fld.containing_oneof is oneof_desc
    assert foostr_fld.type == foostr_fld.TYPE_STRING
    assert foostr_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_custom_field_numbers(temp_dpool):
    """Make sure that custom fields can be added with Annotated types"""

    @dataclass
    class Foo:
        foo: Union[
            Annotated[bool, FieldNumber(10), OneofField("foo_bool")],
            Annotated[str, FieldNumber(20), OneofField("foo_str")],
        ]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    assert len(desc.oneofs) == 1
    oneof_desc = desc.oneofs_by_name["foo"]
    foobool_fld = desc.fields_by_name["foo_bool"]
    foostr_fld = desc.fields_by_name["foo_str"]
    assert foobool_fld.number == 10
    assert foobool_fld.type == foobool_fld.TYPE_BOOL
    assert foobool_fld.containing_oneof is oneof_desc
    assert foostr_fld.number == 20
    assert foostr_fld.type == foostr_fld.TYPE_STRING
    assert foostr_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_optional_fields(temp_dpool):
    """Make sure that an optional field (with a default value) is handled with a
    true optional (local oneof)
    """

    @dataclass
    class Foo:
        foo: int = 42

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    assert len(desc.oneofs) == 1
    oneof_desc = desc.oneofs[0]
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == foo_fld.TYPE_INT64
    assert foo_fld.containing_oneof is oneof_desc


def test_dataclass_to_proto_enum_with_aliases(temp_dpool):
    """Make sure that an Enum with multiple keys mapping to the same value are
    handled as an enum alias in the descriptor
    """

    class FooEnum(Enum):
        RED = 1
        ROJO = 1
        GREEN = 2
        VERDE = 2

    desc = dataclass_to_proto("foo.bar", FooEnum, descriptor_pool=temp_dpool)
    assert desc.GetOptions().allow_alias
    assert desc.values_by_name["RED"].number == desc.values_by_name["ROJO"].number
    assert desc.values_by_name["GREEN"].number == desc.values_by_name["VERDE"].number


def test_dataclass_to_proto_enum_with_non_sequential_values(temp_dpool):
    """Make sure that an Enum with non sequential numbers works"""

    class FooEnum(Enum):
        RED = 10
        GREEN = 20

    desc = dataclass_to_proto("foo.bar", FooEnum, descriptor_pool=temp_dpool)
    assert not desc.GetOptions().allow_alias
    assert desc.values_by_name["RED"].number == 10
    assert desc.values_by_name["GREEN"].number == 20


def test_dataclass_to_proto_enum_with_defined_zero_values(temp_dpool):
    """Make sure that an Enum with an explicitly defined zero-value does not get
    the "special" zero placeholder
    """

    class FooEnum(Enum):
        RED = 0
        GREEN = 1

    desc = dataclass_to_proto("foo.bar", FooEnum, descriptor_pool=temp_dpool)
    assert set(desc.values_by_name.keys()) == {"RED", "GREEN"}


def test_dataclass_to_proto_custom_type_mapping(temp_dpool):
    """Make sure that a custom type mapping can be added for alternate typing"""

    @dataclass
    class Foo:
        foo: int

    desc = dataclass_to_proto(
        package="foo.bar",
        dataclass_=Foo,
        descriptor_pool=temp_dpool,
        type_mapping={int: _descriptor.FieldDescriptor.TYPE_UINT32},
    )
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == _descriptor.FieldDescriptor.TYPE_UINT32


def test_dataclass_to_proto_optional_field(temp_dpool):
    """Make sure that an Optional[] field is not treated as a oneof"""

    @dataclass
    class Foo:
        foo: Optional[Annotated[int, "foo"]]
        bar: Annotated[Optional[Union[str, int]], "foo"]
        baz: Optional[str]

    desc = dataclass_to_proto("foo.bar", Foo, descriptor_pool=temp_dpool)
    foo_fld = desc.fields_by_name["foo"]
    assert foo_fld.type == _descriptor.FieldDescriptor.TYPE_INT64
    oneof_desc = desc.oneofs_by_name["bar"]
    barstr_fld = desc.fields_by_name["bar_str"]
    assert barstr_fld.type == _descriptor.FieldDescriptor.TYPE_STRING
    assert barstr_fld.containing_oneof is oneof_desc
    barint_fld = desc.fields_by_name["bar_int"]
    assert barint_fld.type == _descriptor.FieldDescriptor.TYPE_INT64
    assert barint_fld.containing_oneof is oneof_desc
    baz_fld = desc.fields_by_name["baz"]
    assert baz_fld.type == _descriptor.FieldDescriptor.TYPE_STRING


## Error Cases #################################################################


def test_dataclass_to_proto_invalid_source_schema():
    """Make sure that with validation turned on, only dataclasses and Enums are
    allowed
    """
    with pytest.raises(ValueError):
        dataclass_to_proto("foo.bar", "not valid", validate=True)


def test_dataclass_to_proto_conflicting_annotations():
    """Make sure that if a field has conflicting Annotations, an error is raised"""

    @dataclass
    class Foo:
        foo: Annotated[int, FieldNumber(1), FieldNumber(2)]

    with pytest.raises(ValueError):
        dataclass_to_proto("foo.bar", Foo)


def test_dataclass_to_proto_invalid_field_number():
    """Make sure that invalid field numbers raise an error"""
    with pytest.raises(ValueError):
        FieldNumber(-1)
    with pytest.raises(ValueError):
        FieldNumber(0)
