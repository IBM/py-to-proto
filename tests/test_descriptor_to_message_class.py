"""
Tests for descriptor_to_message_class
"""

# Standard
import os
import tempfile

# Third Party
from google.protobuf import message
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper

# Local
from .conftest import temp_dpool
from py_to_proto.descriptor_to_message_class import descriptor_to_message_class
from py_to_proto.jtd_to_proto import jtd_to_proto


def test_descriptor_to_message_class_generated_descriptor(temp_dpool):
    """Make sure that a generated descriptor can be used to create a class"""
    descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )
    Foo = descriptor_to_message_class(descriptor)
    assert issubclass(Foo, message.Message)
    foo = Foo(foo=True, bar=1.234)
    assert foo.foo is True
    assert foo.bar is not None  # NOTE: There are precision errors comparing == 1.234

    # Make sure the class can be serialized
    serialized_content = Foo.to_proto_file()
    assert "message Foo" in serialized_content


def test_descriptor_to_message_class_write_proto_file(temp_dpool):
    """Make sure that each message class has write_proto_files attached to it
    and that it correctly writes the protobufs to the right named files.
    """
    Foo = descriptor_to_message_class(
        jtd_to_proto(
            name="Foo",
            package="foobar",
            jtd_def={
                "properties": {
                    "foo": {
                        "type": "boolean",
                    },
                }
            },
            descriptor_pool=temp_dpool,
        )
    )

    Bar = descriptor_to_message_class(
        jtd_to_proto(
            name="Bar",
            package="foobar",
            jtd_def={
                "properties": {
                    "bar": {
                        "type": Foo.DESCRIPTOR,
                    },
                },
            },
            descriptor_pool=temp_dpool,
        ),
    )

    with tempfile.TemporaryDirectory() as workdir:
        Foo.write_proto_file(workdir)
        Bar.write_proto_file(workdir)
        assert set(os.listdir(workdir)) == {
            Foo.DESCRIPTOR.file.name,
            Bar.DESCRIPTOR.file.name,
        }
        with open(os.path.join(workdir, Bar.DESCRIPTOR.file.name), "r") as handle:
            bar_content = handle.read()
        assert f'import "{Foo.DESCRIPTOR.file.name}"' in bar_content


def test_descriptor_to_message_class_write_proto_file_no_dir(temp_dpool):
    """Make sure that each message class has write_proto_files attached to it
    and that it correctly writes the protobufs to the right named files.
    Also ensures that the directory gets created if it doesn't exist
    """
    Foo = descriptor_to_message_class(
        jtd_to_proto(
            name="Foo",
            package="foobar",
            jtd_def={
                "properties": {
                    "foo": {
                        "type": "boolean",
                    },
                }
            },
            descriptor_pool=temp_dpool,
        )
    )

    with tempfile.TemporaryDirectory() as workdir:
        protos_dir_path = os.path.join(workdir, "protos")
        Foo.write_proto_file(protos_dir_path)
        assert set(os.listdir(protos_dir_path)) == {
            Foo.DESCRIPTOR.file.name,
        }


def test_descriptor_to_message_class_nested_messages(temp_dpool):
    """Make sure that nested messages are wrapped and added to the parents"""
    top = descriptor_to_message_class(
        jtd_to_proto(
            name="Top",
            package="foobar",
            jtd_def={
                "properties": {
                    "ghost": {
                        "properties": {
                            "boo": {
                                "type": "string",
                            }
                        }
                    }
                }
            },
            descriptor_pool=temp_dpool,
        )
    )
    assert issubclass(top, message.Message)
    assert issubclass(top.Ghost, message.Message)


def test_descriptor_to_message_class_nested_enums(temp_dpool):
    """Make sure that nested enums are wrapped and added to the parents"""
    top = descriptor_to_message_class(
        jtd_to_proto(
            name="Top",
            package="foobar",
            jtd_def={
                "properties": {
                    "bat": {
                        "enum": ["VAMPIRE", "BASEBALL"],
                    }
                }
            },
            descriptor_pool=temp_dpool,
        )
    )
    assert issubclass(top, message.Message)
    assert isinstance(top.Bat, EnumTypeWrapper)


def test_descriptor_to_message_class_top_level_enum(temp_dpool):
    """Make sure that a top-level EnumDescriptor results in an EnumTypeWrapper"""
    top = descriptor_to_message_class(
        jtd_to_proto(
            name="Top",
            package="foobar",
            jtd_def={"enum": ["VAMPIRE", "DRACULA"]},
            descriptor_pool=temp_dpool,
        )
    )
    assert isinstance(top, EnumTypeWrapper)
    with tempfile.TemporaryDirectory() as workdir:
        top.write_proto_file(workdir)
        assert os.listdir(workdir) == [top.DESCRIPTOR.file.name]


def test_multiple_invocations_of_descriptor_to_message(temp_dpool):
    """Ensure that invoking descriptor_to_message_class with the same descriptor
    returns the same instance of a class.
    """
    descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )
    Foo = descriptor_to_message_class(descriptor)
    foo = Foo(foo=True, bar=1.234)

    Bar = descriptor_to_message_class(descriptor)
    bar = Bar(foo=True, bar=1.234)

    assert Foo is Bar
    assert Foo == Bar
    assert id(Foo) == id(Bar)
    assert foo == bar
