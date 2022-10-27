"""
Tests for descriptor_to_message_class
"""

# Standard
import os
import tempfile

# Third Party
from google.protobuf import message

# Local
from .helpers import temp_dpool
from jtd_to_proto.descriptor_to_message_class import descriptor_to_message_class
from jtd_to_proto.jtd_to_proto import jtd_to_proto


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
