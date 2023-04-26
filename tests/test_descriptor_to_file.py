"""
Tests for descriptor_to_file
"""

# Standard
from types import ModuleType
from typing import Dict, List, Optional
import importlib
import os
import random
import shlex
import string
import subprocess
import sys
import tempfile

# Third Party
import pytest

# First Party
import alog

# Local
from .conftest import temp_dpool
from py_to_proto.descriptor_to_file import descriptor_to_file
from py_to_proto.json_to_service import json_to_service
from py_to_proto.jtd_to_proto import jtd_to_proto

log = alog.use_channel("TEST")

## Helpers #####################################################################

sample_jtd_def = jtd_def = {
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
        # Enum
        "bat": {
            "enum": ["VAMPIRE", "DRACULA"],
        },
        # Array of enums
        "bif": {
            "elements": {
                "enum": ["NAME", "SOUND_EFFECT"],
            }
        },
        # Typed dict with primitive values
        "biz": {
            "values": {
                "type": "float32",
            }
        },
        # Dict with message values
        "bonk": {
            "values": {
                "properties": {
                    "how_hard": {"type": "float32"},
                }
            }
        },
        # Dict with enum values
        "bang": {
            "values": {
                "enum": ["BLAM", "KAPOW"],
            }
        },
        # Descriminator (oneof)
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
    },
    # optionalProperties are also handled as properties
    "optionalProperties": {
        # Optional primitive
        "optionalString": {
            "type": "string",
        },
        # Optional array
        "optionalList": {
            "elements": {
                "type": "string",
            }
        },
    },
}


def compile_proto_module(
    proto_content: str, imported_file_contents: Dict[str, str] = None
) -> Optional[ModuleType]:
    """Compile the proto file content locally"""
    with tempfile.TemporaryDirectory() as dirname:
        mod_name = "{}_temp".format(
            "".join([random.choice(string.ascii_lowercase) for _ in range(8)])
        )

        fname = os.path.join(dirname, f"{mod_name}.proto")
        with open(fname, "w") as handle:
            handle.write(proto_content)

        # Write out any files that need to be imported
        if imported_file_contents:
            for file_name, file_content in imported_file_contents.items():
                file_path = os.path.join(dirname, file_name)
                with open(file_path, "w") as handle:
                    handle.write(file_content)

        proto_files_to_compile = " ".join(os.listdir(dirname))

        proc = subprocess.Popen(
            shlex.split(
                f"{sys.executable} -m grpc_tools.protoc -I '{dirname}' --python_out {dirname} {proto_files_to_compile}"
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        log.debug("Std Out--------\n%s", stdout)
        log.debug("Std Err--------\n%s", stderr)
        if proc.returncode != 0:
            return

        # Put this dir on the sys.path and load the module
        sys.path.append(dirname)

        mod = importlib.import_module(f"{mod_name}_pb2")
        sys.path.pop()
        return mod


## Tests #######################################################################


def test_descriptor_to_file_compilable_proto(temp_dpool):
    """Make sure that the generated protobuf can be compiled"""
    assert compile_proto_module(
        descriptor_to_file(
            jtd_to_proto(
                "Widgets",
                "foo.bar.baz.bat",
                sample_jtd_def,
                descriptor_pool=temp_dpool,
                validate_jtd=True,
            )
        )
    )


def test_descriptor_to_file_non_generated_proto():
    """Make sure that a descriptor for an object generated with protoc can be
    serialized
    """
    # Make a "standard" protobuf module
    temp_pb2 = compile_proto_module(
        """
        syntax = "proto3";
        package foo.bar.baz.biz;

        enum FooEnum {
            FOO = 0;
            BAR = 1;
        }

        message MsgWithMap {
            map<string, string> the_map = 1;
        }

        message MsgWithOneof {
            oneof test_oneof {
                string str_version = 1;
                MsgWithMap msg_version = 2;
            }
        }
        """
    )
    assert temp_pb2

    # Try to serialize from the file descriptor
    auto_gen_content = descriptor_to_file(temp_pb2.DESCRIPTOR)
    assert "enum FooEnum" in auto_gen_content
    assert "message MsgWithMap" in auto_gen_content
    assert "message MsgWithOneof" in auto_gen_content

    # Serialize from one of the messages
    # NOTE: This just de-aliases to the file, so the generated content will hold
    #   all of the messages
    auto_gen_content = descriptor_to_file(temp_pb2.MsgWithMap.DESCRIPTOR)
    assert "enum FooEnum" in auto_gen_content
    assert "message MsgWithMap" in auto_gen_content
    assert "message MsgWithOneof" in auto_gen_content


def test_descriptor_to_file_invalid_descriptor_arg():
    """Make sure an error is raised if the argument is not a valid descriptor"""
    with pytest.raises(ValueError):
        descriptor_to_file({"foo": "bar"})


def test_descriptor_to_file_enum_descriptor(temp_dpool):
    """Make sure descriptor_to_file can be called on a EnumDescriptor"""
    enum_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {"enum": ["FOO", "BAR"]},
        descriptor_pool=temp_dpool,
    )
    res = descriptor_to_file(enum_descriptor)
    assert "enum Foo {" in res


def test_descriptor_to_file_optional_properties(temp_dpool):
    """Make sure descriptor_to_file sticks `optional` in front of optional fields"""
    raw_protobuf = descriptor_to_file(
        jtd_to_proto(
            "Widgets",
            "foo.bar.baz.bat",
            sample_jtd_def,
            descriptor_pool=temp_dpool,
            validate_jtd=True,
        )
    )
    raw_protobuf_lines = raw_protobuf.splitlines()
    # Non-array things in `optionalProperties` should have `optional`
    assert any(
        "optional string optionalString" in line for line in raw_protobuf_lines
    ), f"optionalString not in {raw_protobuf}"
    # But fields cannot be both `repeated` and `optional`
    assert any(
        "repeated string optionalList" in line for line in raw_protobuf_lines
    ), f"optionalList broken in {raw_protobuf}"
    # Additionally, check that the internal oneof was not rendered
    assert "_optionalString" not in raw_protobuf


def test_descriptor_to_file_service_descriptor(temp_dpool):
    """Make sure descriptor_to_file can be called on a ServiceDescriptor"""
    foo_message_descriptor = jtd_to_proto(
        name="Foo",
        package="foo.bar",
        jtd_def={
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )
    service_descriptor = json_to_service(
        name="FooService",
        package="foo.bar",
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
        descriptor_pool=temp_dpool,
    )
    # TODO: type annotation fixup
    res = descriptor_to_file(service_descriptor)
    assert "service FooService {" in res


def test_descriptor_to_file_compilable_proto_with_service_descriptor(temp_dpool):
    """Make sure descriptor_to_file can be called on a ServiceDescriptor"""

    random_message_name = "".join(
        [random.choice(string.ascii_lowercase) for _ in range(8)]
    )
    # 🌶️🌶️🌶️ The message names must be capitalized to work
    random_message_name = random_message_name.capitalize()

    foo_message_descriptor = jtd_to_proto(
        name=f"{random_message_name}",
        package="foo.bar",
        jtd_def={
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )
    message_descriptor_file = descriptor_to_file(foo_message_descriptor)
    imported_files = {foo_message_descriptor.file.name: message_descriptor_file}
    service_descriptor = json_to_service(
        name=f"{random_message_name}Service",
        package="foo.bar",
        json_service_def={
            "service": {
                "rpcs": [
                    {
                        "name": "FooPredict",
                        "input_type": f"foo.bar.{random_message_name}",
                        "output_type": f"foo.bar.{random_message_name}",
                    }
                ]
            }
        },
        descriptor_pool=temp_dpool,
    )
    res = descriptor_to_file(service_descriptor)
    assert compile_proto_module(res, imported_file_contents=imported_files)
