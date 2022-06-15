"""
Tests for descriptor_to_file
"""

# Standard
from types import ModuleType
from typing import Optional
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
from .helpers import temp_dpool
from jtd_to_proto.descriptor_to_file import descriptor_to_file
from jtd_to_proto.jtd_to_proto import jtd_to_proto

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
        "metoo": {
            "type": "string",
        }
    },
}


def compile_proto_module(proto_content: str) -> Optional[ModuleType]:
    """Compile the proto file content locally"""
    with tempfile.TemporaryDirectory() as dirname:
        mod_name = "{}_temp".format(
            "".join([random.choice(string.ascii_lowercase) for _ in range(8)])
        )
        fname = os.path.join(dirname, f"{mod_name}.proto")
        with open(fname, "w") as handle:
            handle.write(proto_content)

        proc = subprocess.Popen(
            shlex.split(
                f"{sys.executable} -m grpc_tools.protoc -I '{dirname}' --python_out {dirname} {fname}"
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
                "Foo",
                "foo.bar",
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
        package foo.bar;

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
