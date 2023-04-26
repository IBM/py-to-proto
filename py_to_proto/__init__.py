"""
This library holds utilities for converting JSON Typedef to Protobuf.

Rerferences:
* https://jsontypedef.com/
* https://developers.google.com/protocol-buffers

Example:

```
import py_to_proto

# Declare the Foo protobuf message class
Foo = py_to_proto.descriptor_to_message_class(
    py_to_proto.py_to_proto(
        name="Foo",
        package="foobar",
        jtd_def={
            "properties": {
                # Bool field
                "foo": {
                    "type": "boolean",
                },
                # Array of nested enum values
                "bar": {
                    "elements": {
                        "enum": ["EXAM", "JOKE_SETTING"],
                    }
                }
            }
        },
    )
)

def write_foo_proto(filename: str):
    \"\"\"Write out the .proto file for Foo to the given filename\"\"\"
    with open(filename, "w") as handle:
        handle.write(Foo.to_proto_file())
```
"""

# Local
from .dataclass_to_proto import dataclass_to_proto
from .descriptor_to_file import descriptor_to_file
from .descriptor_to_message_class import descriptor_to_message_class
from .jtd_to_proto import jtd_to_proto
