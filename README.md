# PY To Proto

This library holds utilities for converting in-memory data schema representations to [Protobuf](https://developers.google.com/protocol-buffers). The intent is to allow python libraries to leverage the power of `protobuf` while maintaining the source-of-truth for their data in pure python and avoiding static build steps.

## Why?

The `protobuf` langauge is a powerful tool for defining language-agnostic, composable datastructures. `Protobuf` also offers cross-language compatibility so that a given set of definitions can be compiled into numerous target programming languages. The downside is that `protobuf` requires_a static built step to perform this `proto` -> `X` conversion step. Alternately, there are multiple ways of representing data schemas in pure python which allow a python library to interact with well-typed data objects. The downside here is that these structures can not easily be used from other programming languages. The pros/cons of these generally fall along the following lines:

-   `Protobuf`:
    -   **Advantages**
        -   Compact serialization
        -   Auto-generated [`grpc`](https://grpc.io/) client and service libraries
        -   Client libraries can be used from different programming languages
    -   **Disadvantages**
        -   Learning curve to understand the full ecosystem
        -   Not a familiar tool outside of service engineering
        -   Static compilation step required to use in code
-   Python schemas:
    -   **Advantages**
        -   Can be learned quickly using pure-python documentation
        -   Can be written inline in pure python
    -   **Disadvantages**
        -   Generally, no standard serialization beyond `json`
        -   No automated service implementations
        -   No/manual mechanism for usage in other programming languages

This project aims to bring the advantages of both types of schema representation so that a given project can take advantage of the best of both:

-   Define your structures in pure python for simplicity
-   Dynamically create [`google.protobuf.Descriptor`](https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/descriptor.py#L245) objects to allow for `protobuf` serialization and deserialization
-   Reverse render a `.proto` file from the generated `Descriptor` so that stubs can be generated in other languages
-   No static compiliation needed!

## Supported Python Schema Types

Currently, objects can be declared using either [python `dataclasses`](https://docs.python.org/3/library/dataclasses.html) or [Json TypeDef (JTD)](https://jsontypedef.com/). Additional schemas can be added by [subclassing `ConverterBase`](py_to_proto/converter_base.py).

### Dataclass To Proto

The following example illustrates how `dataclasses` and `enums` can be converted to proto:

```py
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Dict, List, Enum
import py_to_proto

# Define the Foo structure as a python dataclass, including a nested enum
@dataclass
class Foo:

    class BarEnum(Enum):
        EXAM: 0
        JOKE_SETTING: 1

    foo: bool
    bar: List[BarEnum]

# Define the Foo protobuf message class
FooProto = py_to_proto.descriptor_to_message_class(
    py_to_proto.dataclass_to_proto(
        package="foobar",
        dataclass_=Foo,
    )
)

# Declare the Bar structure as a python dataclass with a reference to the
# FooProto type
@dataclass
class Bar:
    baz: FooProto

# Define the Bar protobuf message class
BarProto = py_to_proto.descriptor_to_message_class(
    py_to_proto.dataclass_to_proto(
        package="foobar",
        dataclass_=Bar,
    )
)

# Instantiate a BarProto
print(BarProto(baz=FooProto(foo=True, bar=[Foo.BarEnum.EXAM.value])))

def write_protos(proto_dir: str):
    """Write out the .proto files for FooProto and BarProto to the given
    directory
    """
    FooProto.write_proto_file(proto_dir)
    BarProto.write_proto_file(proto_dir)
```

### JTD To Proto

The following example illustrates how JTD schemas can be converted to proto:

```py
import py_to_proto

# Declare the Foo protobuf message class
Foo = py_to_proto.descriptor_to_message_class(
    py_to_proto.jtd_to_proto(
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

# Declare an object that references Foo as the type for a field
Bar = py_to_proto.descriptor_to_message_class(
    py_to_proto.jtd_to_proto(
        name="Bar",
        package="foobar",
        jtd_def={
            "properties": {
                "baz": {
                    "type": Foo.DESCRIPTOR,
                },
            },
        },
    ),
)

def write_protos(proto_dir: str):
    """Write out the .proto files for Foo and Bar to the given directory"""
    Foo.write_proto_file(proto_dir)
    Bar.write_proto_file(proto_dir)
```

## Similar Projects

There are a number of similar projects in this space that offer slightly different value:

-   [`jtd-codegen`](https://jsontypedef.com/docs/jtd-codegen/): This project focuses on statically generating language-native code (including `python`) to represent the JTD schema.
-   [`py-json-to-proto`](https://pypi.org/project/py-json-to-proto/): This project aims to deduce a schema from an instance of a `json` object.
-   [`pure-protobuf`](https://pypi.org/project/pure-protobuf/): This project has a very similar aim to `jtd-to-proto`, but it skips the intermediate `descriptor` representation and thus is not able to produce native `message.Message` classes.
