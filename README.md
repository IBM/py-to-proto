# JTD To Proto

This library holds utilities for converting [JSON Typedef](https://jsontypedef.com/) to [Protobuf](https://developers.google.com/protocol-buffers).

## Why?

The `protobuf` langauge is a powerful tool for defining language-agnostic, composable datastructures. `JSON Typedef` (`JTD`) is _also_ a powerful tool to accomplish the same task. Both have advantages and disadvantages that make each fit better for certain use cases. For example:

* `Protobuf`:
    * **Advantages**
        * Compact serialization
        * Auto-generated [`grpc`](https://grpc.io/) client and service libraries
        * Client libraries can be used from different programming languages
    * **Disadvantages**
        * Learning curve to understand the full ecosystem
        * Not a familiar tool outside of service engineering
        * Static compilation step required to use in code
* `JTD`:
    * **Advantages**
        * Can be [learned in 5 minutes](https://jsontypedef.com/docs/jtd-in-5-minutes/)
        * Can be written inline in the programming language of choice (e.g. as a `dict` in `python`)
    * **Disadvantages**
        * No optimized serialization beyond `json`
        * No automated service implementations
        * Static [`jtd-codegen`](https://jsontypedef.com/docs/jtd-codegen/) step needed to generate native structures

This project aims to bring them together so that a given project can take advantage of the best of both:

* Define your structures in `JTD` for simplicity
* Dynamically create [`google.protobuf.Descriptor`](https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/descriptor.py#L245) objects to allow for `protobuf` serialization and deserialization
* Reverse render a `.proto` file from the generated `Descriptor` so that stubs can be generated in other languages
* No static compiliation needed!

## Usage

The usage of this library can be best understood with a simple example:

```py
import jtd_to_proto

# Declare the Foo protobuf message class
Foo = jtd_to_proto.descriptor_to_message_class(
    jtd_to_proto.jtd_to_proto(
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
    """Write out the .proto file for Foo to the given filename"""
    with open(filename, "w") as handle:
        handle.write(Foo.to_proto_file())
```

## Similar Projects

There are a number of similar projects in this space that offer slightly diferent value:

* [`jtd-codegen`](https://jsontypedef.com/docs/jtd-codegen/): This project focuses on statically generating language-native code (including `python`) to represent the JTD schema.
* [`py-json-to-proto`](https://pypi.org/project/py-json-to-proto/): This project aims to deduce a schema from an instance of a `json` object.
* [`pure-protobuf`](https://pypi.org/project/pure-protobuf/): This project has a very similar aim to `jtd-to-proto`, but it skips the intermediate `descriptor` representation and thus is not able to produce native `message.Message` classes.
