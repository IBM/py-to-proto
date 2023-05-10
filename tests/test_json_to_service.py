"""
Tests for json_to_service functions
"""
import time
from typing import Iterator

from caikit.core.data_model.streams.data_stream import DataStream

# Standard
from concurrent import futures
import os
import types

# Third Party
import grpc
import pytest

# Local
from py_to_proto import descriptor_to_message_class
from py_to_proto.json_to_service import (
    json_to_service,
    service_descriptor_to_client_stub,
    service_descriptor_to_server_registration_function,
    service_descriptor_to_service,
)
from py_to_proto.jtd_to_proto import jtd_to_proto

## Helpers #####################################################################


@pytest.fixture
def foo_message(temp_dpool):
    """Message foo fixture"""
    # google.protobuf.message.Message
    return descriptor_to_message_class(
        jtd_to_proto(
            "Foo",
            "foo.bar",
            {
                "properties": {
                    "foo": {"type": "boolean"},
                },
                "optionalProperties": {
                    "bar": {"type": "float32"},
                },
            },
            descriptor_pool=temp_dpool,
        )
    )


@pytest.fixture
def bar_message(temp_dpool):
    """Message bar fixture"""
    # google.protobuf.message.Message
    return descriptor_to_message_class(
        jtd_to_proto(
            "Bar",
            "foo.bar",
            {
                "properties": {
                    "boo": {"type": "int32"},
                    "baz": {"type": "boolean"},
                }
            },
            descriptor_pool=temp_dpool,
        )
    )


@pytest.fixture
def foo_service_descriptor(temp_dpool, foo_message, bar_message):
    """Service descriptor fixture"""
    # foo_message needs to have been defined for these input/output message to be valid
    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Bar",
                }
            ]
        }
    }
    return json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=service_json,
        descriptor_pool=temp_dpool,
    )


## Tests #######################################################################


def test_json_to_service_descriptor(temp_dpool, foo_message, bar_message):
    """Ensure that json can be converted to service descriptor"""

    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooTrain",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Bar",
                },
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",
                },
            ]
        }
    }
    # _descriptor.ServiceDescriptor
    service_descriptor = json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=service_json,
        descriptor_pool=temp_dpool,
    )
    # Validate message naming
    assert service_descriptor.name == "FooService"
    assert len(service_descriptor.methods) == 2


def test_duplicate_services_are_okay(temp_dpool, foo_message, bar_message):
    """Ensure that json can be converted to service descriptor multiple times"""

    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooTrain",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Bar",
                },
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",
                },
            ]
        }
    }
    # _descriptor.ServiceDescriptor
    service_descriptor = json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=service_json,
        descriptor_pool=temp_dpool,
    )

    another_service_descriptor = json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=service_json,
        descriptor_pool=temp_dpool,
    )
    assert service_descriptor == another_service_descriptor


ORIGINAL_SERVICE = {
    "service": {
        "rpcs": [
            {
                "name": "FooTrain",
                "input_type": "foo.bar.Foo",
                "output_type": "foo.bar.Bar",
            }
        ]
    }
}
INVALID_DUPLICATE_SERVICES = [
    {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",  # Different method name
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",
                }
            ]
        }
    },
    {
        "service": {
            "rpcs": [
                {
                    "name": "FooTrain",
                    "input_type": "foo.bar.Bar",  # Different input
                    "output_type": "foo.bar.Bar",
                }
            ]
        }
    },
    {
        "service": {
            "rpcs": [
                {
                    "name": "FooTrain",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",  # Different output
                }
            ]
        }
    },
]


@pytest.mark.parametrize("schema", INVALID_DUPLICATE_SERVICES)
def test_multiple_services_with_the_same_name_are_not_okay(
    schema, temp_dpool, foo_message, bar_message
):
    """Ensure that json can be converted to service descriptor"""

    json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=ORIGINAL_SERVICE,
        descriptor_pool=temp_dpool,
    )

    with pytest.raises(TypeError):
        json_to_service(
            package="foo.bar",
            name="FooService",
            json_service_def=schema,
            descriptor_pool=temp_dpool,
        )


def test_json_to_service_input_validation(temp_dpool, foo_message):
    """Make sure that an error is raised if the service definition is invalid"""
    # This def is missing the `input_type` field
    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "output_type": "foo.bar.Foo",
                }
            ]
        }
    }
    with pytest.raises(ValueError) as excinfo:
        json_to_service(
            package="foo.bar",
            name="FooService",
            json_service_def=service_json,
            descriptor_pool=temp_dpool,
        )
    assert "Invalid service json" in str(excinfo.value)


def test_service_descriptor_to_service(foo_service_descriptor):
    """Ensure that service class can be created from service descriptor"""
    ServiceClass = service_descriptor_to_service(foo_service_descriptor)

    assert hasattr(ServiceClass, "FooPredict")
    assert ServiceClass.__name__ == foo_service_descriptor.name


def test_services_can_be_written_to_protobuf_files(foo_service_descriptor, tmp_path):
    """Ensure that service class can be created from service descriptor"""
    ServiceClass = service_descriptor_to_service(foo_service_descriptor)

    assert hasattr(ServiceClass, "to_proto_file")
    assert hasattr(ServiceClass, "write_proto_file")

    tempdir = str(tmp_path)
    ServiceClass.write_proto_file(tempdir)
    assert "fooservice.proto" in os.listdir(tempdir)
    with open(os.path.join(tempdir, "fooservice.proto"), "r") as f:
        assert "service FooService {" in f.read()


def test_service_descriptor_to_client_stub(foo_service_descriptor):
    """Ensure that client stub can be created from service descriptor"""

    stub_class = service_descriptor_to_client_stub(foo_service_descriptor)
    assert hasattr(stub_class(grpc.insecure_channel("localhost:9000")), "FooPredict")
    assert stub_class.__name__ == "FooServiceStub"


def test_service_descriptor_to_registration_function(foo_service_descriptor):
    """Ensure that server registration function can be created from service descriptor"""

    registration_fn = service_descriptor_to_server_registration_function(
        foo_service_descriptor
    )
    assert isinstance(registration_fn, types.FunctionType)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    service_class = service_descriptor_to_service(foo_service_descriptor)

    registration_fn(service_class(), server)

    # GORP
    assert (
        "/foo.bar.FooService/FooPredict"
        in server._state.generic_handlers[0]._method_handlers
    )


def test_end_to_end_unary_unary_integration(foo_message, bar_message, foo_service_descriptor):
    """Test a full grpc service integration"""
    registration_fn = service_descriptor_to_server_registration_function(
        foo_service_descriptor
    )
    service_class = service_descriptor_to_service(foo_service_descriptor)
    stub_class = service_descriptor_to_client_stub(foo_service_descriptor)

    # Define and start a gRPC service
    class Servicer(service_class):
        """gRPC Service Impl"""

        def FooPredict(self, request, context):
            # Test that the `optionalProperty` "bar" of the request can be checked for existence
            if request.foo:
                assert request.HasField("bar")
            else:
                assert not request.HasField("bar")
            return bar_message(boo=42, baz=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    registration_fn(Servicer(), server)
    server.add_insecure_port("[::]:9001")
    server.start()

    # Create the client-side connection
    chan = grpc.insecure_channel("localhost:9001")
    my_stub = stub_class(chan)
    # nb: we'll set "foo" to the existence of "bar" to put asserts in the request handler
    input = foo_message(foo=True, bar=-9000)

    # Make a gRPC call
    response = my_stub.FooPredict(request=input)
    assert isinstance(response, bar_message)
    assert response.boo == 42
    assert response.baz

    # Test that we can not set `bar` and correctly check that it was not set on the server side
    input = foo_message(foo=False)
    response = my_stub.FooPredict(request=input)
    assert isinstance(response, bar_message)

    server.stop(grace=0)


def test_end_to_end_unary_streaming_integration(foo_message, bar_message, temp_dpool):
    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Bar",
                    "output_streaming": True,
                }
            ]
        }
    }
    service_descriptor = json_to_service(
        package="foo.bar",
        name="FooService",
        json_service_def=service_json,
        descriptor_pool=temp_dpool,
    )

    registration_fn = service_descriptor_to_server_registration_function(
        service_descriptor
    )
    service_class = service_descriptor_to_service(service_descriptor)
    stub_class = service_descriptor_to_client_stub(service_descriptor)

    class Servicer(service_class):
        """gRPC Service Impl"""

        def to_proto(self, i: int) -> bar_message:
            return bar_message(boo=i, baz=True)

        def some_generator(self, n) -> Iterator[int]:
            for i in range(n):
                yield i
                time.sleep(0.01)

        def some_model_run(self) -> DataStream[int]:
            my_output_stream = DataStream(self.some_generator, 100)
            return my_output_stream

        def FooPredict(self, request, context):
            # Test that the `optionalProperty` "bar" of the request can be checked for existence
            if request.foo:
                assert request.HasField("bar")
            else:
                assert not request.HasField("bar")

            datastream = self.some_model_run()

            return iter(datastream.map(self.to_proto))
            # for thing in datastream:
            #     yield self.to_proto(thing)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    registration_fn(Servicer(), server)
    server.add_insecure_port("[::]:9001")
    server.start()

    # Create the client-side connection
    chan = grpc.insecure_channel("localhost:9001")
    my_stub = stub_class(chan)
    # nb: we'll set "foo" to the existence of "bar" to put asserts in the request handler
    input = foo_message(foo=True, bar=-9000)

    # Make a gRPC call
    i = 0
    for bar in my_stub.FooPredict(request=input):
        assert bar.boo == i
        i += 1
    assert i == 100

    server.stop(grace=0)
