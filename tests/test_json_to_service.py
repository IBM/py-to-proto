"""
Tests for json_to_service functions
"""

# Standard
from concurrent import futures
import types

# Third Party
import grpc
import pytest

# Local
from jtd_to_proto import descriptor_to_message_class
from jtd_to_proto.json_to_service import (
    json_to_service,
    service_descriptor_to_client_stub,
    service_descriptor_to_server_registration_function,
    service_descriptor_to_service,
)
from jtd_to_proto.jtd_to_proto import jtd_to_proto

## Helpers #####################################################################


@pytest.fixture
def foo_message(temp_dpool):
    """Message fixture"""
    # google.protobuf.message.Message
    return descriptor_to_message_class(
        jtd_to_proto(
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
    )


@pytest.fixture
def foo_service_descriptor(temp_dpool, foo_message):
    """Service descriptor fixture"""
    # foo_message needs to have been defined for these input/output message to be valid
    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",
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


def test_json_to_service_descriptor(temp_dpool, foo_message):
    """Ensure that json can be converted to service descriptor"""

    # Note: This is a repeat of foo_service_descriptor fixture but
    # parts of that could change
    service_json = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input_type": "foo.bar.Foo",
                    "output_type": "foo.bar.Foo",
                }
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
    assert len(service_descriptor.methods) == 1


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


def test_end_to_end_integration(foo_message, foo_service_descriptor):
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
            return foo_message(foo=True, bar=42.0)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    registration_fn(Servicer(), server)
    server.add_insecure_port("[::]:9001")
    server.start()

    # Create the client-side connection
    chan = grpc.insecure_channel("localhost:9001")
    my_stub = stub_class(chan)
    input = foo_message(foo=False, bar=-9000)

    # Make a gRPC call
    response = my_stub.FooPredict(request=input)
    assert response.foo
    assert response.bar == 42.0

    server.stop(grace=0)
