"""
Tests for jtd_to_service functions
"""

# Standard
import types
from concurrent import futures

# Third Party
import grpc
import pytest

# Local
from .helpers import temp_dpool
from jtd_to_proto import descriptor_to_message_class
from jtd_to_proto.jtd_to_proto import (
    jtd_to_proto,
)
from jtd_to_proto.jtd_to_service import (
    jtd_to_service,
    service_descriptor_to_service,
    service_descriptor_to_client_stub,
    service_descriptor_to_server_registration_function,
)

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
    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": foo_message,
                    "output": foo_message,
                }
            ]
        }
    }
    return jtd_to_service(
        package="foo.bar", name="FooService", jtd_def=jtd, descriptor_pool=temp_dpool
    )


## Tests #######################################################################


def test_jtd_to_service_descriptor(temp_dpool, foo_message):
    """Ensure that JTD can be converted to service descriptor"""

    # Note: This is a repeat of foo_service_descriptor fixture but
    # parts of that could change
    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": foo_message,
                    "output": foo_message,
                }
            ]
        }
    }
    # _descriptor.ServiceDescriptor
    service_descriptor = jtd_to_service(
        package="foo.bar", name="FooService", jtd_def=jtd, descriptor_pool=temp_dpool
    )
    # Validate message naming
    assert service_descriptor.name == "FooService"
    assert len(service_descriptor.methods) == 1


def test_jtd_to_service_descriptor_no_service(temp_dpool):
    """Make sure that an error is raised if top-level `service` key missing"""
    jtd = {
        "rpcs": [
            {
                "name": "FooPredict",
                "input": foo_message,
                "output": foo_message,
            }
        ]
    }
    with pytest.raises(ValueError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_descriptor_no_rpcs(temp_dpool):
    """Make sure that an error is raised if `rpcs` key missing"""
    jtd = {
        "service": {
            "name": "FooPredict",
            "input": foo_message,
            "output": foo_message,
        }
    }
    with pytest.raises(ValueError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_descriptor_no_rpc_name(temp_dpool, foo_message):
    """Make sure that an error is raised if rpc does not have a name"""
    jtd = {
        "service": {
            "rpcs": [
                {
                    "input": foo_message,
                    "output": foo_message,
                }
            ]
        }
    }
    with pytest.raises(ValueError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_no_input(temp_dpool, foo_message):
    """Make sure that an error is raised if rpc does not have a name"""
    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "output": foo_message,
                }
            ]
        }
    }
    with pytest.raises(ValueError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_no_output(temp_dpool, foo_message):
    """Make sure that an error is raised if rpc does not have a name"""
    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": foo_message,
                }
            ]
        }
    }
    with pytest.raises(ValueError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_wrong_input_type(temp_dpool, foo_message):
    """Make sure that an error is raised if rpc does not have a name"""
    jtd = {
        "service": {
            "rpcs": [{"name": "FooPredict", "input": "foo", "output": foo_message}]
        }
    }
    with pytest.raises(TypeError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


def test_jtd_to_service_wrong_output_type(temp_dpool, foo_message):
    """Make sure that an error is raised if rpc does not have a name"""
    jtd = {
        "service": {
            "rpcs": [{"name": "FooPredict", "input": foo_message, "output": "foo"}]
        }
    }
    with pytest.raises(TypeError):
        jtd_to_service(
            package="foo.bar",
            name="FooService",
            jtd_def=jtd,
            descriptor_pool=temp_dpool,
        )


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
    assert type(registration_fn) == types.FunctionType

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


def test_jtd_to_service_validation():
    """Check that we can use the validate_jtd flag"""
    with pytest.raises(AttributeError):
        jtd_to_service("Foo", "foo.bar", {"foo": "bar"}, validate_jtd=True)
