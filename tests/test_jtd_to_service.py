"""
Tests for descriptor_to_service
"""

# Local
import grpc

from .helpers import temp_dpool
from jtd_to_proto.jtd_to_proto import (
    jtd_to_proto,
    jtd_to_service,
    service_descriptor_to_service, service_descriptor_to_client_stub,
)


def test_jtd_to_service_descriptor(temp_dpool):
    """Ensure that JTD can be converted to service descriptor"""
    message_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )  # _descriptor.Descriptor

    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": message_descriptor,
                    "output": message_descriptor,
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


def test_service_descriptor_to_service(temp_dpool):
    """Ensure that service class can be created from service descriptor"""

    # TODO: avoid repeat from previous test
    message_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )  # _descriptor.Descriptor

    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": message_descriptor,
                    "output": message_descriptor,
                }
            ]
        }
    }

    service_descriptor = jtd_to_service(
        package="foo.bar", name="FooService", jtd_def=jtd, descriptor_pool=temp_dpool
    )

    ServiceClass = service_descriptor_to_service(service_descriptor)

    assert hasattr(ServiceClass, "FooPredict")
    assert ServiceClass.__name__ == service_descriptor.name

def test_service_descriptor_to_client_stub(temp_dpool):
    """Ensure that service class can be created from service descriptor"""

    # TODO: avoid repeat from previous test
    message_descriptor = jtd_to_proto(
        "Foo",
        "foo.bar",
        {
            "properties": {
                "foo": {"type": "boolean"},
                "bar": {"type": "float32"},
            }
        },
        descriptor_pool=temp_dpool,
    )  # _descriptor.Descriptor

    jtd = {
        "service": {
            "rpcs": [
                {
                    "name": "FooPredict",
                    "input": message_descriptor,
                    "output": message_descriptor,
                }
            ]
        }
    }

    service_descriptor = jtd_to_service(
        package="foo.bar", name="FooService", jtd_def=jtd, descriptor_pool=temp_dpool
    )

    stub_class = service_descriptor_to_client_stub(service_descriptor)
    assert hasattr(stub_class(grpc.insecure_channel("localhost:9000")), "FooPredict")
    assert stub_class.__name__ == "FooServiceStub"
