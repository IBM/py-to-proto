# Standard
from typing import Callable, Dict, List, Optional, Type
import dataclasses
import types

# Third Party
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import service
from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor
from google.protobuf.service import Service
from google.protobuf.service_reflection import GeneratedServiceType
import grpc

# First Party
import alog

# Local
from .descriptor_to_message_class import (
    _add_protobuf_serializers,
    descriptor_to_message_class,
)
from .utils import safe_add_fd_to_pool
from .validation import JTD_TYPE_VALIDATORS, validate_jtd

log = alog.use_channel("JSON2S")

SERVICE_JTD_SCHEMA = {
    "properties": {
        "service": {
            "properties": {
                "rpcs": {
                    "elements": {
                        "properties": {
                            "input_type": {"type": "string"},
                            "name": {"type": "string"},
                            "output_type": {"type": "string"},
                        },
                        "optionalProperties": {
                            "server_streaming": {"type": "boolean"},
                            "client_streaming": {"type": "boolean"},
                        },
                    }
                }
            }
        }
    }
}

EXTENDED_TYPE_VALIDATORS = dict(
    bytes=lambda x: isinstance(x, bytes), **JTD_TYPE_VALIDATORS
)

# Python type hint equivalent of jtd service schema
ServiceJsonType = Dict[str, Dict[str, List[Dict[str, str]]]]


@dataclasses.dataclass
class GRPCService:
    descriptor: ServiceDescriptor
    registration_function: Callable[[Service, grpc.Server], None]
    client_stub_class: Type
    service_class: Type[service.Service]


def json_to_service(
    name: str,
    package: str,
    json_service_def: ServiceJsonType,
    *,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> GRPCService:
    """Convert a JSON representation of an RPC service into a GRPCService.

    Reference: https://jsontypedef.com/docs/jtd-in-5-minutes/

    Args:
        name:  str
            The name for the top-level service object
        package:  str
            The proto package name to use for this service
        json_service_def:  Dict[str, Union[dict, str]]
            A JSON dict describing a service that matches the SERVICE_JTD_SCHEMA

    Kwargs:
        descriptor_pool:  Optional[descriptor_pool.DescriptorPool]
            If given, this DescriptorPool will be used to aggregate the set of
            message descriptors

    Returns:
        grpc_service:  GRPCService
            The GRPCService container with the service descriptor and other associated
            grpc bits required to boot a server:
            - Servicer registration function
            - Client stub class
            - Servicer base class
    """
    # Ensure we have a valid service spec
    log.debug2("Validating service json")
    if not validate_jtd(json_service_def, SERVICE_JTD_SCHEMA, EXTENDED_TYPE_VALIDATORS):
        raise ValueError("Invalid service json")

    # And descriptor pool
    if descriptor_pool is None:
        log.debug2("Using the default descriptor pool")
        descriptor_pool = _descriptor_pool.Default()

    # First get the descriptor proto:
    service_fd_proto = _json_to_service_file_descriptor_proto(
        name, package, json_service_def, descriptor_pool=descriptor_pool
    )
    assert (
        len(service_fd_proto.service) == 1
    ), f"File Descriptor {service_fd_proto.name} should only have one service"
    service_descriptor_proto = service_fd_proto.service[0]

    # Then put that in the pool to get the real descriptor back
    log.debug("Adding Descriptors to DescriptorPool")
    safe_add_fd_to_pool(service_fd_proto, descriptor_pool)
    service_fullname = name if not package else ".".join([package, name])
    service_descriptor = descriptor_pool.FindServiceByName(service_fullname)

    # Then the client stub:
    client_stub = _service_descriptor_to_client_stub(
        service_descriptor, service_descriptor_proto
    )

    # And the registration function:
    registration_function = _service_descriptor_to_server_registration_function(
        service_descriptor, service_descriptor_proto
    )

    # And service class!
    service_class = _service_descriptor_to_service(service_descriptor)

    return GRPCService(
        descriptor=service_descriptor,
        service_class=service_class,
        client_stub_class=client_stub,
        registration_function=registration_function,
    )


def _json_to_service_file_descriptor_proto(
    name: str,
    package: str,
    json_service_def: ServiceJsonType,
    *,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> descriptor_pb2.FileDescriptorProto:
    """Creates the FileDescriptorProto for the service definition"""

    method_descriptor_protos: List[descriptor_pb2.MethodDescriptorProto] = []
    imports: List[str] = []

    json_service = json_service_def["service"]
    rpcs_def = json_service["rpcs"]
    for rpc_def in rpcs_def:
        rpc_input_type = rpc_def["input_type"]
        input_descriptor = descriptor_pool.FindMessageTypeByName(rpc_input_type)

        rpc_output_type = rpc_def["output_type"]
        output_descriptor = descriptor_pool.FindMessageTypeByName(rpc_output_type)

        method_descriptor_protos.append(
            descriptor_pb2.MethodDescriptorProto(
                name=rpc_def["name"],
                input_type=input_descriptor.full_name,
                output_type=output_descriptor.full_name,
                client_streaming=rpc_def.get("client_streaming", False),
                server_streaming=rpc_def.get("server_streaming", False),
            )
        )
        imports.append(input_descriptor.file.name)
        imports.append(output_descriptor.file.name)

    imports = sorted(list(set(imports)))

    service_descriptor_proto = descriptor_pb2.ServiceDescriptorProto(
        name=name, method=method_descriptor_protos
    )

    fd_proto = descriptor_pb2.FileDescriptorProto(
        name=f"{name.lower()}.proto",
        package=package,
        syntax="proto3",
        dependency=imports,
        # **proto_kwargs,
        service=[service_descriptor_proto],
    )

    return fd_proto


def _service_descriptor_to_service(
    service_descriptor: ServiceDescriptor,
) -> Type[service.Service]:
    """Create a service class from a service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for

    Returns:
        Type[google.protobuf.service.Service]
            A new class with metaclass google.protobuf.service_reflection.GeneratedServiceType
            containing the methods from the service_descriptor
    """
    service_class = types.new_class(
        service_descriptor.name,
        (service.Service,),
        {"metaclass": GeneratedServiceType},
        lambda ns: ns.update({"DESCRIPTOR": service_descriptor}),
    )
    service_class = _add_protobuf_serializers(service_class, service_descriptor)

    return service_class


def _service_descriptor_to_client_stub(
    service_descriptor: ServiceDescriptor,
    service_descriptor_proto: descriptor_pb2.ServiceDescriptorProto,
) -> Type:
    """Generates a new client stub class from the service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for
        service_descriptor_proto:  google.protobuf.descriptor_pb2.ServiceDescriptorProto
            The descriptor proto for that service. This holds the I/O streaming information
            for each method
    """
    _assert_method_lists_same(service_descriptor, service_descriptor_proto)

    def _get_channel_func(
        channel: grpc.Channel, method: descriptor_pb2.MethodDescriptorProto
    ) -> Callable:
        if method.client_streaming and method.server_streaming:
            return channel.stream_stream
        if not method.client_streaming and method.server_streaming:
            return channel.unary_stream
        if method.client_streaming and not method.server_streaming:
            return channel.stream_unary
        return channel.unary_unary

    # Initializer
    def initializer(self, channel: grpc.Channel):
        f"""Initializes a client stub with for the {service_descriptor.name} Service"""
        for method, method_proto in zip(
            service_descriptor.methods, service_descriptor_proto.method
        ):
            setattr(
                self,
                method.name,
                _get_channel_func(channel, method_proto)(
                    _get_method_fullname(method),
                    request_serializer=descriptor_to_message_class(
                        method.input_type
                    ).SerializeToString,
                    response_deserializer=descriptor_to_message_class(
                        method.output_type
                    ).FromString,
                ),
            )

    # Creating class dynamically
    return type(
        f"{service_descriptor.name}Stub",
        (object,),
        {
            "__init__": initializer,
        },
    )


def _service_descriptor_to_server_registration_function(
    service_descriptor: ServiceDescriptor,
    service_descriptor_proto: descriptor_pb2.ServiceDescriptorProto,
) -> Callable[[Service, grpc.Server], None]:
    """Generates a server registration function from the service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for
        service_descriptor_proto:  google.protobuf.descriptor_pb2.ServiceDescriptorProto
            The descriptor proto for that service. This holds the I/O streaming information
            for each method

    Returns:
        function:  Server registration function to add service handlers to a server
    """
    _assert_method_lists_same(service_descriptor, service_descriptor_proto)

    def _get_handler(method: descriptor_pb2.MethodDescriptorProto):
        if method.client_streaming and method.server_streaming:
            return grpc.stream_stream_rpc_method_handler
        if not method.client_streaming and method.server_streaming:
            return grpc.unary_stream_rpc_method_handler
        if method.client_streaming and not method.server_streaming:
            return grpc.stream_unary_rpc_method_handler
        return grpc.unary_unary_rpc_method_handler

    def registration_function(servicer: Service, server: grpc.Server):
        """Server registration function"""
        rpc_method_handlers = {
            method.name: _get_handler(method_proto)(
                getattr(servicer, method.name),
                request_deserializer=descriptor_to_message_class(
                    method.input_type
                ).FromString,
                response_serializer=descriptor_to_message_class(
                    method.output_type
                ).SerializeToString,
            )
            for method, method_proto in zip(
                service_descriptor.methods, service_descriptor_proto.method
            )
        }
        generic_handler = grpc.method_handlers_generic_handler(
            service_descriptor.full_name, rpc_method_handlers
        )
        server.add_generic_rpc_handlers((generic_handler,))

    return registration_function


def _get_method_fullname(method: MethodDescriptor):
    method_name_parts = method.full_name.split(".")
    return f"/{'.'.join(method_name_parts[:-1])}/{method_name_parts[-1]}"


def _assert_method_lists_same(
    service_descriptor: ServiceDescriptor,
    service_descriptor_proto: descriptor_pb2.ServiceDescriptorProto,
):
    assert len(service_descriptor.methods) == len(service_descriptor_proto.method), (
        f"Method count mismatch: {service_descriptor.full_name} has"
        f" {len(service_descriptor.methods)} methods but proto descriptor"
        f" {service_descriptor_proto.name} has {len(service_descriptor_proto.method)} methods"
    )

    for m1, m2 in zip(service_descriptor.methods, service_descriptor_proto.method):
        assert m1.name == m2.name, f"Method mismatch: {m1.name}, {m2.name}"
