# Standard
from typing import Callable, Dict, List, Optional, Type
import dataclasses
import types

# Third Party
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message, service
from google.protobuf.descriptor import ServiceDescriptor
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
from .jtd_to_proto import _safe_add_fd_to_pool
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
                        }
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


def json_to_service(
    name: str,
    package: str,
    json_service_def: ServiceJsonType,
    *,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> _descriptor.ServiceDescriptor:
    """Convert a JSON representation of an RPC service into a ServiceDescriptor.

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
        descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor corresponding to this json definition
    """
    # Ensure we have a valid service spec
    log.debug2("Validating service json")
    if not validate_jtd(json_service_def, SERVICE_JTD_SCHEMA, EXTENDED_TYPE_VALIDATORS):
        raise ValueError("Invalid service json")

    method_descriptor_protos: List[descriptor_pb2.MethodDescriptorProto] = []
    imports: List[str] = []

    if descriptor_pool is None:
        log.debug2("Using the default descriptor pool")
        descriptor_pool = _descriptor_pool.Default()

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

    # Add the FileDescriptorProto to the Descriptor Pool
    log.debug("Adding Descriptors to DescriptorPool")
    _safe_add_fd_to_pool(fd_proto, descriptor_pool)

    # Return the descriptor for the top-level message
    fullname = name if not package else ".".join([package, name])

    return descriptor_pool.FindServiceByName(fullname)


def service_descriptor_to_service(
    service_descriptor: _descriptor.ServiceDescriptor,
) -> Type[service.Service]:
    """Create a service class from a service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for

    Returns:
        Type[google.protobuf.service.Service]
            A new class with metaclass google.protobuf.service_reflection.GeneratedServiceType containing the methods
            from the service_descriptor
    """
    service_class = types.new_class(
        service_descriptor.name,
        (service.Service,),
        {"metaclass": GeneratedServiceType},
        lambda ns: ns.update({"DESCRIPTOR": service_descriptor}),
    )
    service_class = _add_protobuf_serializers(service_class, service_descriptor)

    return service_class


def service_descriptor_to_client_stub(
    service_descriptor: _descriptor.ServiceDescriptor,
) -> Type:
    """Generates a new client stub class from the service descriptor

    Args:
        service_descriptor (google.protobuf.descriptor.ServiceDescriptor):
            The ServiceDescriptor to generate a service interface for
    """
    methods = _get_rpc_methods(service_descriptor)

    # Initializer
    def initializer(self, channel):
        f"""Initializes a client stub with for the {service_descriptor.name} Service"""
        for method in methods:
            setattr(
                self,
                method.name,
                channel.unary_unary(
                    method.fullname,
                    request_serializer=method.input_message_class.SerializeToString,
                    response_deserializer=method.output_message_class.FromString,
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


def service_descriptor_to_server_registration_function(
    service_descriptor: _descriptor.ServiceDescriptor,
) -> Callable[[Service, grpc.Server], None]:
    """Generates a server registration function from the service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for

    Returns:
        function:  Server registration function to add service handlers to a server
    """
    methods = _get_rpc_methods(service_descriptor)

    def registration_function(servicer: Service, server: grpc.Server):
        """Server registration function"""
        rpc_method_handlers = {
            method.name: grpc.unary_unary_rpc_method_handler(
                getattr(servicer, method.name),
                request_deserializer=method.input_message_class.FromString,
                response_serializer=method.output_message_class.SerializeToString,
            )
            for method in methods
        }
        generic_handler = grpc.method_handlers_generic_handler(
            service_descriptor.full_name, rpc_method_handlers
        )
        server.add_generic_rpc_handlers((generic_handler,))

    return registration_function


@dataclasses.dataclass
class _RPCMethod:
    name: str
    fullname: str
    input_message_class: Type[message.Message]
    output_message_class: Type[message.Message]


def _get_rpc_methods(service_descriptor: ServiceDescriptor) -> List[_RPCMethod]:
    """Get list of RPC methods from a service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to get RPC methods for

    Returns:
        List of RPC methods
    """
    # For each method, need to know input / output message
    methods: List[_RPCMethod] = []

    for method in service_descriptor.methods:
        method: _descriptor.MethodDescriptor

        input_descriptor: _descriptor.Descriptor = method.input_type
        output_descriptor: _descriptor.Descriptor = method.output_type

        input_message_class = descriptor_to_message_class(input_descriptor)
        output_message_class = descriptor_to_message_class(output_descriptor)

        method_name_parts = method.full_name.split(".")
        method_full_name = (
            f"/{'.'.join(method_name_parts[:-1])}/{method_name_parts[-1]}"
        )
        methods.append(
            _RPCMethod(
                name=method.name,
                fullname=method_full_name,
                input_message_class=input_message_class,
                output_message_class=output_message_class,
            )
        )

    return methods
