# Standard
from typing import Callable, Dict, List, Optional, Type, Union
import dataclasses
import inspect
import types

# Third Party
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message, service
from google.protobuf.descriptor import ServiceDescriptor
from google.protobuf.message import Message
from google.protobuf.service import Service
from google.protobuf.service_reflection import GeneratedServiceType
import grpc
import jtd

# First Party
import alog

# Local
from jtd_to_proto import descriptor_to_message_class

log = alog.use_channel("JTD2S")


def jtd_to_service(
    name: str,
    package: str,
    jtd_def: Dict[str, Union[dict, str]],
    *,
    validate_jtd: bool = False,
    descriptor_pool: Optional[_descriptor_pool.DescriptorPool] = None,
) -> _descriptor.ServiceDescriptor:
    """Convert a JTD schema into a set of proto DESCRIPTOR objects.
    Operates on service definitions.

    Reference: https://jsontypedef.com/docs/jtd-in-5-minutes/

    Args:
        name:  str
            The name for the top-level service object
        package:  str
            The proto package name to use for this service
        jtd_def:  Dict[str, Union[dict, str]]
            The full JTD schema dict

    Kwargs:
        validate_jtd:  bool
            Whether or not to validate the JTD schema
        descriptor_pool:  Optional[descriptor_pool.DescriptorPool]
            If given, this DescriptorPool will be used to aggregate the set of
            message descriptors

    Returns:
        descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor corresponding to this jtd definition
    """
    # If performing validation, attempt to parse schema with jtd and throw away
    # the results
    if validate_jtd:
        log.debug2("Validating JTD")
        jtd.schema.Schema.from_dict(jtd_def)

    # Make sure we have the correct things...
    if "service" not in jtd_def.keys():
        raise ValueError("Top level `service` key required in jtd_to_service spec")
    if "rpcs" not in jtd_def["service"]:
        raise ValueError("Missing `rpcs` key required in jtd_def.service")

    method_descriptor_protos: List[descriptor_pb2.MethodDescriptorProto] = []
    imports: List[str] = []

    rpc_list = jtd_def["service"]["rpcs"]
    for rpc_def in rpc_list:
        if "input" not in rpc_def:
            raise ValueError("Missing required key `input` in rpc definition")
        input_message: Message = rpc_def["input"]
        if not (inspect.isclass(input_message) and issubclass(input_message, Message)):
            raise TypeError(
                f"Expected `input` to be type google.protobuf.message.Message but got type {type(input_message)}"
            )

        if "output" not in rpc_def:
            raise ValueError("Missing required key `output` in rpc definition")
        output_message: Message = rpc_def["output"]
        if not (
            inspect.isclass(output_message) and issubclass(output_message, Message)
        ):
            raise TypeError(
                f"Expected `output` to be type google.protobuf.message.Message but got type {type(output_message)}"
            )

        if "name" not in rpc_def:
            raise ValueError("Missing required key `name` in rpc definition")

        method_descriptor_protos.append(
            descriptor_pb2.MethodDescriptorProto(
                name=rpc_def["name"],
                input_type=input_message.DESCRIPTOR.full_name,
                output_type=output_message.DESCRIPTOR.full_name,
            )
        )
        imports.append(input_message.DESCRIPTOR.file.name)
        imports.append(output_message.DESCRIPTOR.file.name)

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
    if descriptor_pool is None:
        log.debug2("Using default descriptor pool")
        descriptor_pool = _descriptor_pool.Default()
    descriptor_pool.Add(fd_proto)

    # Return the descriptor for the top-level message
    fullname = name if not package else ".".join([package, name])

    return descriptor_pool.FindServiceByName(fullname)


def service_descriptor_to_service(
    service_descriptor: _descriptor.ServiceDescriptor,
) -> Type[GeneratedServiceType]:
    """Create a service class from a service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for

    Returns:
        Type[GeneratedServiceType]
            A new class with metaclass GeneratedServiceType containing the methods
            from the service_descriptor
    """

    return types.new_class(
        service_descriptor.name,
        (service.Service,),
        {"metaclass": GeneratedServiceType},
        lambda ns: ns.update({"DESCRIPTOR": service_descriptor}),
    )


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
        """Initializes a client stub with the service descriptor name"""
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
) -> Callable[[Type[Service], grpc.Server], None]:
    """Generates a server registration function from the service descriptor

    Args:
        service_descriptor:  google.protobuf.descriptor.ServiceDescriptor
            The ServiceDescriptor to generate a service interface for

    Returns:
        function:  Server registration function to add service handlers to a server
    """
    methods = _get_rpc_methods(service_descriptor)

    def registration_function(servicer: Type[Service], server: grpc.Server):
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
        output_descriptor: _descriptor.Descriptor = method.input_type

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
