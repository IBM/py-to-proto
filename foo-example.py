from concurrent import futures
import google.protobuf.service_reflection
from google.protobuf import reflection, service
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
import grpc

import alog

alog.configure(default_level="debug4")

import jtd_to_proto

pool = _descriptor_pool.DescriptorPool()

message_descriptor = jtd_to_proto.jtd_to_proto(
    "Foo",
    "foo.bar",
    {
        "properties": {
            "foo": {
                "type": "boolean",
            },
        }
    },
    descriptor_pool=pool,
)

print("DESC")
print(message_descriptor)
print(type(message_descriptor))

message_class = jtd_to_proto.descriptor_to_message_class(message_descriptor)

print("\n\n~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
print("MESSAGE")
print(message_class)
print(type(message_class))
print(message_class.to_proto_file())


print("\n\n~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
print("SERVICE!")
method_descriptor_proto = descriptor_pb2.MethodDescriptorProto(
    name="FooPredict", input_type="foo.bar.Foo", output_type="foo.bar.Foo"
)
service_proto_descriptor = descriptor_pb2.ServiceDescriptorProto(
    name="FooService", method=[method_descriptor_proto]
)


print("service_proto_descriptor: ", service_proto_descriptor)
print("service_proto_descriptor type: ", type(service_proto_descriptor))

fd_proto = descriptor_pb2.FileDescriptorProto(
    name="fooservice.proto",
    package="foo.bar",
    syntax="proto3",
    dependency=["foo.proto"],
    # **proto_kwargs,
    service=[service_proto_descriptor],
)

print("fd_proto:", fd_proto)
print(type(fd_proto))

pool.Add(fd_proto)

some_service_desc = pool.FindServiceByName("foo.bar.FooService")
print("some_service_desc", some_service_desc)
print(type(some_service_desc))


# Thing that needs to be generated
class FooService(
    service.Service, metaclass=google.protobuf.service_reflection.GeneratedServiceType
):
    DESCRIPTOR = some_service_desc


# Impl for demo
class FooImpl(FooService):
    def FooPredict(self, request, context):
        return message_class(foo=True)


myservice_instance = FooService()

print(myservice_instance)
print(dir(myservice_instance))

# Thing that needs to be generated
class FooStub:
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.FooPredict = channel.unary_unary(
            "/foo.bar.FooService/FooPredict",
            request_serializer=message_class.SerializeToString,
            response_deserializer=message_class.FromString,
        )


server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))

# Thing that needs to be generated
def add_FooServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "FooPredict": grpc.unary_unary_rpc_method_handler(
            servicer.FooPredict,
            request_deserializer=message_class.FromString,
            response_serializer=message_class.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "foo.bar.FooService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


add_FooServiceServicer_to_server(FooImpl(), server)

server.add_insecure_port("[::]:9001")


server.start()

chan = grpc.insecure_channel("localhost:9001")

my_stub = FooStub(chan)
print(dir(my_stub))

input = message_class(foo=False)
# help(my_stub.FooPredict)

response = my_stub.FooPredict(request=input)

print(response)
print("yas")
