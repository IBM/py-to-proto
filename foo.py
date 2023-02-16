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

descriptor = jtd_to_proto.jtd_to_proto(
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
print(descriptor)
print(type(descriptor))
print(dir(descriptor))

message = jtd_to_proto.descriptor_to_message_class(descriptor)

print("\n\n~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
print("MESSAGE")
print(message)
print(type(message))
print(dir(message))
print(message.to_proto_file())

print("\n\n~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
print("REFLECTION!")
message_class = reflection.message_factory.MessageFactory().GetPrototype(descriptor)
print(message_class)
print(type(message_class))


print("\n\n~~~~~~~~~~~~~~~~~~~~~~~~\n\n")
print("SERVICE! ????")
method_descriptor_proto = descriptor_pb2.MethodDescriptorProto(
    name="FooPredict", input_type="foo.bar.Foo", output_type="foo.bar.Foo"
)
service_proto_descriptor = descriptor_pb2.ServiceDescriptorProto(
    name="FooService", method=[method_descriptor_proto]
)


# service_message_class = reflection.message_factory.MessageFactory().GetPrototype(
#             s
#         )
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

# descriptor_pool = _descriptor_pool.Default()
# descriptor_pool.Add(fd_proto)
pool.Add(fd_proto)

# descriptor_pool.FindMessageTypeByName("FooService")
some_service_desc = pool.FindServiceByName("foo.bar.FooService")
print("some_service_desc", some_service_desc)
print(type(some_service_desc))


# jtd_to_proto.descriptor_to_message_class(some_service_desc)
# reflection.message_factory.MessageFactory().GetPrototype()

# google.protobuf.service_reflection.GeneratedServiceType

# google.protobuf.service_reflection._ServiceBuilder

# print(service_message_class)
# print(type(service_message_class))


class FooService(
    service.Service, metaclass=google.protobuf.service_reflection.GeneratedServiceType
):
    DESCRIPTOR = some_service_desc


class FooImpl(FooService):
    def FooPredict(self, request, context):
        return message(foo=True)


myservice_instance = FooService()

print(myservice_instance)
print(dir(myservice_instance))

# class FooStub(service.Service, metaclass= google.protobuf.service_reflection.GeneratedServiceStubType):
#     DESCRIPTOR = some_service_desc


class FooStub:
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.FooPredict = channel.unary_unary(
            "/foo.bar.FooService/FooPredict",
            request_serializer=message.SerializeToString,
            response_deserializer=message.FromString,
        )


server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))


def add_FooServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "FooPredict": grpc.unary_unary_rpc_method_handler(
            servicer.FooPredict,
            request_deserializer=message.FromString,
            response_serializer=message.SerializeToString,
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

input = message(foo=False)
# help(my_stub.FooPredict)

response = my_stub.FooPredict(request=input)

print(response)
print("yas")


# pip3 install nlp_runtime_client
#
# nlp_runtime_client.render_protos(my_protos_path)
#
# nlp_runtime_client.Sentimentr
#
