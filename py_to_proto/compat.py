"""
Compatibility module for API changes between different versions of protobuf
"""

# Standard
from typing import Type
import types

# Third Party
from google.protobuf.descriptor import ServiceDescriptor


# protobuf >= 6
try:  # pragma: no cover
    from google.protobuf.service_reflection import GeneratedServiceType

    def make_service_class(service_descriptor: ServiceDescriptor) -> Type[GeneratedServiceType]:
        return GeneratedServiceType(
            service_descriptor.name,
            (),
            {"DESCRIPTOR": service_descriptor},
        )

# protobuf < 6
except ImportError:  # pragma: no cover
    from google.protobuf.service import Service as GeneratedServiceType

    def make_service_class(service_descriptor: ServiceDescriptor) -> Type[GeneratedServiceType]:
        return types.new_class(
            service_descriptor.name,
            (GeneratedServiceType,),
            {"metaclass": GeneratedServiceType},
            lambda ns: ns.update({"DESCRIPTOR": service_descriptor}),
        )
