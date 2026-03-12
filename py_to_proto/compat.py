"""
Compatibility module for API changes between different versions of protobuf
"""

# Standard
from typing import Type
import types

# Third Party
from google.protobuf.descriptor import FieldDescriptor, ServiceDescriptor

# protobuf >= 6
try:  # pragma: no cover
    # Third Party
    from google.protobuf.service_reflection import GeneratedServiceType

    def make_service_class(
        service_descriptor: ServiceDescriptor,
    ) -> Type[GeneratedServiceType]:
        return GeneratedServiceType(
            service_descriptor.name,
            (),
            {"DESCRIPTOR": service_descriptor},
        )


# protobuf < 6
except ImportError:  # pragma: no cover
    # Third Party
    from google.protobuf.service import Service as GeneratedServiceType

    def make_service_class(
        service_descriptor: ServiceDescriptor,
    ) -> Type[GeneratedServiceType]:
        return types.new_class(
            service_descriptor.name,
            (GeneratedServiceType,),
            {"metaclass": GeneratedServiceType},
            lambda ns: ns.update({"DESCRIPTOR": service_descriptor}),
        )


# protobuf >= 6: label property is deprecated in favor of is_repeated/is_required
if hasattr(FieldDescriptor, "is_repeated"):  # pragma: no cover

    def is_field_repeated(field: FieldDescriptor) -> bool:
        return field.is_repeated

    def is_field_optional(field: FieldDescriptor) -> bool:
        return not field.is_required and not field.is_repeated

else:  # pragma: no cover

    def is_field_repeated(field: FieldDescriptor) -> bool:
        return field.label == FieldDescriptor.LABEL_REPEATED

    def is_field_optional(field: FieldDescriptor) -> bool:
        return field.label == FieldDescriptor.LABEL_OPTIONAL
