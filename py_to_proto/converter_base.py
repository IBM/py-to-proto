"""
This base class provides the abstract interface that needs to be implemented to
convert from some schema format into a protobuf descriptor. It also implements
the common conversion scaffolding that all converters will use to create the
descriptor.
"""

# Standard
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union
import abc
import copy

# Third Party
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import struct_pb2

# First Party
import alog

# Local
from .utils import safe_add_fd_to_pool, to_upper_camel

T = TypeVar("T")


log = alog.use_channel("2PCVRT")


# Top level descriptor types
_DescriptorTypes = (_descriptor.Descriptor, _descriptor.EnumDescriptor)
_DescriptorTypesUnion = Union[_descriptor.Descriptor, _descriptor.EnumDescriptor]


class ConverterBase(Generic[T], abc.ABC):
    __doc__ = __doc__

    # Types that can be returned from _convert. This is exposed as public so
    # that derived converters can reference it
    ConvertOutputTypes = Union[
        # Concrete type
        int,
        # Message descriptor reference
        _descriptor.Descriptor,
        # Enum descriptor reference
        _descriptor.EnumDescriptor,
        # Nested message
        descriptor_pb2.DescriptorProto,
        # Nested enum
        descriptor_pb2.EnumDescriptorProto,
    ]

    def __init__(
        self,
        name: str,
        package: str,
        source_schema: T,
        type_mapping: Dict[Any, Union[int, _descriptor.Descriptor]],
        validate: bool,
        descriptor_pool: Optional[_descriptor_pool.DescriptorPool],
    ):
        """This class performs its work on initialization by invoking the
        abstract methods that the child class will implement.

        Args:
            name (str)
                The name for the top-level message or enum descriptor
            package (str)
                The proto package name to use for this object
            source_schema (T)
                The source schema object for the derived converter
            type_mapping (Dict[Any, Union[int, _descriptor.Descriptor]])
                A mapping from how types are represented in T to the protobuf
                type enum and/or Descriptor to be used to represent the type in
                proto.
            validate (bool)
                Whether or not to perform validation before attempting
                conversion
            descriptor_pool (Optional[_descriptor_pool.DescriptorPool])
                An explicit descriptor pool to use for the new descriptor
        """
        # Set up the shared members for this converter
        self.type_mapping = type_mapping
        self.package = package
        self.imports = set()

        # Perform validation if requested
        if validate:
            log.debug2("Validating")
            if not self.validate(source_schema):
                raise ValueError(f"Invalid Schema: {source_schema}")

        # Figure out which descriptor pool to use
        if descriptor_pool is None:
            log.debug2("Using default descriptor pool")
            descriptor_pool = _descriptor_pool.Default()
        self.descriptor_pool = descriptor_pool

        # Perform the recursive conversion to update the descriptors and enums in
        # place
        log.debug("Performing conversion")
        descriptor_proto = self._convert(entry=source_schema, name=name)
        proto_kwargs = {}
        is_enum = False
        if isinstance(descriptor_proto, descriptor_pb2.DescriptorProto):
            proto_kwargs["message_type"] = [descriptor_proto]
        elif isinstance(descriptor_proto, descriptor_pb2.EnumDescriptorProto):
            is_enum = True
            proto_kwargs["enum_type"] = [descriptor_proto]
        else:
            raise ValueError("Only messages and enums are supported")

        # Create the FileDescriptorProto with all messages
        log.debug("Creating FileDescriptorProto")
        fd_proto = descriptor_pb2.FileDescriptorProto(
            name=f"{name.lower()}.proto",
            package=package,
            syntax="proto3",
            dependency=sorted(list(self.imports)),
            **proto_kwargs,
        )
        log.debug4("Full FileDescriptorProto:\n%s", fd_proto)

        # Add the new file descriptor to the pool
        log.debug("Adding Descriptors to DescriptorPool")
        safe_add_fd_to_pool(fd_proto, self.descriptor_pool)

        # Return the descriptor for the top-level message
        fullname = name if not package else ".".join([package, name])
        if is_enum:
            self.descriptor = descriptor_pool.FindEnumTypeByName(fullname)
        else:
            self.descriptor = descriptor_pool.FindMessageTypeByName(fullname)

    ## Abstract Interface ######################################################

    @abc.abstractmethod
    def validate(self, source_schema: T) -> bool:
        """Perform preprocess validation of the input"""

    ## Types ##

    @abc.abstractmethod
    def get_concrete_type(self, entry: Any) -> Any:
        """If this is a concrete type, get the type map key for it"""

    ## Maps ##

    @abc.abstractmethod
    def get_map_key_val_types(
        self,
        entry: Any,
    ) -> Optional[Tuple[int, ConvertOutputTypes]]:
        """Get the key and value types for a given map type"""

    ## Enums ##

    @abc.abstractmethod
    def get_enum_vals(self, entry: Any) -> Optional[Iterable[Tuple[str, int]]]:
        """Get the ordered list of enum name -> number mappings if this entry is
        an enum

        NOTE: If any values appear multiple times, this implies an alias

        NOTE 2: All names must be unique
        """

    ## Messages ##

    @abc.abstractmethod
    def get_message_fields(self, entry: Any) -> Optional[Iterable[Tuple[str, Any]]]:
        """Get the mapping of names to type-specific field descriptors if this
        entry is a message
        """

    @abc.abstractmethod
    def has_additional_fields(self, entry: Any) -> bool:
        """Check whether the given entry expects to support arbitrary key/val
        additional properties
        """

    @abc.abstractmethod
    def get_optional_field_names(self, entry: Any) -> List[str]:
        """Get the names of any fields which are explicitly marked 'optional'"""

    ## Fields ##

    @abc.abstractmethod
    def get_field_number(self, num_fields: int, field_def: Any) -> int:
        """From the given field definition and index, get the proto field number"""

    @abc.abstractmethod
    def get_oneof_fields(self, field_def: Any) -> Optional[Iterable[Tuple[str, Any]]]:
        """If the given field is a oneof, return an iterable of the sub-field
        definitions
        """

    @abc.abstractmethod
    def get_oneof_name(self, field_def: Any) -> str:
        """For an identified oneof field def, get the name"""

    @abc.abstractmethod
    def get_field_type(self, field_def: Any) -> Any:
        """Get the type of the field. The definition of type here will be
        specific to the converter (e.g. string for JTD, py type for dataclass)
        """

    @abc.abstractmethod
    def is_repeated_field(self, field_def: Any) -> bool:
        """Determine if the given field def is repeated"""

    ## Implementation Details ##################################################

    def get_descriptor(self, entry: Any) -> Optional[_DescriptorTypesUnion]:
        """Given an entry, try to get a pre-existing descriptor from it. Child
        classes may overwrite this for alternate converters that have other
        known ways of getting a descriptor beyond these basics.
        """
        if isinstance(entry, _DescriptorTypes):
            return entry
        descriptor_attr = getattr(entry, "DESCRIPTOR", None)
        if descriptor_attr and isinstance(descriptor_attr, _DescriptorTypes):
            return descriptor_attr
        return None

    def _add_descriptor_imports(self, descriptor: _DescriptorTypesUnion):
        """Helper to add the descriptor's file to the required imports"""
        import_file = descriptor.file.name
        log.debug3("Adding import file %s", import_file)

        # If the referenced descriptor lives in a different descriptor pool, we
        # need to copy it over to the target pool
        if descriptor.file.pool != self.descriptor_pool:
            log.debug2("Copying descriptor file %s to pool", import_file)
            fd_proto = descriptor_pb2.FileDescriptorProto()
            descriptor.file.CopyToProto(fd_proto)
            safe_add_fd_to_pool(fd_proto, self.descriptor_pool)
        self.imports.add(import_file)

    @staticmethod
    def _get_field_type_name(field_type: Any, field_name: str) -> str:
        """If the nested field definition is a type (a class), the expectation
        is that the nested object will have the same name as the class itself,
        otherwise we use the field name as the implicit name for nested objects.
        """
        if isinstance(field_type, type):
            return field_type.__name__
        return field_name

    def _convert(self, entry: Any, name: str) -> ConvertOutputTypes:
        """This is the core recursive implementation detail function that does
        the common conversion logic for all converters.
        """

        # Handle concrete types
        concrete_type = self.get_concrete_type(entry)
        if concrete_type:
            log.debug2("Handling concrete type: %s", concrete_type)
            return self._convert_concrete_type(concrete_type)

        # Handle Dicts
        map_info = self.get_map_key_val_types(entry)
        if map_info:
            log.debug2("Handling map type: %s", entry)
            return self._convert_map(name, *map_info)

        # Handle enums
        enum_entries = self.get_enum_vals(entry)
        if enum_entries is not None:
            log.debug2("Handling Enum: %s", entry)
            return self._convert_enum(name, entry, enum_entries)

        # Handle messages
        #
        # Returns: descriptor_pb2.DescriptorProto
        message_fields = self.get_message_fields(entry)
        if message_fields is not None:
            log.debug2("Handling Message")
            return self._convert_message(name, entry, message_fields)

        # We should never get here!
        raise ValueError(f"Got unsupported entry type {entry}")

    def _convert_concrete_type(
        self, concrete_type: Any
    ) -> Union[int, _descriptor.Descriptor, _descriptor.EnumDescriptor]:
        """Perform the common conversion for an extracted concrete type"""
        entry_type = self.type_mapping.get(concrete_type, concrete_type)
        proto_type_descriptor = None
        descriptor_ref = self.get_descriptor(entry_type)
        if descriptor_ref is not None:
            proto_type_descriptor = descriptor_ref
        else:
            if concrete_type not in self.type_mapping:
                raise ValueError(f"Invalid type specifier: {concrete_type}")
            proto_type_val = self.type_mapping[concrete_type]
            proto_type_descriptor = getattr(proto_type_val, "DESCRIPTOR", None)
            if proto_type_descriptor is None:
                if not isinstance(proto_type_val, int):
                    raise ValueError(
                        "All proto_type_map values must be Descriptors or int"
                    )
                proto_type_descriptor = proto_type_val

        # If this is a non-primitive type, make sure any import files are added
        if isinstance(proto_type_descriptor, _DescriptorTypes):
            self._add_descriptor_imports(proto_type_descriptor)
        log.debug3("Returning type %s", proto_type_descriptor)
        return proto_type_descriptor

    def _convert_map(
        self,
        name: str,
        key_type: int,
        val_type: ConvertOutputTypes,
    ) -> descriptor_pb2.DescriptorProto:
        """Handle map conversion

        If this is a Dict, handle it by making the "special" submessage and then
        making this field's type be that submessage

        Maps in descriptors are implemented in a _funky_ way. The map syntax
            map<KeyType, ValType> the_map = 1;

        gets converted to a repeated message as follows:
            option map_entry = true;
            optional KeyType key = 1;
            optional ValType value = 2;

         CITE: https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.cc#L7512
        """
        nested_cls_name = f"{to_upper_camel(name)}Entry"
        log.debug3("Making nested map<> class: %s", nested_cls_name)
        key_field = descriptor_pb2.FieldDescriptorProto(
            name="key",
            type=key_type,
            number=1,
        )
        val_field_kwargs = {}
        msg_descriptor_kwargs = {}
        if isinstance(val_type, int):
            val_field_kwargs = {"type": val_type}
        elif isinstance(val_type, _descriptor.EnumDescriptor):
            val_field_kwargs = {
                "type": _descriptor.FieldDescriptor.TYPE_ENUM,
                "type_name": val_type.name,
            }
        elif isinstance(val_type, _descriptor.Descriptor):
            val_field_kwargs = {
                "type": _descriptor.FieldDescriptor.TYPE_MESSAGE,
                "type_name": val_type.name,
            }
        elif isinstance(val_type, descriptor_pb2.EnumDescriptorProto):
            val_field_kwargs = {
                "type": _descriptor.FieldDescriptor.TYPE_ENUM,
                "type_name": val_type.name,
            }
            msg_descriptor_kwargs["enum_type"] = [val_type]
        elif isinstance(val_type, descriptor_pb2.DescriptorProto):
            val_field_kwargs = {
                "type": _descriptor.FieldDescriptor.TYPE_MESSAGE,
                "type_name": val_type.name,
            }
            msg_descriptor_kwargs["nested_type"] = [val_type]
        assert (
            val_field_kwargs
        ), f"Programming Error: Got unhandled map value type: {val_type}"
        val_field = descriptor_pb2.FieldDescriptorProto(
            name="value",
            number=2,
            **val_field_kwargs,
        )
        nested = descriptor_pb2.DescriptorProto(
            name=nested_cls_name,
            field=[key_field, val_field],
            options=descriptor_pb2.MessageOptions(map_entry=True),
            **msg_descriptor_kwargs,
        )
        return nested

    def _convert_enum(
        self, name: str, entry: Any, enum_entries: Iterable[Tuple[str, int]]
    ) -> descriptor_pb2.EnumDescriptorProto:
        """Convert nested enums"""
        enum_name = self._get_field_type_name(entry, to_upper_camel(name))
        log.debug("Enum name: %s", enum_name)
        has_aliases = len(set([entry[1] for entry in enum_entries])) != len(
            enum_entries
        )
        options = descriptor_pb2.EnumOptions(allow_alias=has_aliases)
        enum_proto = descriptor_pb2.EnumDescriptorProto(
            name=enum_name,
            value=[
                descriptor_pb2.EnumValueDescriptorProto(
                    name=entry[0],
                    number=entry[1],
                )
                for entry in enum_entries
            ],
            options=options,
        )
        return enum_proto

    def _convert_message(
        self,
        name: str,
        entry: Any,
        message_fields,
    ) -> descriptor_pb2.DescriptorProto:
        """Convert a nested message"""
        field_descriptors = []
        nested_enums = []
        nested_messages = []
        nested_oneofs = []
        message_name = to_upper_camel(name)
        log.debug("Message name: %s", message_name)

        for field_name, field_def in message_fields:
            field_number = self.get_field_number(len(field_descriptors), field_def)
            log.debug2(
                "Handling field [%s.%s] (%d)",
                message_name,
                field_name,
                field_number,
            )

            # Get the field's number
            field_kwargs = {
                "name": field_name,
                "number": field_number,
                "label": _descriptor.FieldDescriptor.LABEL_OPTIONAL,
            }

            # Check to see if the field is repeated
            if self.is_repeated_field(field_def):
                log.debug3("Handling repeated field %s", field_name)
                field_kwargs["label"] = _descriptor.FieldDescriptor.LABEL_REPEATED

            # If the field is a oneof, handle it as such
            oneof_fields = self.get_oneof_fields(field_def)
            if oneof_fields:
                log.debug2("Handling oneof field %s", field_name)
                nested_results = [
                    (
                        self._convert(
                            entry=oneof_field_def,
                            name=self._get_field_type_name(
                                oneof_field_def, oneof_field_name
                            ),
                        ),
                        {
                            "oneof_index": len(nested_oneofs),
                            "number": self.get_field_number(
                                len(field_descriptors) + oneof_field_idx,
                                oneof_field_def,
                            ),
                            "name": oneof_field_name.lower(),
                        },
                    )
                    for oneof_field_idx, (
                        oneof_field_name,
                        oneof_field_def,
                    ) in enumerate(oneof_fields)
                ]
                # Add the name for this oneof
                nested_oneofs.append(
                    descriptor_pb2.OneofDescriptorProto(
                        name=self.get_oneof_name(field_def)
                    )
                )

            # Otherwise, it's a "regular" field, so just recurse on the type
            else:
                log.debug3("Handling non-oneof field: %s", field_name)
                # If the nested field definition is a type (a class), the
                # expectation is that the nested object will have the same name
                # as the class itself, otherwise we use the field name as the
                # implicit name for nested objects.
                field_type = self.get_field_type(field_def)
                nested_name = self._get_field_type_name(field_type, field_name)
                nested_result = self._convert(entry=field_type, name=nested_name)
                nested_results = [(nested_result, {})]

            # For all nested fields produced by either the onoof logic or
            # the single-field logic, construct a FieldDescriptor and add it
            # to the message descriptor.
            for nested, extra_kwargs in nested_results:
                nested_field_kwargs = copy.copy(field_kwargs)
                nested_field_kwargs.update(extra_kwargs)

                # If the result is an int, it's a type value
                if isinstance(nested, int):
                    nested_field_kwargs["type"] = nested

                # If the result is an enum descriptor ref, it's an external
                # enum
                elif isinstance(nested, _descriptor.EnumDescriptor):
                    nested_field_kwargs["type"] = _descriptor.FieldDescriptor.TYPE_ENUM
                    nested_field_kwargs["type_name"] = nested.full_name

                # If the result is a message descriptor ref, it's an
                # external message
                elif isinstance(nested, _descriptor.Descriptor):
                    nested_field_kwargs[
                        "type"
                    ] = _descriptor.FieldDescriptor.TYPE_MESSAGE
                    nested_field_kwargs["type_name"] = nested.full_name

                # If the result is an enum proto, it's a nested enum
                elif isinstance(nested, descriptor_pb2.EnumDescriptorProto):
                    log.debug3("Adding nested enum %s", nested.name)
                    nested_field_kwargs["type"] = _descriptor.FieldDescriptor.TYPE_ENUM
                    nested_field_kwargs["type_name"] = nested.name
                    nested_enums.append(nested)

                # If the result is a message proto, it's a nested message
                elif isinstance(nested, descriptor_pb2.DescriptorProto):
                    log.debug3("Adding nested message %s", nested.name)
                    nested_field_kwargs[
                        "type"
                    ] = _descriptor.FieldDescriptor.TYPE_MESSAGE
                    nested_field_kwargs["type_name"] = nested.name
                    nested_messages.append(nested)

                    # If the message has map_entry set, we need to indicate that
                    # it's repeated
                    if nested.options.map_entry:
                        nested_field_kwargs[
                            "label"
                        ] = _descriptor.FieldDescriptor.LABEL_REPEATED

                        # If the nested map entry itself has nested types or enums,
                        # they need to be moved up to this message
                        while nested.nested_type:
                            nested_type = nested.nested_type.pop()
                            plain_name = nested_type.name
                            nested_name = to_upper_camel(
                                "_".join([field_name, plain_name])
                            )
                            nested_type.MergeFrom(
                                descriptor_pb2.DescriptorProto(name=nested_name)
                            )
                            for field in nested.field:
                                if field.type_name == plain_name:
                                    field.MergeFrom(
                                        descriptor_pb2.FieldDescriptorProto(
                                            type_name=nested_name
                                        )
                                    )
                            nested_messages.append(nested_type)
                        while nested.enum_type:
                            nested_enum = nested.enum_type.pop()
                            plain_name = nested_enum.name
                            nested_name = to_upper_camel(
                                "_".join([field_name, plain_name])
                            )
                            nested_enum.MergeFrom(
                                descriptor_pb2.EnumDescriptorProto(name=nested_name)
                            )
                            for field in nested.field:
                                if field.type_name == plain_name:
                                    field.MergeFrom(
                                        descriptor_pb2.FieldDescriptorProto(
                                            type_name=nested_name
                                        )
                                    )
                            nested_enums.append(nested_enum)

                # Create the field descriptor
                field_descriptors.append(
                    descriptor_pb2.FieldDescriptorProto(**nested_field_kwargs)
                )

        # If additional keys/vals allowed, add a 'special' field for this.
        # This is one place where there's not a good mapping between some
        # schema types and proto since proto does not allow for arbitrary
        # mappings _in addition_ to specific keys. Instead, there needs to
        # be a special Struct field to hold these additional fields.
        if self.has_additional_fields(entry):
            if "additionalProperties" in [field.name for field in field_descriptors]:
                raise ValueError(
                    "Cannot specify 'additionalProperties' as a field and support arbitrary key/vals"
                )
            field_descriptors.append(
                descriptor_pb2.FieldDescriptorProto(
                    name="additionalProperties",
                    number=len(field_descriptors) + 1,
                    type=_descriptor.FieldDescriptor.TYPE_MESSAGE,
                    label=_descriptor.FieldDescriptor.LABEL_OPTIONAL,
                    type_name=struct_pb2.Struct.DESCRIPTOR.full_name,
                )
            )
            self.imports.add(struct_pb2.Struct.DESCRIPTOR.file.name)

        # Support optional properties as oneofs i.e. optional int32 foo = 1;
        # becomes interpreted as oneof _foo { int32 foo = 1; }
        optional_oneofs: List[descriptor_pb2.OneofDescriptorProto] = []
        for field in field_descriptors:
            if (
                field.name in self.get_optional_field_names(entry)
                and field.label == _descriptor.FieldDescriptor.LABEL_OPTIONAL
            ):
                log.debug3("Making field %s as optional with oneof", field.name)
                # OneofDescriptorProto do not contain fields themselves.
                # Instead the FieldDescriptorProto must contain the index of
                # the oneof inside the DescriptorProto
                optional_oneofs.append(
                    descriptor_pb2.OneofDescriptorProto(name=f"_{field.name}")
                )
                field.oneof_index = len(nested_oneofs) + len(optional_oneofs) - 1

        # Construct the message descriptor proto with the aggregated fields
        # and nested enums/messages/oneofs
        log.debug3(
            "All field descriptors for [%s]:\n%s", message_name, field_descriptors
        )
        descriptor_proto = descriptor_pb2.DescriptorProto(
            name=message_name,
            field=field_descriptors,
            enum_type=nested_enums,
            nested_type=nested_messages,
            oneof_decl=nested_oneofs + optional_oneofs,
        )
        return descriptor_proto
