"""
Helper module to allow Annotated to be imported in 3.8

The majority of this module is pulled directly from cpython 3.10.10
CITE: https://github.com/python/cpython/blob/v3.10.10/Lib/typing.py#L1613
"""

try:
    # Standard
    from typing import Annotated, get_args, get_origin
except ImportError:
    # Standard
    from typing import _GenericAlias, _tp_cache, _type_check, _type_repr
    from typing import get_args as _get_args
    from typing import get_origin as _get_origin
    import operator

    ############################################################################
    # The code below retains the copyright from the PSF License Agreement
    #
    # Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010,
    # 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
    # 2023 Python Software Foundation;
    # All Rights Reserved
    #
    # This derivative work has one small change from the original source to
    # avoid needing to pull more code in. The _type_check invocation inside
    # Annotated.__class_getitem__ omits the allow_special_forms argument which
    # is there in the 3.10 version to handle cases that are not relevant to the
    # usage here and would require further copy-paste porting to support.
    ############################################################################

    class _AnnotatedAlias(_GenericAlias, _root=True):
        """Runtime representation of an annotated type.

        At its core 'Annotated[t, dec1, dec2, ...]' is an alias for the type 't'
        with extra annotations. The alias behaves like a normal typing alias,
        instantiating is the same as instantiating the underlying type, binding
        it to types is also the same.
        """

        def __init__(self, origin, metadata):
            if isinstance(origin, _AnnotatedAlias):
                metadata = origin.__metadata__ + metadata
                origin = origin.__origin__
            super().__init__(origin, origin)
            self.__metadata__ = metadata

        def copy_with(self, params):
            assert len(params) == 1
            new_type = params[0]
            return _AnnotatedAlias(new_type, self.__metadata__)

        def __repr__(self):
            return "typing.Annotated[{}, {}]".format(
                _type_repr(self.__origin__),
                ", ".join(repr(a) for a in self.__metadata__),
            )

        def __reduce__(self):
            return operator.getitem, (Annotated, (self.__origin__,) + self.__metadata__)

        def __eq__(self, other):
            if not isinstance(other, _AnnotatedAlias):
                return NotImplemented
            return (
                self.__origin__ == other.__origin__
                and self.__metadata__ == other.__metadata__
            )

        def __hash__(self):
            return hash((self.__origin__, self.__metadata__))

        def __getattr__(self, attr):
            if attr in {"__name__", "__qualname__"}:
                return "Annotated"
            return super().__getattr__(attr)

    class Annotated:
        """Add context specific metadata to a type.

        Example: Annotated[int, runtime_check.Unsigned] indicates to the
        hypothetical runtime_check module that this type is an unsigned int.
        Every other consumer of this type can ignore this metadata and treat
        this type as int.

        The first argument to Annotated must be a valid type.

        Details:

        - It's an error to call `Annotated` with less than two arguments.
        - Nested Annotated are flattened::

            Annotated[Annotated[T, Ann1, Ann2], Ann3] == Annotated[T, Ann1, Ann2, Ann3]

        - Instantiating an annotated type is equivalent to instantiating the
        underlying type::

            Annotated[C, Ann1](5) == C(5)

        - Annotated can be used as a generic type alias::

            Optimized = Annotated[T, runtime.Optimize()]
            Optimized[int] == Annotated[int, runtime.Optimize()]

            OptimizedList = Annotated[List[T], runtime.Optimize()]
            OptimizedList[int] == Annotated[List[int], runtime.Optimize()]
        """

        __slots__ = ()

        def __new__(cls, *args, **kwargs):
            raise TypeError("Type Annotated cannot be instantiated.")

        @_tp_cache
        def __class_getitem__(cls, params):
            if not isinstance(params, tuple) or len(params) < 2:
                raise TypeError(
                    "Annotated[...] should be used "
                    "with at least two arguments (a type and an "
                    "annotation)."
                )
            msg = "Annotated[t, ...]: t must be a type."
            origin = _type_check(params[0], msg)  # NOTE: Removed allow_special_forms
            metadata = tuple(params[1:])
            return _AnnotatedAlias(origin, metadata)

        def __init_subclass__(cls, *args, **kwargs):
            raise TypeError("Cannot subclass {}.Annotated".format(cls.__module__))

    def get_origin(tp):
        """Compatibility layer for get_origin to support Annotated"""
        if isinstance(tp, _AnnotatedAlias):
            return Annotated
        return _get_origin(tp)

    def get_args(tp):
        """Compatibility layer for get_args to support Annotated"""
        if isinstance(tp, _AnnotatedAlias):
            return (tp.__origin__,) + tp.__metadata__
        return _get_args(tp)
