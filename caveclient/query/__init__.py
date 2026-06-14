"""Typed query intermediates for CAVEclient's unified query interface.

This package holds the schema-truth taxonomy of column kinds and filter
operations, the typed ``Filter`` intermediate, and serialization to the
server's request payload. It underpins the unified query switchboard (see
``docs/design/unified_query_interface.md``).
"""

from .backends import (
    DEFAULT_BACKENDS,
    Capabilities,
    QueryBackend,
    Switchboard,
    UnroutableQueryError,
)
from .expressions import Column, parse_filter_kwargs
from .filters import ColumnHandle, Filter, InvalidFilterError
from .kinds import FilterKind, FilterOp, legal_ops
from .serialize import filters_to_method_kwargs, filters_to_payload
from .spec import (
    At,
    InvalidQueryError,
    Join,
    OutputOptions,
    QuerySpec,
    Source,
    Table,
    resolve_version_fallback,
)
from .tables import TableManager, TableQuery, ViewManager

__all__ = [
    "At",
    "Capabilities",
    "Column",
    "ColumnHandle",
    "DEFAULT_BACKENDS",
    "Filter",
    "FilterKind",
    "FilterOp",
    "InvalidFilterError",
    "InvalidQueryError",
    "Join",
    "OutputOptions",
    "QueryBackend",
    "QuerySpec",
    "Source",
    "Switchboard",
    "Table",
    "TableManager",
    "TableQuery",
    "UnroutableQueryError",
    "ViewManager",
    "filters_to_method_kwargs",
    "filters_to_payload",
    "legal_ops",
    "parse_filter_kwargs",
    "resolve_version_fallback",
]
