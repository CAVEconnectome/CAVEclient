"""Typed query intermediates for CAVEclient's unified query interface.

This package holds the schema-truth taxonomy of column kinds and filter
operations, the typed ``Filter`` intermediate, and serialization to the
server's request payload. It underpins the unified query switchboard (see
``docs/design/unified_query_interface.md``).
"""

from .filters import ColumnHandle, Filter, InvalidFilterError
from .kinds import FilterKind, FilterOp, legal_ops
from .serialize import filters_to_payload
from .spec import At, InvalidQueryError, OutputOptions, QuerySpec, Source

__all__ = [
    "At",
    "ColumnHandle",
    "Filter",
    "FilterKind",
    "FilterOp",
    "InvalidFilterError",
    "InvalidQueryError",
    "OutputOptions",
    "QuerySpec",
    "Source",
    "filters_to_payload",
    "legal_ops",
]
