"""The closed set of column kinds and filter operations.

CAVE's schema flexibility reduces, at the level of what can be *filtered*, to a
small fixed taxonomy. A schema column is one of a handful of declared types
(integer, float, boolean, string, SpatialPoint, BoundSpatialPoint); each expands
into one or more *filterable* columns, and the operations each filterable column
permits follow from its kind rather than being freeform.

This module is the single source of truth for that taxonomy. Both filter
validation (``caveclient.query.filters``) and, eventually, the dynamic per-table
classes in ``caveclient.tools.table_manager`` should derive from it so they
agree by construction.
"""

from __future__ import annotations

import enum

# Schema-declared column types, as they appear in emannotationschemas / the
# server's JSON schema definitions. Mirrors the constants historically kept in
# tools/table_manager.py.
SCALAR_TYPES = ("integer", "boolean", "string", "float")
NUMERIC_TYPES = ("integer", "float")
SPATIAL_POINT_TYPE = "SpatialPoint"
BOUND_SPATIAL_POINT_TYPE = "BoundSpatialPoint"


class FilterKind(enum.Enum):
    """The kind of a *filterable* column — i.e. a leaf column a filter can target.

    A ``BoundSpatialPoint`` schema column is not itself a ``FilterKind``; it
    expands into a ``POSITION`` column plus ``ID`` columns (supervoxel_id,
    root_id). ``SpatialPoint`` expands into a single ``POSITION`` column.
    """

    NUMERIC = "numeric"
    STRING = "string"
    BOOLEAN = "boolean"
    ID = "id"  # root_id / supervoxel_id: an equatable integer, but never ordered
    POSITION = "position"  # the x/y/z of a spatial point; bounding-box filterable


class FilterOp(enum.Enum):
    """A filter operation, with its server wire key.

    The ``out`` operation is sent over the wire as ``filter_notin_dict`` — a
    historical quirk that previously leaked into every query method separately.
    """

    IN = "in"
    NOT_IN = "not_in"
    EQUAL = "equal"
    GREATER = "greater"
    LESS = "less"
    GREATER_EQUAL = "greater_equal"
    LESS_EQUAL = "less_equal"
    REGEX = "regex"
    SPATIAL = "spatial"

    @property
    def payload_key(self) -> str:
        """The key this operation uses in the server request payload."""
        return _PAYLOAD_KEYS[self]


_PAYLOAD_KEYS = {
    FilterOp.IN: "filter_in_dict",
    FilterOp.NOT_IN: "filter_notin_dict",
    FilterOp.EQUAL: "filter_equal_dict",
    FilterOp.GREATER: "filter_greater_dict",
    FilterOp.LESS: "filter_less_dict",
    FilterOp.GREATER_EQUAL: "filter_greater_equal_dict",
    FilterOp.LESS_EQUAL: "filter_less_equal_dict",
    FilterOp.REGEX: "filter_regex_dict",
    FilterOp.SPATIAL: "filter_spatial_dict",
}

_ORDERED_OPS = frozenset(
    {
        FilterOp.GREATER,
        FilterOp.LESS,
        FilterOp.GREATER_EQUAL,
        FilterOp.LESS_EQUAL,
    }
)

_EQUATABLE_OPS = frozenset({FilterOp.IN, FilterOp.NOT_IN, FilterOp.EQUAL})

# Which operations each filterable-column kind permits. This is the same
# constraint the server enforces (e.g. inequality only on numeric columns,
# surfaced server-side as InvalidInequalityException).
LEGAL_OPS: dict[FilterKind, frozenset[FilterOp]] = {
    FilterKind.NUMERIC: _EQUATABLE_OPS | _ORDERED_OPS,
    FilterKind.STRING: _EQUATABLE_OPS | frozenset({FilterOp.REGEX}),
    FilterKind.BOOLEAN: _EQUATABLE_OPS,
    FilterKind.ID: _EQUATABLE_OPS,
    FilterKind.POSITION: frozenset({FilterOp.SPATIAL}),
}


def legal_ops(kind: FilterKind) -> frozenset[FilterOp]:
    """Return the operations permitted on a column of the given kind."""
    return LEGAL_OPS[kind]
