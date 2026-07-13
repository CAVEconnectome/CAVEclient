"""Typed filter intermediates with structural (schema-free) validation.

A ``Filter`` pairs a ``ColumnHandle`` (a named column of a known kind) with an
operation and a value. Validation happens in two stages across the codebase:

* **Structural** — here, at construction. Schema-free: the operation must be
  legal for the column's kind, and the value's shape must match the operation
  (a bounding box is 2x3, a regex is a string, an inequality bound is a real
  number, an ``in``/``not_in`` value is a non-string sequence). A malformed
  filter therefore fails in the caller's stack frame, not as a server 500.
* **Schema-aware** — later, against cached table/view metadata, just before
  dispatch (column exists, kinds line up). That lives with the query spec.
"""

from __future__ import annotations

import numbers
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .kinds import FilterKind, FilterOp, legal_ops


class InvalidFilterError(ValueError):
    """Raised when a filter is structurally invalid (illegal op or bad value)."""


# Why OR/NOT are refused: the CAVE server WHERE is a conjunction of per-column
# predicates (see docs/design/accessor_polars_alignment.md §5). AND lowers; OR and
# general negation do not, so they fail in the caller's frame with a pointer to the
# escape hatch rather than silently producing something lossy.
_OR_MESSAGE = (
    "`|` (OR) is not expressible against the CAVE backend: the server filters are a "
    "conjunction of per-column predicates. Combine predicates with `&`, post-filter "
    "the returned frame, or issue separate queries and concatenate."
)
_NOT_MESSAGE = (
    "`~` (NOT) is not expressible against the CAVE backend; use `.notin([...])` for "
    "set-exclusion, or post-filter the returned frame."
)


@dataclass(frozen=True)
class ColumnHandle:
    """A reference to a single filterable column.

    Parameters
    ----------
    name :
        The column name as the server/dataframe knows it (e.g. ``"pre_pt_root_id"``).
    kind :
        The filterable kind, which determines the legal operations.
    table :
        The table or view the column belongs to. ``None`` means the query's
        primary source, resolved at serialization time.
    """

    name: str
    kind: FilterKind
    table: str | None = None


def _is_sequence(value: Any) -> bool:
    # A sequence for our purposes is list/tuple/ndarray-like, but NOT a string.
    if isinstance(value, (str, bytes)):
        return False
    if isinstance(value, Sequence):
        return True
    # numpy arrays, pandas Series, etc. are sequence-like without subclassing Sequence
    return hasattr(value, "__len__") and hasattr(value, "__iter__")


def _validate_value(op: FilterOp, value: Any) -> None:
    if op in (FilterOp.IN, FilterOp.NOT_IN):
        if not _is_sequence(value):
            raise InvalidFilterError(
                f"`{op.value}` requires a sequence of values, got {type(value).__name__}"
            )
    elif op in (
        FilterOp.GREATER,
        FilterOp.LESS,
        FilterOp.GREATER_EQUAL,
        FilterOp.LESS_EQUAL,
    ):
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise InvalidFilterError(
                f"`{op.value}` requires a real-number bound, got {value!r}"
            )
    elif op is FilterOp.REGEX:
        if not isinstance(value, str):
            raise InvalidFilterError(
                f"`regex` requires a string pattern, got {type(value).__name__}"
            )
    elif op is FilterOp.SPATIAL:
        _validate_bbox(value)
    # EQUAL accepts any scalar; no shape constraint beyond not being a sequence
    elif op is FilterOp.EQUAL:
        if _is_sequence(value):
            raise InvalidFilterError(
                "`equal` requires a single value; use `in` for multiple values"
            )


def _validate_bbox(value: Any) -> None:
    if not _is_sequence(value) or len(value) != 2:
        raise InvalidFilterError(
            "`spatial` requires bounds [[min_x, min_y, min_z], [max_x, max_y, max_z]]"
        )
    for corner in value:
        if not _is_sequence(corner) or len(corner) != 3:
            raise InvalidFilterError(
                "`spatial` bounds corners must each have three coordinates (x, y, z)"
            )


def _normalized_bbox(value: Any) -> list:
    """Sort a 2-corner bounding box into ``[[min...], [max...]]`` per axis.

    The server requires the lower corner to be strictly the min on every axis;
    sorting client-side lets a caller pass any two opposite corners in any order.
    """
    a, b = value[0], value[1]
    lo = [min(a[i], b[i]) for i in range(len(a))]
    hi = [max(a[i], b[i]) for i in range(len(a))]
    return [lo, hi]


@dataclass(frozen=True)
class Filter:
    """A single typed filter: a column, an operation, and a value.

    Structural validation runs at construction. Constructing a ``Filter`` with
    an operation the column kind does not permit, or a value whose shape does
    not match the operation, raises ``InvalidFilterError``.
    """

    column: ColumnHandle
    op: FilterOp
    value: Any

    def __post_init__(self) -> None:
        if self.op not in legal_ops(self.column.kind):
            permitted = ", ".join(sorted(o.value for o in legal_ops(self.column.kind)))
            raise InvalidFilterError(
                f"operation `{self.op.value}` is not valid for column "
                f"`{self.column.name}` of kind `{self.column.kind.value}`; "
                f"permitted operations: {permitted}"
            )
        _validate_value(self.op, self.value)
        if self.op is FilterOp.SPATIAL:
            # sort the corners so callers needn't give them min-first
            object.__setattr__(self, "value", _normalized_bbox(self.value))

    # -- combinators (Polars-style) ------------------------------------------
    # `&` builds a conjunction (the only combinator the server supports); `|`/`~`
    # are refused because the backend WHERE is a conjunction of per-column
    # predicates. See docs/design/accessor_polars_alignment.md §5.
    def __and__(self, other) -> "AllOf":
        return AllOf((self, *_as_filter_tuple(other)))

    def __or__(self, other):
        raise InvalidFilterError(_OR_MESSAGE)

    def __invert__(self):
        raise InvalidFilterError(_NOT_MESSAGE)


@dataclass(frozen=True)
class AllOf:
    """A conjunction of filters — the result of ``filter_a & filter_b``.

    A thin carrier so ``a & b & c`` reads like Polars; it flattens to its
    underlying :class:`Filter` tuple at query-build time (every CAVE filter is
    AND-ed anyway). Only ``&`` extends it; ``|``/``~`` are refused.
    """

    filters: tuple

    def __and__(self, other) -> "AllOf":
        return AllOf((*self.filters, *_as_filter_tuple(other)))

    def __or__(self, other):
        raise InvalidFilterError(_OR_MESSAGE)

    def __invert__(self):
        raise InvalidFilterError(_NOT_MESSAGE)


def _as_filter_tuple(item) -> tuple:
    """Coerce a combinator operand to a tuple of ``Filter``, or raise."""
    if isinstance(item, Filter):
        return (item,)
    if isinstance(item, AllOf):
        return item.filters
    raise InvalidFilterError(
        f"cannot combine a filter with {type(item).__name__}; expected a filter "
        "expression (e.g. `col > 1`). A bare column handle is not a predicate."
    )


def flatten_filters(items) -> tuple:
    """Expand a mix of ``Filter`` and ``AllOf`` into a flat tuple of ``Filter``.

    Used by the accessor's ``.filter()``/``.query()`` to accept both single
    predicates and ``&``-conjunctions. Anything else (a bare column handle, a
    bool, ...) raises with a clear message rather than silently dropping.
    """
    out = []
    for item in items:
        if isinstance(item, Filter):
            out.append(item)
        elif isinstance(item, AllOf):
            out.extend(item.filters)
        else:
            raise InvalidFilterError(
                f"expected a filter expression (e.g. `col > 1` or `col.isin([...])`)"
                f", got {type(item).__name__}. A bare column handle is not a "
                "predicate; apply an operator. Use keyword filters (col=, col__op=) "
                "for value filters."
            )
    return tuple(out)
