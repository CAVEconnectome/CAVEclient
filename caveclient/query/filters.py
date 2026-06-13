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
