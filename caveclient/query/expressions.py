"""Column handles and keyword filters for the table/view accessor.

Two ways to express a filter, both producing validated typed
:class:`~caveclient.query.filters.Filter` objects:

* **Column handles** (the expressive path)::

      syn.size > 100
      syn.pre_pt_root_id.isin([1, 2])
      syn.tag.regex("^L2")
      syn.pt_position.within([[0, 0, 0], [10, 10, 10]])

* **Keyword filters** (the easy path), parsed from ``col=`` / ``col__op=``::

      pre_pt_root_id=[1, 2]            # sequence -> in, scalar -> equal
      size__gt=100, size__lte=1000
      tag__regex="^L2"
      pt_position__bbox=[[0, 0, 0], [10, 10, 10]]

Because both build ``Filter`` objects, an illegal operator for a column's kind
(e.g. a regex on a numeric column) fails immediately with ``InvalidFilterError``.
"""

from __future__ import annotations

import difflib
from typing import Any, Optional

from .filters import (
    _NOT_MESSAGE,
    _OR_MESSAGE,
    AllOf,
    ColumnHandle,
    Filter,
    InvalidFilterError,
    _is_sequence,
)
from .kinds import FilterKind, FilterOp


class Column:
    """A table column exposed as an attribute on a table query object.

    Comparison operators and the ``isin``/``notin``/``regex``/``within`` methods
    each return a :class:`~caveclient.query.filters.Filter`. (Defining the rich
    comparisons means a ``Column`` should not be used as a plain value, e.g. as a
    dict key for equality — it is a query-building handle, à la SQLAlchemy.)
    """

    __slots__ = ("name", "kind", "table")

    def __init__(self, name: str, kind: FilterKind, table: Optional[str] = None):
        self.name = name
        self.kind = kind
        self.table = table

    def _handle(self) -> ColumnHandle:
        return ColumnHandle(self.name, self.kind, self.table)

    def _filter(self, op: FilterOp, value: Any) -> Filter:
        if isinstance(value, (Column, ColumnHandle)):
            raise InvalidFilterError(
                f"cannot compare column `{self.name}` to another column "
                f"(`{getattr(value, 'name', value)!r}`): CAVE filters compare a "
                "column to a literal value, not to another column. Compute the "
                "comparison on the returned frame instead."
            )
        return Filter(self._handle(), op, value)

    # ordering operators
    def __gt__(self, value):
        return self._filter(FilterOp.GREATER, value)

    def __lt__(self, value):
        return self._filter(FilterOp.LESS, value)

    def __ge__(self, value):
        return self._filter(FilterOp.GREATER_EQUAL, value)

    def __le__(self, value):
        return self._filter(FilterOp.LESS_EQUAL, value)

    # equality: == a scalar -> equal; == a sequence -> in
    def __eq__(self, value):
        op = FilterOp.IN if _is_sequence(value) else FilterOp.EQUAL
        return self._filter(op, value)

    def __ne__(self, value):
        if isinstance(value, (Column, ColumnHandle)):
            return self._filter(FilterOp.NOT_IN, value)  # _filter raises
        return self._filter(FilterOp.NOT_IN, value if _is_sequence(value) else [value])

    __hash__ = object.__hash__  # identity; handles aren't value-compared

    # boolean combinators: `&` joins *filter expressions* (not bare columns), so
    # combining bare columns, or using `|`/`~`, is refused with a clear message.
    def __and__(self, other):
        raise InvalidFilterError(
            "`&` combines filter expressions, not bare columns; write "
            "`(col > 1) & (col2 < 2)`, applying an operator to each column first."
        )

    def __or__(self, other):
        raise InvalidFilterError(_OR_MESSAGE)

    def __invert__(self):
        raise InvalidFilterError(_NOT_MESSAGE)

    # explicit, readable methods
    def isin(self, values):
        return self._filter(FilterOp.IN, values)

    def is_in(self, values):
        """Polars spelling of :meth:`isin`."""
        return self.isin(values)

    def notin(self, values):
        return self._filter(FilterOp.NOT_IN, values)

    def is_between(self, lower, upper, closed: str = "both"):
        """Range filter, like Polars: ``lower <= col <= upper`` for ``closed="both"``.

        ``closed`` is one of ``"both"``, ``"left"``, ``"right"``, ``"none"``. Lowers
        to a conjunction of two bound filters, so it inherits numeric-only legality
        (a non-numeric column raises ``InvalidFilterError``).
        """
        if closed not in ("both", "left", "right", "none"):
            raise ValueError(
                f"closed must be 'both', 'left', 'right', or 'none', got {closed!r}"
            )
        lo_op = (
            FilterOp.GREATER_EQUAL if closed in ("both", "left") else FilterOp.GREATER
        )
        hi_op = FilterOp.LESS_EQUAL if closed in ("both", "right") else FilterOp.LESS
        return AllOf((self._filter(lo_op, lower), self._filter(hi_op, upper)))

    def equals(self, value):
        return self._filter(FilterOp.EQUAL, value)

    def regex(self, pattern):
        return self._filter(FilterOp.REGEX, pattern)

    def within(self, bounds):
        """Spatial (bounding-box) filter: ``[[min_x, min_y, min_z], [max_x, ...]]``."""
        return self._filter(FilterOp.SPATIAL, bounds)

    def __repr__(self):
        return f"Column({self.name!r}, {self.kind.value})"


# Legacy inequality wrapper dicts: ``col={"<": 100}`` (back-compat with the
# original table interface).
_WRAPPER_OPS = {
    "<": FilterOp.LESS,
    ">": FilterOp.GREATER,
    "<=": FilterOp.LESS_EQUAL,
    ">=": FilterOp.GREATER_EQUAL,
}


def _is_wrapper_dict(value) -> bool:
    return (
        isinstance(value, dict)
        and len(value) == 1
        and next(iter(value)) in _WRAPPER_OPS
    )


# Keyword-filter operator suffixes (col__<suffix>=value).
_SUFFIX_OPS = {
    "in": FilterOp.IN,
    "not_in": FilterOp.NOT_IN,
    "notin": FilterOp.NOT_IN,
    "eq": FilterOp.EQUAL,
    "gt": FilterOp.GREATER,
    "lt": FilterOp.LESS,
    "ge": FilterOp.GREATER_EQUAL,
    "gte": FilterOp.GREATER_EQUAL,
    "le": FilterOp.LESS_EQUAL,
    "lte": FilterOp.LESS_EQUAL,
    "regex": FilterOp.REGEX,
    "bbox": FilterOp.SPATIAL,
    "within": FilterOp.SPATIAL,
}


def parse_filter_kwargs(
    column_kinds: dict,
    kwargs: dict,
    table: Optional[str] = None,
    column_tables: Optional[dict] = None,
    column_real_names: Optional[dict] = None,
) -> tuple:
    """Turn ``col=``/``col__op=`` keyword filters into typed ``Filter`` objects.

    Parameters
    ----------
    column_kinds :
        Mapping of column name to ``FilterKind`` for the table/view.
    kwargs :
        The filter keyword arguments. A bare ``col=value`` is an ``in`` filter for
        a sequence value, an inequality for a wrapper dict (``col={"<": 100}``,
        back-compatible with the original interface), otherwise ``equal``. A
        ``col__suffix=value`` uses the named operator (``gt``, ``lt``, ``ge``/
        ``gte``, ``le``/``lte``, ``in``, ``not_in``, ``eq``, ``regex``,
        ``bbox``/``within``). The legacy ``<col>_bbox=[[...],[...]]`` spatial form
        is also accepted.
    table :
        Default table name stamped on each column handle.
    column_tables :
        Optional ``{column: owning_table}`` overriding ``table`` per column (so a
        filter on a merged reference-table column routes to the reference table).
    column_real_names :
        Optional ``{display_column: real_column}`` for columns exposed under a
        different name than they carry on the wire (a reference column that
        collided and is surfaced as ``<col>_ref`` maps back to ``<col>``).
    """
    column_tables = column_tables or {}
    column_real_names = column_real_names or {}
    filters = []
    for key, value in kwargs.items():
        col, _, suffix = key.partition("__")
        if suffix:
            op = _SUFFIX_OPS.get(suffix)
            if op is None:
                raise TypeError(
                    f"unknown filter operator `__{suffix}` in `{key}`; valid: "
                    f"{', '.join(sorted(_SUFFIX_OPS))}"
                )
        elif _is_wrapper_dict(value):
            op_key, value = next(iter(value.items()))
            op = _WRAPPER_OPS[op_key]
        elif (
            col not in column_kinds
            and col.endswith("_bbox")
            and column_kinds.get(col[: -len("_bbox")]) is FilterKind.POSITION
        ):
            # legacy spatial form: pt_position_bbox=[[...],[...]]
            col, op = col[: -len("_bbox")], FilterOp.SPATIAL
        else:
            op = FilterOp.IN if _is_sequence(value) else FilterOp.EQUAL
        kind = column_kinds.get(col)
        if kind is None:
            close = difflib.get_close_matches(col, list(column_kinds), n=3)
            hint = (
                f"; did you mean {' or '.join(repr(c) for c in close)}?"
                if close
                else ""
            )
            raise KeyError(
                f"`{col}` is not a filterable column"
                + (f" of `{table}`" if table else "")
                + hint
            )
        owning = column_tables.get(col, table)
        real = column_real_names.get(col, col)
        filters.append(Filter(ColumnHandle(real, kind, owning), op, value))
    return tuple(filters)
