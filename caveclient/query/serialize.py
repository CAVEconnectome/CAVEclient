"""Serialize typed filters into the server's request payload.

Because the column kinds and operations are a closed set, this is a total
function: every ``Filter`` maps to exactly one wire key and one nested
``{table: {column: value}}`` entry. The per-operation wire-key quirks (notably
``out`` -> ``filter_notin_dict``) live here, once, instead of being repeated in
each query method.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .filters import ColumnHandle, Filter
from .kinds import FilterKind, FilterOp

# Reverse of FilterOp.kwarg_key: the argument names the existing methods accept.
_KWARG_TO_OP = {op.kwarg_key: op for op in FilterOp}


def _serialize_value(f: Filter) -> Any:
    if f.op in (FilterOp.IN, FilterOp.NOT_IN):
        # Normalize any sequence-like (list/tuple/ndarray/Series) to a list so
        # the JSON encoder sees a plain list regardless of input container.
        return list(f.value)
    if f.op is FilterOp.SPATIAL:
        return [list(corner) for corner in f.value]
    return f.value


def filters_to_payload(
    filters: Iterable[Filter],
    default_table: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Group typed filters into the server's nested filter dicts.

    Parameters
    ----------
    filters :
        The typed filters to serialize.
    default_table :
        Table name used for any ``ColumnHandle`` whose ``table`` is ``None``
        (the query's primary source).

    Returns
    -------
    dict
        A mapping of wire key (e.g. ``"filter_in_dict"``) to a nested
        ``{table: {column: value}}`` dict. Only keys with at least one filter
        are present, so the result can be merged directly into a request body.
    """
    return _group_filters(filters, default_table, key=lambda op: op.payload_key)


def filters_to_method_kwargs(
    filters: Iterable[Filter],
    default_table: str,
    nested: bool,
) -> dict[str, Any]:
    """Group typed filters into keyword arguments for the existing query methods.

    Like :func:`filters_to_payload` but keyed by the methods' argument names
    (``filter_out_dict`` rather than the wire's ``filter_notin_dict``), and with
    a choice of nesting:

    * ``nested=True`` → ``{table: {column: value}}`` (for ``join_query`` /
      ``live_live_query``, which take nested filter dicts).
    * ``nested=False`` → ``{column: value}`` (for ``query_table`` /
      ``query_view``, which take flat dicts for a single table). Raises if the
      filters span more than one table.
    """
    grouped = _group_filters(filters, default_table, key=lambda op: op.kwarg_key)
    if nested:
        return grouped
    flat: dict[str, Any] = {}
    for kwarg, by_table in grouped.items():
        if len(by_table) > 1:
            raise ValueError(
                f"flat filter kwargs require a single table, but `{kwarg}` spans "
                f"{sorted(by_table)}"
            )
        ((_only_table, columns),) = by_table.items()
        flat[kwarg] = columns
    return flat


def filters_from_kwargs(by_kwarg: dict, default_table: str) -> tuple:
    """Build typed filters from the familiar filter-dict keyword arguments.

    The inverse of :func:`filters_to_method_kwargs`: given a mapping of method
    argument name (``"filter_in_dict"`` etc.) to its dict, produce a tuple of
    :class:`~caveclient.query.filters.Filter`. Columns get the ``UNTYPED`` kind
    (the user picked the operation by choosing the dict; per-op value-shape
    validation still runs).

    Both shapes are accepted, distinguished unambiguously because filter values
    are never dicts:

    * flat ``{column: value}`` → columns belong to ``default_table``;
    * nested ``{table: {column: value}}`` → columns belong to the named table.
    """
    filters = []
    for kwarg, d in by_kwarg.items():
        if d is None:
            continue
        op = _KWARG_TO_OP[kwarg]
        is_nested = bool(d) and all(isinstance(v, dict) for v in d.values())
        if is_nested:
            for table, columns in d.items():
                for col, value in columns.items():
                    filters.append(
                        Filter(
                            ColumnHandle(col, FilterKind.UNTYPED, table=table),
                            op,
                            value,
                        )
                    )
        else:
            for col, value in d.items():
                filters.append(Filter(ColumnHandle(col, FilterKind.UNTYPED), op, value))
    return tuple(filters)


def _group_filters(filters, default_table, key):
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for f in filters:
        table = f.column.table or default_table
        by_table = grouped.setdefault(key(f.op), {})
        by_column = by_table.setdefault(table, {})
        if f.column.name in by_column:
            raise ValueError(
                f"conflicting `{f.op.value}` filters for column "
                f"`{table}.{f.column.name}`"
            )
        by_column[f.column.name] = _serialize_value(f)
    return grouped
