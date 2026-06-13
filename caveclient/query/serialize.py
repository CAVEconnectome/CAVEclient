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

from .filters import Filter
from .kinds import FilterOp


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
    payload: dict[str, dict[str, dict[str, Any]]] = {}
    for f in filters:
        table = f.column.table or default_table
        key = f.op.payload_key
        by_table = payload.setdefault(key, {})
        by_column = by_table.setdefault(table, {})
        if f.column.name in by_column:
            raise ValueError(
                f"conflicting `{f.op.value}` filters for column "
                f"`{table}.{f.column.name}`"
            )
        by_column[f.column.name] = _serialize_value(f)
    return payload
