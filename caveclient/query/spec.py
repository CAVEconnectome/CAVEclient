"""The normalized query specification consumed by the switchboard and backends.

A ``QuerySpec`` captures, in one place and in canonical form, everything a query
needs: what to query (``Source``), at what point in time or version (``At``),
the typed filters, column selection, paging, and output shaping
(``OutputOptions``). Backends translate a spec into a concrete request; the
switchboard routes on it.

Structural validation (schema-free) happens at construction. Schema-aware
validation against cached table/view metadata happens separately, just before
dispatch — see ``validate_against_schema``.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Literal, Optional, Sequence

from .filters import ColumnHandle, Filter
from .kinds import FilterKind, FilterOp
from .serialize import filters_from_kwargs, filters_to_payload

SourceKind = Literal["table", "view", "dataset", "auto"]

# Default per-table suffixes for joined tables when the caller doesn't set one,
# applied positionally. Underscore-prefixed to read like pandas merge defaults
# (``_x``/``_y``); the server's own fallback list omits the underscore.
DEFAULT_JOIN_SUFFIXES = ("_x", "_y", "_z", "_xx", "_yy", "_zz", "_xxx", "_yyy", "_zzz")


class InvalidQueryError(ValueError):
    """Raised when a query spec is structurally invalid."""


@dataclass(frozen=True)
class At:
    """A temporal address: a materialization version, a timestamp, or neither.

    At most one of ``version``/``timestamp`` may be set. Neither set means
    "the client's default version", resolved by the backend at dispatch time.
    """

    version: Optional[int] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.version is not None and self.timestamp is not None:
            raise InvalidQueryError(
                "cannot specify both a version and a timestamp; a query is "
                "addressed either by materialization version or by time, not both"
            )

    @property
    def is_live(self) -> bool:
        """Whether this query is addressed by timestamp (live) rather than version."""
        return self.timestamp is not None


@dataclass(frozen=True)
class Join:
    """A single join between two tables on a column each.

    A readable replacement for the server's two divergent join encodings
    (``join_query``'s ``[[table, col], ...]`` pairs and ``live_live_query``'s
    ``[[t1, c1, t2, c2], ...]`` quads); serialization to either form lives in
    ``caveclient.query.serialize``.
    """

    left_table: str
    left_column: str
    right_table: str
    right_column: str


@dataclass(frozen=True)
class Table:
    """A table's participation in a query: its join column, suffix, and filters.

    The ergonomic way to express a (possibly multi-table) query, gathering
    everything about one table into a single object so its name is written once.
    Pass one ``Table``, or a list of them to ``query()``; adjacent tables in the
    list are joined on their respective ``join_on`` columns.

    Examples
    --------
    >>> client.materialize.query([
    ...     Table("synapses", "post_pt_root_id", suffix="", filter_greater={"size": 100}),
    ...     Table("nuclei", "pt_root_id", suffix="_nuc"),
    ... ])

    Parameters
    ----------
    name :
        Table name.
    join_on :
        Column on this table used to join to the adjacent table(s) in the query.
        Required for every table when more than one is given.
    suffix :
        Suffix appended to this table's columns to disambiguate joins.
    select :
        Columns to return from this table (defaults to all).
    merge_reference :
        If True (default) and this is a single-table query, auto-join this
        table's reference table; its columns appear suffixed with ``_ref``. Has
        no effect when explicit joins are given (a multi-table query).
    filter_in, filter_out, filter_equal, filter_greater, filter_less, filter_greater_equal, filter_less_equal, filter_spatial, filter_regex :
        Flat ``{column: value}`` filter dicts scoped to this table.
    """

    name: str
    join_on: Optional[str] = None
    suffix: Optional[str] = None
    select: Optional[Sequence[str]] = None
    merge_reference: bool = True
    kind: str = (
        "table"  # "table" or "view"; the primary table's kind sets the source kind
    )
    filter_in: Optional[dict] = None
    filter_out: Optional[dict] = None
    filter_equal: Optional[dict] = None
    filter_greater: Optional[dict] = None
    filter_less: Optional[dict] = None
    filter_greater_equal: Optional[dict] = None
    filter_less_equal: Optional[dict] = None
    filter_spatial: Optional[dict] = None
    filter_regex: Optional[dict] = None

    def _filter_dicts(self) -> dict:
        """Map this table's set filter dicts to their FilterOp."""
        return {
            op: getattr(self, attr)
            for attr, op in _TABLE_FILTER_FIELDS.items()
            if getattr(self, attr)
        }


# Table filter-field name -> FilterOp. The field names are the query() kwarg
# names without the redundant `_dict` suffix (it is implied on a table object).
_TABLE_FILTER_FIELDS = {op.field_key: op for op in FilterOp}


@dataclass(frozen=True)
class Source:
    """What is being queried: a table, a view, or an external dataset.

    Parameters
    ----------
    name :
        Table, view, or dataset name.
    kind :
        ``"table"``, ``"view"``, ``"dataset"`` (deltalake), or ``"auto"`` to let
        the switchboard resolve it from metadata.
    joins :
        Optional explicit joins, as a tuple of :class:`Join`.
    suffixes :
        Optional per-table column suffixes for disambiguating joined columns,
        ``{table_name: suffix}``.
    """

    name: str
    kind: SourceKind = "auto"
    joins: Optional[tuple] = None
    suffixes: Optional[dict] = None

    @property
    def is_join(self) -> bool:
        return bool(self.joins)


@dataclass(frozen=True)
class OutputOptions:
    """How to shape the returned dataframe (independent of where it came from)."""

    split_positions: bool = False
    desired_resolution: Optional[Sequence[float]] = None
    metadata: bool = True


@dataclass(frozen=True)
class QuerySpec:
    """A complete, normalized query.

    Filters are held as typed ``Filter`` objects; ``select_columns`` is the
    canonical nested ``{table: [columns]}`` form (flat input is wrapped with the
    source name at the construction boundary).
    """

    source: Source
    at: At = field(default_factory=At)
    filters: tuple[Filter, ...] = ()
    select_columns: Optional[dict] = None
    offset: Optional[int] = None
    limit: Optional[int] = None
    random_sample: Optional[int] = None
    get_counts: bool = False
    # auto-join the primary table's reference table (table-query behavior)
    merge_reference: bool = True
    # live-query behavior; no-ops for versioned (frozen) queries
    allow_missing_lookups: bool = False
    allow_invalid_root_ids: bool = False
    output: OutputOptions = field(default_factory=OutputOptions)

    def __post_init__(self) -> None:
        if self.limit is not None and self.limit <= 0:
            raise InvalidQueryError(
                f"limit must be a positive integer, got {self.limit}"
            )
        if self.offset is not None and self.offset < 0:
            raise InvalidQueryError(f"offset must be non-negative, got {self.offset}")
        if self.random_sample is not None and self.random_sample <= 0:
            raise InvalidQueryError(
                f"random_sample must be a positive integer, got {self.random_sample}"
            )

    @property
    def is_live(self) -> bool:
        return self.at.is_live

    def filter_payload(self) -> dict:
        """Serialize this spec's filters to the server's nested filter dicts."""
        return filters_to_payload(self.filters, default_table=self.source.name)

    def validate_against_schema(self, column_kinds: dict) -> list[str]:
        """Check filters against known columns; return a list of problems.

        Parameters
        ----------
        column_kinds :
            Mapping of column name to ``FilterKind`` for the source's columns,
            derived from cached table/view metadata. A filter referencing an
            unknown column, or one whose declared kind disagrees with the
            filter's column handle, is reported.

        Returns
        -------
        list of str
            Human-readable problems. Empty if the spec is consistent with the
            schema. Callers collect and raise these together so a user sees all
            issues at once rather than one per round-trip.
        """
        problems: list[str] = []
        for f in self.filters:
            # only schema-check columns belonging to the primary source; joined
            # tables are validated against their own metadata by the caller
            if f.column.table not in (None, self.source.name):
                continue
            known_kind = column_kinds.get(f.column.name)
            if known_kind is None:
                problems.append(
                    f"column `{f.column.name}` not found in `{self.source.name}`"
                )
            elif known_kind is not f.column.kind:
                problems.append(
                    f"column `{f.column.name}` is `{known_kind.value}` but the "
                    f"filter treats it as `{f.column.kind.value}`"
                )
        return problems


def build_query_spec(
    source: str,
    *,
    kind: SourceKind = "auto",
    version: Optional[int] = None,
    timestamp: Optional[datetime] = None,
    filters_by_kwarg: Optional[dict] = None,
    select_columns=None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    random_sample: Optional[int] = None,
    get_counts: bool = False,
    split_positions: bool = False,
    desired_resolution: Optional[Sequence[float]] = None,
    metadata: bool = True,
    merge_reference: bool = True,
    allow_missing_lookups: bool = False,
    allow_invalid_root_ids: bool = False,
) -> QuerySpec:
    """Build a :class:`QuerySpec` from the familiar filter-dict keyword style.

    ``filters_by_kwarg`` maps argument names (``"filter_in_dict"`` etc.) to their
    dicts; they are converted to typed filters (see
    :func:`~caveclient.query.serialize.filters_from_kwargs`). Structural
    validation runs as the spec and its filters are constructed.
    """
    filters = filters_from_kwargs(filters_by_kwarg or {}, source)
    return QuerySpec(
        source=Source(source, kind=kind),
        at=At(version=version, timestamp=timestamp),
        filters=filters,
        select_columns=select_columns,
        offset=offset,
        limit=limit,
        random_sample=random_sample,
        get_counts=get_counts,
        merge_reference=merge_reference,
        allow_missing_lookups=allow_missing_lookups,
        allow_invalid_root_ids=allow_invalid_root_ids,
        output=OutputOptions(
            split_positions=split_positions,
            desired_resolution=desired_resolution,
            metadata=metadata,
        ),
    )


def build_query_spec_from_tables(
    tables,
    *,
    version: Optional[int] = None,
    timestamp: Optional[datetime] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    random_sample: Optional[int] = None,
    get_counts: bool = False,
    split_positions: bool = False,
    desired_resolution: Optional[Sequence[float]] = None,
    metadata: bool = True,
    allow_missing_lookups: bool = False,
    allow_invalid_root_ids: bool = False,
) -> QuerySpec:
    """Build a :class:`QuerySpec` from one or more :class:`Table` objects.

    The first table is the primary source. Adjacent tables are joined on their
    ``join_on`` columns; per-table suffixes, selected columns, filters, and the
    ``merge_reference`` flag are gathered into the spec.
    """
    tables = list(tables)
    if not tables:
        raise InvalidQueryError("at least one Table is required")
    if len(tables) > 1 and any(t.join_on is None for t in tables):
        missing = [t.name for t in tables if t.join_on is None]
        raise InvalidQueryError(
            f"every table in a multi-table query must set join_on; missing on: {missing}"
        )

    joins = tuple(
        Join(
            tables[i].name, tables[i].join_on, tables[i + 1].name, tables[i + 1].join_on
        )
        for i in range(len(tables) - 1)
    )
    if len(tables) > 1:
        # joined tables get a suffix so duplicate columns don't collide; default
        # to pandas-style _x/_y/... so the server's underscore-less fallback
        # ("x"/"y") is never reached.
        suffixes = {
            t.name: t.suffix if t.suffix is not None else DEFAULT_JOIN_SUFFIXES[i]
            for i, t in enumerate(tables)
        }
    else:
        suffixes = {t.name: t.suffix for t in tables if t.suffix is not None}
    select_columns = {t.name: list(t.select) for t in tables if t.select}
    filters = []
    for t in tables:
        for op, d in t._filter_dicts().items():
            for col, value in d.items():
                filters.append(
                    Filter(
                        ColumnHandle(col, FilterKind.UNTYPED, table=t.name), op, value
                    )
                )

    return QuerySpec(
        source=Source(
            tables[0].name,
            kind=tables[0].kind,
            joins=joins or None,
            suffixes=suffixes or None,
        ),
        at=At(version=version, timestamp=timestamp),
        filters=tuple(filters),
        select_columns=select_columns or None,
        offset=offset,
        limit=limit,
        random_sample=random_sample,
        get_counts=get_counts,
        # per-table merge_reference is honored by query() (which resolves
        # references into deduped joins), not via this spec-level flag.
        allow_missing_lookups=allow_missing_lookups,
        allow_invalid_root_ids=allow_invalid_root_ids,
        output=OutputOptions(
            split_positions=split_positions,
            desired_resolution=desired_resolution,
            metadata=metadata,
        ),
    )


def build_query_spec_from_edges(
    edges,
    *,
    version: Optional[int] = None,
    timestamp: Optional[datetime] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    random_sample: Optional[int] = None,
    get_counts: bool = False,
    split_positions: bool = False,
    desired_resolution: Optional[Sequence[float]] = None,
    metadata: bool = True,
    allow_missing_lookups: bool = False,
    allow_invalid_root_ids: bool = False,
) -> tuple["QuerySpec", dict]:
    """Build a :class:`QuerySpec` for a general join **graph** from an edge list.

    Each edge is a ``[left, right]`` pair of :class:`Table` objects denoting one
    pairwise join ``left.join_on == right.join_on``. A table may appear in several
    edges (with a different ``join_on`` each time) to express a star or any
    non-chain topology. ``join_on`` is per-edge; a table's filters/``select``/
    ``suffix`` are per-table identity and must be set on its **first** appearance
    — a later appearance may set only ``join_on``, else this raises.

    Returns the spec and the ``{name: Table}`` of first appearances (for the
    caller's reference-merge / source-kind handling).
    """
    edges = [list(e) for e in edges]
    if not edges:
        raise InvalidQueryError("at least one join edge is required")

    first_seen: dict = {}
    joins = []
    for i, edge in enumerate(edges):
        if len(edge) != 2:
            raise InvalidQueryError(
                f"each join edge must be a [left, right] pair of Tables; "
                f"edge {i} has {len(edge)}"
            )
        left, right = edge
        for t in (left, right):
            if not isinstance(t, Table):
                raise InvalidQueryError(
                    f"join edges must contain Table objects; edge {i} has a "
                    f"{type(t).__name__}"
                )
            if t.join_on is None:
                raise InvalidQueryError(
                    f"every table in a join edge must set join_on; '{t.name}' "
                    f"in edge {i} does not"
                )
        joins.append(Join(left.name, left.join_on, right.name, right.join_on))
        for t in (left, right):
            if t.name not in first_seen:
                first_seen[t.name] = t
            elif t._filter_dicts() or t.select is not None or t.suffix is not None:
                # a table is one node; its attributes belong on its first edge
                raise InvalidQueryError(
                    f"table '{t.name}' appears more than once in the join list; set "
                    f"its filters/select/suffix on its first appearance only — a "
                    f"later appearance may set only join_on"
                )

    distinct = list(first_seen)
    suffixes = {
        name: (
            first_seen[name].suffix
            if first_seen[name].suffix is not None
            else DEFAULT_JOIN_SUFFIXES[i]
        )
        for i, name in enumerate(distinct)
    }
    select_columns = {
        name: list(t.select) for name, t in first_seen.items() if t.select
    }
    filters = []
    for name, t in first_seen.items():
        for op, d in t._filter_dicts().items():
            for col, value in d.items():
                filters.append(
                    Filter(ColumnHandle(col, FilterKind.UNTYPED, table=name), op, value)
                )

    anchor = edges[0][0]
    spec = QuerySpec(
        source=Source(
            anchor.name,
            kind=anchor.kind,
            joins=tuple(joins),
            suffixes=suffixes or None,
        ),
        at=At(version=version, timestamp=timestamp),
        filters=tuple(filters),
        select_columns=select_columns or None,
        offset=offset,
        limit=limit,
        random_sample=random_sample,
        get_counts=get_counts,
        allow_missing_lookups=allow_missing_lookups,
        allow_invalid_root_ids=allow_invalid_root_ids,
        output=OutputOptions(
            split_positions=split_positions,
            desired_resolution=desired_resolution,
            metadata=metadata,
        ),
    )
    return spec, first_seen


def resolve_version_fallback(
    spec: QuerySpec,
    available_versions: Collection[int],
    timestamp_lookup: Callable[[int], Optional[datetime]],
) -> tuple[QuerySpec, bool]:
    """Fall a stale version-pinned query back to an equivalent timestamp query.

    Materialization versions churn: a pinned ``version=N`` may name a version
    whose database has since been deleted, even though its metadata row (and
    thus its timestamp) survives. Because a live query at a version's exact
    timestamp reconstructs equivalent data from the nearest surviving version,
    we can silently degrade rather than fail.

    Parameters
    ----------
    spec :
        The query, possibly addressed by version.
    available_versions :
        Versions whose materialized databases currently exist and are queryable
        (e.g. ``client.materialize.get_versions(expired=False)``).
    timestamp_lookup :
        Resolves a version to its frozen timestamp, working even for expired
        versions (``client.materialize.get_timestamp``). Should return ``None``
        if the version is wholly unknown (no metadata row), in which case no
        fallback is possible and the original spec is returned unchanged so the
        natural error surfaces downstream.

    Returns
    -------
    (QuerySpec, bool)
        The spec to dispatch (with ``At`` rewritten to a timestamp if a fallback
        occurred) and whether a fallback occurred (so the caller can warn).
    """
    at = spec.at
    if at.version is None or at.version in available_versions:
        return spec, False
    ts = timestamp_lookup(at.version)
    if ts is None:
        return spec, False
    return replace(spec, at=At(timestamp=ts)), True
