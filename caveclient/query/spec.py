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

from .filters import Filter
from .serialize import filters_to_payload

SourceKind = Literal["table", "view", "dataset", "auto"]


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
        Optional explicit joins, each ``[table_a, col_a, table_b, col_b]``.
    suffixes :
        Optional per-table column suffixes for disambiguating joined columns,
        ``{table_name: suffix}``.
    """

    name: str
    kind: SourceKind = "auto"
    joins: Optional[list] = None
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
