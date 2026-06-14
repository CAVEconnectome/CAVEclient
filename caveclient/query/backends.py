"""Query backends and the switchboard that routes between them.

A backend answers two questions: *can you serve this spec?* (``can_handle``,
returning ``True`` or a human-readable reason it cannot) and *serve it*
(``execute``). The :class:`Switchboard` tries backends in order and dispatches
to the first that can handle the spec; if none can, it raises an error
assembled from every backend's reason, so the user sees why each option was
ruled out rather than a downstream server error.

In this phase the backends **delegate** to the existing ``MaterializationClient``
methods rather than owning the HTTP pipeline. Routing and the typed spec are
proven first; the execution internals can be inverted later without changing
this layer's contract.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Optional, Union

from packaging.version import Version

from .serialize import filters_to_method_kwargs, joins_to_quads
from .spec import QuerySpec

# live_live_query carries @_check_version_compatibility(method_constraint=">=5.13.0")
LIVE_QUERY_MIN_VERSION = Version("5.13.0")


@dataclass
class Capabilities:
    """What the server and the resolved source support, for routing decisions.

    Built from the live client at dispatch time; passed to ``can_handle`` so the
    routing logic stays pure and testable.
    """

    server_version: Optional[Version] = None
    has_chunkedgraph: bool = False
    # for a view source: whether the server advertises it as live-queryable
    source_live_compatible: bool = False
    deltalake_available: bool = False

    def supports_live_endpoint(self) -> bool:
        # If the server version is unknown, assume the modern endpoint and let
        # the method's own version-compatibility decorator raise if it is too old.
        return (
            self.server_version is None or self.server_version >= LIVE_QUERY_MIN_VERSION
        )


def _flat_select(spec: QuerySpec) -> Optional[list]:
    """The flat (single-table) select_columns list for the primary source."""
    if spec.select_columns is None:
        return None
    if isinstance(spec.select_columns, dict):
        return spec.select_columns.get(spec.source.name)
    return spec.select_columns


class QueryBackend(abc.ABC):
    """A strategy for serving a :class:`QuerySpec`."""

    name: str

    @abc.abstractmethod
    def can_handle(self, spec: QuerySpec, caps: Capabilities) -> Union[bool, str]:
        """Return ``True`` if this backend can serve ``spec``, else a reason string."""

    @abc.abstractmethod
    def execute(self, spec: QuerySpec, client) -> "object":
        """Serve ``spec`` using ``client`` (a ``MaterializationClient``)."""


class MaterializedBackend(QueryBackend):
    """Versioned (frozen) single-table queries, via ``query_table``.

    The fast path. Joins (including reference merges) are resolved upstream into
    explicit joins and routed to the live backend, so this backend only ever sees
    single-table, join-free queries.
    """

    name = "materialized"

    def can_handle(self, spec, caps):
        if spec.is_live:
            return "materialized backend serves versioned queries, not timestamps"
        if spec.source.kind not in ("table", "auto"):
            return f"materialized backend serves tables, not {spec.source.kind}s"
        if spec.source.is_join:
            return "joins are served by the live backend"
        return True

    def execute(self, spec, client):
        return client.query_table(
            spec.source.name,
            materialization_version=spec.at.version,
            select_columns=_flat_select(spec),
            offset=spec.offset,
            limit=spec.limit,
            split_positions=spec.output.split_positions,
            desired_resolution=spec.output.desired_resolution,
            metadata=spec.output.metadata,
            get_counts=spec.get_counts,
            random_sample=spec.random_sample,
            # references are resolved upstream into joins (which route live), so
            # there is never a reference to merge here.
            merge_reference=False,
            **filters_to_method_kwargs(spec.filters, spec.source.name, nested=False),
        )


class ViewBackend(QueryBackend):
    """Versioned (frozen) queries on a view, via ``query_view``."""

    name = "view"

    def can_handle(self, spec, caps):
        if spec.source.kind != "view":
            return "view backend serves views only"
        if spec.is_live:
            return "this view does not support timestamp queries on this server"
        return True

    def execute(self, spec, client):
        return client.query_view(
            spec.source.name,
            materialization_version=spec.at.version,
            select_columns=_flat_select(spec),
            offset=spec.offset,
            limit=spec.limit,
            split_positions=spec.output.split_positions,
            desired_resolution=spec.output.desired_resolution,
            metadata=spec.output.metadata,
            get_counts=spec.get_counts,
            random_sample=spec.random_sample,
            **filters_to_method_kwargs(spec.filters, spec.source.name, nested=False),
        )


class LiveBackend(QueryBackend):
    """Live and joined queries, via ``live_live_query``.

    Serves every timestamp query and every joined query. Versioned joins reach
    here already converted to the version's timestamp (which reproduces the
    frozen result), so this backend always runs against a timestamp. Joins —
    including reference merges — are resolved upstream into ``spec.source.joins``.
    """

    name = "live"

    def can_handle(self, spec, caps):
        if not spec.is_live:
            return "live backend serves timestamp queries only"
        if spec.source.kind == "view" and not caps.source_live_compatible:
            # Phase 2 (server) will flip this on via source_live_compatible.
            return "this view does not support timestamp queries on this server"
        if not caps.supports_live_endpoint():
            return (
                f"live queries require server >= {LIVE_QUERY_MIN_VERSION}, "
                f"server is {caps.server_version}"
            )
        if not caps.has_chunkedgraph:
            return "live queries require a chunkedgraph client"
        return True

    def execute(self, spec, client):
        return client.live_live_query(
            spec.source.name,
            timestamp=spec.at.timestamp,
            joins=joins_to_quads(spec.source.joins) if spec.source.joins else None,
            select_columns=spec.select_columns,
            offset=spec.offset,
            limit=spec.limit,
            split_positions=spec.output.split_positions,
            desired_resolution=spec.output.desired_resolution,
            metadata=spec.output.metadata,
            suffixes=spec.source.suffixes,
            random_sample=spec.random_sample,
            allow_missing_lookups=spec.allow_missing_lookups,
            allow_invalid_root_ids=spec.allow_invalid_root_ids,
            **filters_to_method_kwargs(spec.filters, spec.source.name, nested=True),
        )


class DeltalakeBackend(QueryBackend):
    """Placeholder for the Phase 3 deltalake backend; never handles yet."""

    name = "deltalake"

    def can_handle(self, spec, caps):
        if spec.source.kind == "dataset" and caps.deltalake_available:
            return "deltalake backend is not yet implemented"
        return "deltalake backend is not yet available"

    def execute(self, spec, client):  # pragma: no cover - never routed to yet
        raise NotImplementedError("deltalake backend is not yet implemented")


class UnroutableQueryError(Exception):
    """Raised when no backend can serve a query; carries each backend's reason."""


# Default routing order. The first backend whose can_handle passes wins.
DEFAULT_BACKENDS: tuple[QueryBackend, ...] = (
    MaterializedBackend(),
    ViewBackend(),
    LiveBackend(),
    DeltalakeBackend(),
)


class Switchboard:
    """Routes a :class:`QuerySpec` to the first capable backend."""

    def __init__(self, backends: tuple[QueryBackend, ...] = DEFAULT_BACKENDS):
        self._backends = backends

    def route(self, spec: QuerySpec, caps: Capabilities) -> QueryBackend:
        """Return the backend that will serve ``spec``, or raise.

        Raises
        ------
        UnroutableQueryError
            If no backend can handle the spec; the message lists each backend's
            reason for declining.
        """
        reasons = []
        for backend in self._backends:
            verdict = backend.can_handle(spec, caps)
            if verdict is True:
                return backend
            reasons.append(f"{backend.name}: {verdict}")
        raise UnroutableQueryError(
            "No query backend could serve this query:\n  " + "\n  ".join(reasons)
        )

    def execute(self, spec: QuerySpec, caps: Capabilities, client):
        """Route ``spec`` and execute it on ``client``."""
        return self.route(spec, caps).execute(spec, client)
