"""Process-wide caching of server-scoped metadata.

Constructing a `CAVEclient` normally asks each service what version it is running
and which API versions it speaks. Those answers are properties of the *server*,
not of the caller, so a process that builds many clients against the same
deployment -- a web application building one client per request, for instance --
repeats identical lookups. This module caches them behind one policy.

Caching is **opt-in**; with it disabled (the default) the cache is entirely out
of the request path and behaviour is unchanged. Turn it on once at startup:

    from caveclient import set_server_cache_defaults

    set_server_cache_defaults(enabled=True)

Entries are deliberately shared across auth tokens, because the values they hold
describe the deployment rather than the user. See `set_server_cache_defaults`
for the one exception to that and how to opt out of it.
"""

import copy
import hashlib
import logging
import os
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple

from cachetools import TTLCache, cached

__all__ = [
    "set_server_cache_defaults",
    "get_server_cache_defaults",
    "invalidate",
    "refresh",
]

logger = logging.getLogger(__name__)

# Cache categories. Server-scoped entries are shared by every datastack served
# from the same address; datastack-scoped ones are not.
CATEGORY_API_VERSIONS = "api_versions"
CATEGORY_SERVER_VERSION = "server_version"
CATEGORY_INFO = "info"
CATEGORY_MAT_VERSIONS = "mat_versions"
CATEGORY_MAT_VERSION_METADATA = "mat_version_metadata"

CATEGORIES = (
    CATEGORY_API_VERSIONS,
    CATEGORY_SERVER_VERSION,
    CATEGORY_INFO,
    CATEGORY_MAT_VERSIONS,
    CATEGORY_MAT_VERSION_METADATA,
)

#: Categories whose value depends only on the server address.
SERVER_SCOPED = frozenset({CATEGORY_API_VERSIONS, CATEGORY_SERVER_VERSION})

#: Categories whose cached payload is mutable and must be copied in and out.
_COPY_CATEGORIES = frozenset(
    {CATEGORY_INFO, CATEGORY_MAT_VERSIONS, CATEGORY_MAT_VERSION_METADATA}
)

_TTL_CONFIG_KEY = {
    CATEGORY_API_VERSIONS: "ttl_api_versions",
    CATEGORY_SERVER_VERSION: "ttl_server_version",
    CATEGORY_INFO: "ttl_info",
    CATEGORY_MAT_VERSIONS: "ttl_mat_versions",
    CATEGORY_MAT_VERSION_METADATA: "ttl_mat_version_metadata",
}

SERVER_CACHE_DEFAULTS = {}

# Bumped on every configuration change so registries can rebuild their caches.
_CONFIG_EPOCH = [0]


@dataclass(frozen=True)
class MetaKey:
    """Identifies one cached lookup.

    A structured key rather than an opaque tuple, so that `invalidate` can select
    entries by field -- server-scoped entries do not mention a datastack, and
    there is otherwise no way to find them from one.
    """

    category: str
    server_address: str
    datastack: Optional[str] = None
    extra: Tuple = field(default=())


def set_server_cache_defaults(
    enabled: bool = False,
    ttl_api_versions: Optional[float] = 86_400,
    ttl_server_version: Optional[float] = 3_600,
    ttl_info: Optional[float] = 3_600,
    ttl_mat_versions: Optional[float] = 3_600,
    ttl_mat_version_metadata: Optional[float] = 43_200,
    maxsize: int = 512,
    metadata_timeout: Optional[float] = 30.0,
    token_scoped_info: bool = False,
    eager_server_version: bool = False,
) -> None:
    """Configure process-wide caching of server metadata.

    Should be done prior to initializing a client. Calling this function with any
    argument missing resets that value to its default, so a bare call restores
    the original configuration.

    Parameters
    ----------
    enabled :
        Whether to cache server metadata at all. Off by default; when off, no
        lookup consults the cache and behaviour is exactly as it was before this
        feature existed.
    ttl_api_versions :
        Seconds to cache the set of API versions a deployment speaks.
    ttl_server_version :
        Seconds to cache the software version a service reports.
    ttl_info :
        Seconds to cache a datastack's info-service payload.
    ttl_mat_versions :
        Seconds to cache the list of available materialization versions. Kept
        short by default, since new versions appear as they are materialized.
    ttl_mat_version_metadata :
        Seconds to cache the metadata for one materialization version.
    maxsize :
        Maximum entries per category before least-recently-used eviction. Scale
        this up if the process serves many distinct datastacks or users.
    metadata_timeout :
        Seconds before a metadata request gives up. Guards against a server that
        accepts connections but never answers, which would otherwise block
        client construction indefinitely.
    token_scoped_info :
        Whether to key the info payload by auth token as well as by datastack.
        Off by default, matching long-standing behaviour: one lookup is shared by
        every user in the process. Turn it on when tokens in a single process
        have *different* permissions on the same datastack, where a shared entry
        would let one user see a payload fetched with another user's token.
    eager_server_version :
        Whether to fetch a service's version during client construction rather
        than on first use. Off by default. Turn it on to restore the previous
        behaviour of failing fast at construction when a server is unreachable.

    Notes
    -----
    A TTL of 0 or None disables storage for that category while still collapsing
    concurrent duplicate lookups into one request.

    See Also
    --------
    [caveclient.get_server_cache_defaults][]
    """
    SERVER_CACHE_DEFAULTS.update(
        enabled=enabled,
        ttl_api_versions=ttl_api_versions,
        ttl_server_version=ttl_server_version,
        ttl_info=ttl_info,
        ttl_mat_versions=ttl_mat_versions,
        ttl_mat_version_metadata=ttl_mat_version_metadata,
        maxsize=maxsize,
        metadata_timeout=metadata_timeout,
        token_scoped_info=token_scoped_info,
        eager_server_version=eager_server_version,
    )
    _CONFIG_EPOCH[0] += 1


set_server_cache_defaults()


def get_server_cache_defaults() -> dict:
    """Get the current configuration for server metadata caching.

    Returns
    -------
    :
        Dictionary of current default values.

    See Also
    --------
    [caveclient.set_server_cache_defaults][]
    """
    return SERVER_CACHE_DEFAULTS


class ServerMetadataRegistry:
    """A thread-safe, TTL-bounded store of server-scoped metadata.

    Concurrent lookups of the same cold key issue a single upstream request; the
    others wait for its result. Failed lookups are never stored, so a momentary
    outage cannot pin a degraded answer.

    Parameters
    ----------
    config :
        Configuration mapping. Defaults to the process-wide configuration set by
        [caveclient.set_server_cache_defaults][].
    timer :
        Callable returning a monotonically increasing time, used for expiry.
        Injectable so that tests can advance a clock instead of sleeping.
    """

    def __init__(self, config=None, timer=None):
        self._config_override = config
        self._timer = timer
        self._epoch = None
        self._caches = {}
        self._locks = {}
        self._getters = {}
        self._index = defaultdict(set)
        self._index_lock = threading.RLock()
        self._stats = Counter()
        self._build_lock = threading.RLock()
        self._ensure_caches()

    def __copy__(self):
        # The registry is shared process infrastructure holding locks, so a
        # copied client must reference the same one rather than clone it.
        return self

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    # -- configuration ----------------------------------------------------

    @property
    def config(self) -> dict:
        if self._config_override is not None:
            return self._config_override
        return SERVER_CACHE_DEFAULTS

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("enabled", False))

    @property
    def metadata_timeout(self) -> Optional[float]:
        return self.config.get("metadata_timeout")

    def _ttl_for(self, category: str) -> Optional[float]:
        return self.config.get(_TTL_CONFIG_KEY[category])

    def _ensure_caches(self) -> None:
        """(Re)build per-category caches when the configuration has changed."""
        epoch = _CONFIG_EPOCH[0] if self._config_override is None else id(self.config)
        if self._epoch == epoch:
            return
        with self._build_lock:
            if self._epoch == epoch:
                return
            maxsize = int(self.config.get("maxsize", 512))
            for category in CATEGORIES:
                ttl = self._ttl_for(category)
                cache_kwargs = {"maxsize": maxsize, "ttl": ttl or 0}
                if self._timer is not None:
                    cache_kwargs["timer"] = self._timer
                cache = TTLCache(**cache_kwargs)
                lock = threading.RLock()
                condition = threading.Condition(lock)

                # `condition` is what makes concurrent misses collapse into a
                # single upstream call; `lock` alone does not. It requires
                # cachetools >= 6.0.0, which the package pins.
                @cached(
                    cache, key=lambda key, fetch: key, lock=lock, condition=condition
                )
                def _get(key, fetch):
                    return fetch()

                self._caches[category] = cache
                self._locks[category] = lock
                self._getters[category] = _get
            self._epoch = epoch

    # -- core lookup ------------------------------------------------------

    def get_or_fetch(
        self,
        key: MetaKey,
        fetch: Callable[[], Any],
        enabled: Optional[bool] = None,
    ) -> Any:
        """Return a cached value for `key`, calling `fetch` only if needed.

        `fetch` must raise on failure rather than returning a degraded value:
        exceptions are never cached, which is what keeps a transient outage from
        pinning a fallback. A definite answer that happens to be `None` -- a 404
        from a server with no version endpoint, say -- is cached normally.
        """
        if enabled is None:
            enabled = self.enabled
        if not enabled:
            # Structurally out of the path: no lock, no bookkeeping, no storage.
            return fetch()

        self._ensure_caches()
        cache = self._caches[key.category]
        lock = self._locks[key.category]

        with lock:
            hit = key in cache
        self._stats["hits" if hit else "misses"] += 1
        if not hit:
            self._stats["fetches"] += 1

        value = self._getters[key.category](key, fetch)

        if key.datastack is not None:
            self.note_datastack(key.datastack, key.server_address)
        if key.category in _COPY_CATEGORIES:
            return copy.deepcopy(value)
        return value

    def refresh(self, key: MetaKey, fetch: Callable[[], Any]) -> Any:
        """Force a refetch of one key, replacing any cached value.

        A failed refresh leaves the existing value in place.
        """
        value = fetch()
        self._ensure_caches()
        ttl = self._ttl_for(key.category)
        if self.enabled and ttl:
            with self._locks[key.category]:
                self._caches[key.category][key] = value
        if key.category in _COPY_CATEGORIES:
            return copy.deepcopy(value)
        return value

    # -- bookkeeping ------------------------------------------------------

    def note_datastack(self, datastack_name: str, server_address: str) -> None:
        """Record that a datastack is served from an address.

        Lets `invalidate(datastack=...)` also drop the server-scoped entries for
        that deployment, which do not themselves mention the datastack.
        """
        if not datastack_name or not server_address:
            return
        with self._index_lock:
            self._index[datastack_name].add(server_address)

    def invalidate(
        self,
        datastack: Optional[str] = None,
        server_address: Optional[str] = None,
        category: Optional[str] = None,
    ) -> int:
        """Drop cached entries, forcing the next lookup to go upstream.

        With no arguments, drops everything. Narrow the scope with `datastack`,
        `server_address`, or `category`. Note that `datastack` also drops the
        server-scoped entries for every address known to serve it, which may
        affect sibling datastacks on the same deployment; the cost of that is one
        extra lookup each.

        Returns
        -------
        :
            Number of entries removed.
        """
        self._ensure_caches()
        addresses = set()
        if datastack is not None:
            with self._index_lock:
                addresses = set(self._index.get(datastack, ()))
        if server_address is not None:
            addresses.add(server_address)

        removed = 0
        for cat in CATEGORIES:
            if category is not None and cat != category:
                continue
            lock = self._locks[cat]
            cache = self._caches[cat]
            with lock:
                for key in list(cache.keys()):
                    if not self._matches(key, datastack, server_address, addresses):
                        continue
                    try:
                        del cache[key]
                    except KeyError:  # expired between listing and deleting
                        continue
                    removed += 1
        if datastack is not None and server_address is None:
            with self._index_lock:
                self._index.pop(datastack, None)
        self._stats["invalidations"] += removed
        return removed

    @staticmethod
    def _matches(key, datastack, server_address, addresses) -> bool:
        if datastack is None and server_address is None:
            return True
        if datastack is not None and key.datastack == datastack:
            return True
        if server_address is not None and key.server_address == server_address:
            return True
        # Server-scoped entries carry no datastack; match them via the reverse
        # index of addresses known to serve the named datastack.
        if key.datastack is None and key.server_address in addresses:
            return True
        return False

    def clear(self) -> None:
        """Drop every entry and reset statistics."""
        self._ensure_caches()
        for cat in CATEGORIES:
            with self._locks[cat]:
                self._caches[cat].clear()
        with self._index_lock:
            self._index.clear()
        self._stats.clear()

    def stats(self) -> dict:
        """Cache counters, for tests and diagnostics."""
        self._ensure_caches()
        out = dict(self._stats)
        out["enabled"] = self.enabled
        out["size"] = sum(len(self._caches[c]) for c in CATEGORIES)
        return out


# Per-process salt: fingerprints must not be stable across processes, and the
# token itself must never be recoverable from a key.
_SALT = os.urandom(16)


def token_fingerprint(headers) -> str:
    """An opaque, non-reversible stand-in for an auth token, safe to use in a key."""
    value = (headers or {}).get("Authorization")
    if not value:
        return "anonymous"
    return hashlib.sha256(_SALT + value.encode("utf-8")).hexdigest()[:16]


_REGISTRY = ServerMetadataRegistry()


def get_registry() -> ServerMetadataRegistry:
    """The process-wide registry."""
    return _REGISTRY


def set_registry(registry: Optional[ServerMetadataRegistry]) -> ServerMetadataRegistry:
    """Install a registry as the process default, returning the previous one.

    Passing None restores the built-in registry.
    """
    global _REGISTRY
    previous = _REGISTRY
    _REGISTRY = registry if registry is not None else ServerMetadataRegistry()
    return previous


def resolve(server_cache=None, registry=None):
    """Resolve a per-client `server_cache` argument to (registry, enabled).

    `server_cache` may be None (follow the process-wide setting), a bool (force
    on or off for this client), or a registry instance to use directly.
    """
    if isinstance(server_cache, ServerMetadataRegistry):
        return server_cache, True
    reg = registry if registry is not None else get_registry()
    return reg, server_cache


def invalidate(
    datastack: Optional[str] = None,
    server_address: Optional[str] = None,
    category: Optional[str] = None,
) -> int:
    """Drop cached server metadata so the next lookup goes upstream.

    Use this after a stale-metadata error to force a cold rebuild and retry,
    rather than waiting out the time-to-live.

    Parameters
    ----------
    datastack :
        Limit to entries for this datastack, including the server-scoped entries
        of the deployments known to serve it.
    server_address :
        Limit to entries for this server.
    category :
        Limit to one kind of metadata.

    Returns
    -------
    :
        Number of entries removed.
    """
    return get_registry().invalidate(
        datastack=datastack, server_address=server_address, category=category
    )


def refresh(client=None, datastack=None, server_address=None, category=None) -> int:
    """Drop cached metadata and, given a client, immediately re-prime it.

    Invalidation alone cannot refetch, because the registry holds no credentials.
    Passing a client lets the versions of its already-constructed sub-clients be
    fetched again straight away, using that client's sessions and token.

    Parameters
    ----------
    client :
        A `CAVEclient` whose already-built sub-clients should be re-primed.
    datastack :
        Limit to entries for this datastack.
    server_address :
        Limit to entries for this server.
    category :
        Limit to one kind of metadata.

    Returns
    -------
    :
        Number of entries removed.
    """
    if datastack is None and client is not None:
        datastack = getattr(client, "datastack_name", None)
    removed = invalidate(
        datastack=datastack, server_address=server_address, category=category
    )
    if client is not None:
        for name in (
            "info",
            "chunkedgraph",
            "materialize",
            "skeleton",
            "state",
            "schema",
            "annotation",
            "l2cache",
        ):
            sub = getattr(client, "_" + name, None)
            if sub is None:
                continue
            try:
                sub.reset_server_version()
            except Exception:  # pragma: no cover - re-priming is best effort
                logger.debug("could not re-prime %s server version", name)
    return removed


#: Caches owned by other modules that should be dropped alongside the registry.
_AUXILIARY_CACHES = []


def register_auxiliary_cache(cache) -> None:
    """Register a module-level cache so that `reset_server_cache` clears it too.

    Method-level caches keyed independently of the client instance are shared
    across clients by design; registering them keeps a single reset hook honest.
    """
    _AUXILIARY_CACHES.append(cache)


def _rebuild_after_fork() -> None:
    """Re-create locks in a forked child.

    A child inherits the parent's cached values, which are still valid, but any
    lock held at the moment of the fork is inherited locked and would deadlock.
    Rebuilding the caches discards those locks. Relevant to pre-fork servers that
    warm the cache in the master process, such as gunicorn with `--preload`.
    """
    registry = get_registry()
    # Snapshot before rebuilding: the values are still good, only the locks
    # guarding them are unusable.
    surviving = {}
    for category in CATEGORIES:
        cache = registry._caches.get(category)
        if cache is None:
            continue
        try:
            surviving[category] = list(cache.items())
        except Exception:  # pragma: no cover - a cache mid-mutation at fork
            surviving[category] = []
    index = {k: set(v) for k, v in registry._index.items()}

    registry._epoch = None
    registry._index_lock = threading.RLock()
    registry._build_lock = threading.RLock()
    registry._ensure_caches()

    for category, items in surviving.items():
        cache = registry._caches[category]
        for key, value in items:
            cache[key] = value
    registry._index = defaultdict(set, index)


if hasattr(os, "register_at_fork"):  # pragma: no cover - platform dependent
    os.register_at_fork(after_in_child=_rebuild_after_fork)


def reset_server_cache() -> None:
    """Clear all cached server metadata and restore the default configuration.

    Intended for tests and for callers that need a guaranteed cold start.
    """
    set_server_cache_defaults()
    get_registry().clear()
    for cache in _AUXILIARY_CACHES:
        try:
            cache.clear()
        except Exception:  # pragma: no cover - a cache that cannot clear
            logger.debug("could not clear auxiliary cache %r", cache)
