"""Process-wide caching of server-scoped metadata.

Constructing a `CAVEclient` normally asks each service what version it is running
and which API versions it speaks, and fetches the datastack's info record. Those
answers are properties of the *server*, not of the caller, so a process that
builds many clients against the same deployment -- a service endpoint building
one client per request, or a worker building one per task -- repeats identical
lookups. This module caches them behind one policy.

Caching of the lookups made while *building* a client is opt-in; with it
disabled (the default) they are entirely out of the request path. Turn it on
once at startup:

    from caveclient import enable_server_cache

    enable_server_cache()

Materialization table and version metadata have always been cached for twelve
hours and continue to be regardless of that switch; they are simply stored here
now, so one reset and one token policy cover everything.

Entries are deliberately shared across auth tokens, because the values they hold
describe the deployment rather than the user. That sharing has a security
consequence: a cached answer is served to any caller in the process, including
one whose own token would have been refused by the server. See
`enable_server_cache` for what that exposes and how to opt out of it.
"""

import copy
import hashlib
import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Tuple

from cachetools import TTLCache, cached

__all__ = [
    "enable_server_cache",
    "disable_server_cache",
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
CATEGORY_MAT_VERSIONS_METADATA = "mat_versions_metadata"
CATEGORY_TABLE_METADATA = "table_metadata"
CATEGORY_TABLES_METADATA = "tables_metadata"

CATEGORIES = (
    CATEGORY_API_VERSIONS,
    CATEGORY_SERVER_VERSION,
    CATEGORY_INFO,
    CATEGORY_MAT_VERSIONS,
    CATEGORY_MAT_VERSION_METADATA,
    CATEGORY_MAT_VERSIONS_METADATA,
    CATEGORY_TABLE_METADATA,
    CATEGORY_TABLES_METADATA,
)

#: Categories whose value depends only on the server address. Returned as-is;
#: everything else is a mutable payload and is copied out of the cache.
SERVER_SCOPED = frozenset({CATEGORY_API_VERSIONS, CATEGORY_SERVER_VERSION})

#: Categories cached regardless of `enabled`. These were cached before the
#: registry existed, and a query issues one of these lookups per call.
ALWAYS_ON = frozenset(
    {
        CATEGORY_MAT_VERSIONS_METADATA,
        CATEGORY_TABLE_METADATA,
        CATEGORY_TABLES_METADATA,
    }
)

#: Categories that use `ttl_fast`: the list of materialization versions changes
#: whenever a new version is materialized. Everything else uses `ttl_slow`.
FAST_CHANGING = frozenset({CATEGORY_MAT_VERSIONS})

#: Seconds before a metadata request made while building a client gives up.
#: Guards against a server that accepts connections but never answers, which
#: would otherwise block client construction indefinitely.
METADATA_TIMEOUT = 30.0

_DEFAULT_SETTINGS = {
    "enabled": False,
    "ttl_slow": 43_200,
    "ttl_fast": 600,
    "token_scoped": False,
    "maxsize": 512,
}
_settings = dict(_DEFAULT_SETTINGS)


@dataclass(frozen=True)
class MetaKey:
    """Identifies one cached lookup."""

    category: str
    server_address: str
    datastack: Optional[str] = None
    extra: Tuple = field(default=())


def enable_server_cache(
    ttl_slow: Optional[float] = 43_200,
    ttl_fast: Optional[float] = 600,
    token_scoped: bool = False,
    maxsize: int = 512,
) -> None:
    """Turn on process-wide caching of server metadata.

    Call once at startup, before building clients. With the cache on, the API
    versions a service speaks, the version it runs, and a datastack's info
    record are looked up once per process rather than once per client. Calling
    again replaces the settings and drops every cached entry.

    Parameters
    ----------
    ttl_slow :
        Seconds to keep slow-changing values: API versions, service versions,
        the datastack info payload, and table and version metadata. Twelve hours
        by default, since these change only when a service is deployed or a
        datastack is reconfigured, and `invalidate` or `refresh` covers those.
    ttl_fast :
        Seconds to keep fast-changing values: the list of available
        materialization versions, which grows as new versions are materialized.
        Ten minutes by default.
    token_scoped :
        Whether to key every datastack-scoped entry by auth token as well as by
        datastack. Off by default: one lookup is shared by every user in the
        process. Turn it on when tokens in a single process have *different*
        permissions on the same datastack.
    maxsize :
        Maximum entries per kind of metadata before least-recently-used
        eviction. Scale this up if the process serves many distinct datastacks
        or, with `token_scoped`, many users.

    Notes
    -----
    A TTL of 0 or None disables storage for that kind of entry while still
    collapsing concurrent duplicate lookups into one request.

    Warnings
    --------
    With caching enabled, the info service is not consulted for every client
    built, so a caller whose token lacks access to a datastack still gets a
    working client and can read the cached info payload (segmentation and image
    source paths, table names) and materialization version history. Data
    requests still carry that caller's own token and are refused as usual.

    Enable caching only in processes that check datastack authorization
    themselves before building a client. The intended setting is a service
    endpoint guarded by an authorization decorator, which then uses the client to
    reach other CAVE services on the user's behalf. In a process that serves
    users with differing permissions and cannot make that check, set
    `token_scoped=True` or leave caching off.

    See Also
    --------
    [caveclient.disable_server_cache][]
    [caveclient.invalidate][]
    [caveclient.refresh][]
    """
    _settings.update(
        enabled=True,
        ttl_slow=ttl_slow,
        ttl_fast=ttl_fast,
        token_scoped=token_scoped,
        maxsize=maxsize,
    )
    _drop_storage()


def disable_server_cache() -> None:
    """Turn off process-wide caching of server metadata.

    Drops every cached entry and restores the default settings. Materialization
    table and version metadata continue to be cached with the default
    lifetimes, as they were before the cache existed.

    See Also
    --------
    [caveclient.enable_server_cache][]
    """
    _settings.clear()
    _settings.update(_DEFAULT_SETTINGS)
    _drop_storage()


# Per-process salt: fingerprints must not be stable across processes, and the
# token itself must never be recoverable from a key.
_SALT = os.urandom(16)


def token_fingerprint(headers: Optional[Mapping]) -> str:
    """An opaque, non-reversible stand-in for an auth token, safe to use in a key."""
    value = (headers or {}).get("Authorization")
    if not value:
        return "anonymous"
    return hashlib.sha256(_SALT + value.encode("utf-8")).hexdigest()[:16]


def datastack_key(
    category: str,
    server_address: str,
    datastack: str,
    headers: Optional[Mapping] = None,
    extra: Tuple = (),
) -> MetaKey:
    """Key for a datastack-scoped entry, applying the token policy in one place.

    `headers` are the session headers of the client making the lookup; only a
    fingerprint of the token ever reaches the key, and only when `token_scoped`
    is set.
    """
    if _settings["token_scoped"]:
        extra = (token_fingerprint(headers),) + tuple(extra)
    return MetaKey(category, server_address, datastack, tuple(extra))


# -- storage -----------------------------------------------------------------
#
# One TTL cache per category, built lazily from the current configuration.
# Each has a lock and a condition so that concurrent misses on one key issue a
# single upstream request; the others wait for it. Exceptions are never stored.

#: Clock used for expiry; tests replace it to advance time without sleeping.
_timer = None

_storage = {}
_storage_lock = threading.Lock()


def _build_storage() -> dict:
    maxsize = int(_settings["maxsize"])
    built = {}
    for category in CATEGORIES:
        which = "ttl_fast" if category in FAST_CHANGING else "ttl_slow"
        ttl = _settings[which] or 0
        cache_kwargs = {"maxsize": maxsize, "ttl": ttl}
        if _timer is not None:
            cache_kwargs["timer"] = _timer
        cache = TTLCache(**cache_kwargs)
        lock = threading.RLock()
        condition = threading.Condition(lock)

        # `condition` is what makes concurrent misses collapse into a single
        # upstream call; `lock` alone does not. Requires cachetools >= 6.0.0.
        @cached(cache, key=lambda key, fetch: key, lock=lock, condition=condition)
        def _get(key, fetch):
            return fetch()

        built[category] = (cache, lock, _get)
    return built


def _get_storage() -> dict:
    if not _storage:
        with _storage_lock:
            if not _storage:
                _storage.update(_build_storage())
    return _storage


def _drop_storage() -> None:
    with _storage_lock:
        _storage.clear()


def _after_fork_in_child() -> None:
    # A lock held at the moment of the fork is inherited locked and would
    # deadlock the child, so the child starts cold with fresh locks.
    global _storage_lock
    _storage_lock = threading.Lock()
    _storage.clear()


if hasattr(os, "register_at_fork"):  # pragma: no cover - platform dependent
    os.register_at_fork(after_in_child=_after_fork_in_child)


# -- lookups -----------------------------------------------------------------


def get_or_fetch(key: MetaKey, fetch: Callable[[], Any], enabled=None) -> Any:
    """Return the cached value for `key`, calling `fetch` only if needed.

    `enabled` is a client's own `server_cache` setting: None follows the
    process-wide switch, a bool forces it. Categories in `ALWAYS_ON`
    ignore it.

    `fetch` must raise on failure rather than return a degraded value: exceptions
    are never cached, which is what keeps a transient outage from pinning a
    fallback. A definite answer that happens to be `None` -- a 404 from a server
    with no version endpoint, say -- is cached normally.
    """
    if key.category not in ALWAYS_ON:
        if enabled is None:
            enabled = _settings["enabled"]
        if not enabled:
            return fetch()
    _, _, getter = _get_storage()[key.category]
    value = getter(key, fetch)
    if key.category in SERVER_SCOPED:
        return value
    return copy.deepcopy(value)


def invalidate(server_address: Optional[str] = None) -> int:
    """Drop cached server metadata so the next lookup goes upstream.

    Use this after a stale-metadata error to force a cold rebuild and retry,
    rather than waiting out the time-to-live.

    Parameters
    ----------
    server_address :
        Limit to entries for this server. By default, everything is dropped;
        a cold rebuild costs one request per kind of metadata, so that is
        rarely worth narrowing.

    Returns
    -------
    :
        Number of entries removed.
    """
    removed = 0
    for cache, lock, _ in _get_storage().values():
        with lock:
            if server_address is None:
                removed += len(cache)
                cache.clear()
                continue
            for key in [k for k in cache if k.server_address == server_address]:
                if cache.pop(key, None) is not None or key not in cache:
                    removed += 1
    return removed


#: Sub-clients a `CAVEclient` builds lazily, by property name.
_SUB_CLIENTS = (
    "info",
    "chunkedgraph",
    "materialize",
    "skeleton",
    "state",
    "schema",
    "annotation",
    "l2cache",
)


def refresh(client=None, server_address: Optional[str] = None) -> int:
    """Drop cached metadata and, given a client, immediately re-prime it.

    Invalidation alone cannot refetch, because the cache holds no credentials.
    Passing a client rebuilds the sub-clients it has already constructed, so that
    API version negotiation is repeated and each service's version is looked up
    again using that client's token. The rebuilt sub-clients are new objects:
    a reference held to, say, `client.materialize` from before the call is not
    refreshed and should be re-read from the client.

    Parameters
    ----------
    client :
        A `CAVEclient` whose already-built sub-clients should be rebuilt.
    server_address :
        Limit to entries for this server.

    Returns
    -------
    :
        Number of entries removed.
    """
    removed = invalidate(server_address=server_address)
    if client is not None:
        built = [name for name in _SUB_CLIENTS if getattr(client, "_" + name, None)]
        for name in built:
            setattr(client, "_" + name, None)
        for name in built:
            try:
                getattr(client, name).server_version
            except Exception:  # pragma: no cover - re-priming is best effort
                logger.debug("could not rebuild %s sub-client", name)
    return removed
