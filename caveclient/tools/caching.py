"""Convenience constructor for a client that reuses cached server metadata.

Retained for backwards compatibility. `CAVEclient` can now cache the same
information -- and more -- process-wide; prefer configuring that directly:

    from caveclient import set_server_cache_defaults

    set_server_cache_defaults(enabled=True)

`CachedClient` remains a one-line way to get the same effect for a single
client without changing process-wide configuration.
"""

from cachetools import TTLCache, keys

from caveclient import CAVEclient
from caveclient.servermeta import CATEGORY_INFO, MetaKey, get_registry

#: Retained so that existing imports keep working. The authoritative store is
#: now the process-wide registry in `caveclient.servermeta`.
info_cache_cache = TTLCache(maxsize=32, ttl=3600)


def server_key(datastack_name, server_address, **kwargs):
    """Cache key for a datastack's info payload.

    Deliberately excludes the auth token, so that one lookup is shared by every
    user in the process.
    """
    key = keys.hashkey(datastack_name, server_address)
    return key


def stored_info_cache(datastack_name, server_address=None, **kwargs):
    """Get a datastack's info payload, reusing a cached copy when available.

    Parameters
    ----------
    datastack_name : str
        Name of the datastack.
    server_address : str, optional
        Global server address. If None, uses the default.

    Returns
    -------
    dict
        An info cache dictionary suitable for passing to `CAVEclient` as
        `info_cache`.
    """
    kwargs.pop("info_cache", None)
    kwargs.setdefault("server_cache", True)
    client = CAVEclient(
        datastack_name=datastack_name,
        server_address=server_address,
        **kwargs,
    )
    return client.info.info_cache


def CachedClient(datastack_name, server_address=None, **kwargs):
    """Initialize a CAVE client that reuses cached info service information.

    Server metadata is cached with a one hour time to live and shared across
    auth tokens, so building many clients for the same datastack performs the
    underlying lookups once rather than once per client.

    Parameters
    ----------
    datastack_name : str
        Name of the datastack.
    server_address : str, optional
        Global server address. If None, uses the default.
    **kwargs
        Additional arguments passed to `CAVEclient`.

    Returns
    -------
    CAVEclient
        A client configured to use cached server metadata.

    See Also
    --------
    [caveclient.set_server_cache_defaults][]
    """
    kwargs.setdefault("server_cache", True)
    kwargs["info_cache"] = stored_info_cache(
        datastack_name, server_address=server_address, **kwargs
    )
    return CAVEclient(
        datastack_name=datastack_name, server_address=server_address, **kwargs
    )


def invalidate_cached_info(datastack_name=None, server_address=None) -> int:
    """Drop cached info payloads so the next lookup goes upstream.

    Returns
    -------
    int
        Number of entries removed.
    """
    return get_registry().invalidate(
        datastack=datastack_name,
        server_address=server_address,
        category=CATEGORY_INFO,
    )


__all__ = [
    "CachedClient",
    "stored_info_cache",
    "server_key",
    "info_cache_cache",
    "invalidate_cached_info",
]

# Referenced so the re-exported key type stays importable for callers building
# keys by hand against the registry.
_META_KEY = MetaKey
