"""Convenience constructor for a client that reuses cached server metadata.

`CachedClient` is a one-line way to opt a single client into the process-wide
cache described in `caveclient.servermeta` without changing the process-wide
configuration. To turn it on for every client instead:

    from caveclient import enable_server_cache

    enable_server_cache()
"""

from cachetools import keys

from caveclient import CAVEclient


def server_key(datastack_name, server_address, **kwargs):
    """Cache key for a datastack's info payload, ignoring the auth token.

    Retained for callers that imported it; the cache itself now lives in
    `caveclient.servermeta`.
    """
    return keys.hashkey(datastack_name, server_address)


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
    kwargs["server_cache"] = True
    client = CAVEclient(
        datastack_name=datastack_name, server_address=server_address, **kwargs
    )
    return client.info.info_cache


def CachedClient(datastack_name, server_address=None, **kwargs):
    """Initialize a CAVE client that reuses cached server metadata.

    The info payload, API versions and service versions are cached process-wide
    and shared across auth tokens, so building many
    clients for the same datastack performs the underlying lookups once rather
    than once per client.

    Because the cache is shared, a caller whose token would be refused by the
    info service still receives a client and the cached info payload. Use this
    only where datastack authorization has already been checked, such as in a
    service endpoint behind an authorization decorator; see the warning on
    [caveclient.enable_server_cache][].

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
    [caveclient.enable_server_cache][]
    """
    kwargs.setdefault("server_cache", True)
    return CAVEclient(
        datastack_name=datastack_name, server_address=server_address, **kwargs
    )


__all__ = ["CachedClient", "stored_info_cache", "server_key"]
