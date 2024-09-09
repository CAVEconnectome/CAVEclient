from cachetools import TTLCache, cached, keys

from caveclient import CAVEclient

info_cache_cache = TTLCache(maxsize=32, ttl=3600)


def server_key(datastack_name, server_address, **kwargs):
    key = keys.hashkey(datastack_name, server_address)
    return key


@cached(info_cache_cache, key=server_key)
def stored_info_cache(datastack_name, server_address, **kwargs):
    client = CAVEclient(
        datastack_name=datastack_name, server_address=server_address, **kwargs
    )
    return client.info.info_cache


def CachedClient(datastack_name, server_address=None, **kwargs):
    """Initialize a CAVE client using a cached version of the info service information with a 1 hour time to live.

    Parameters
    ----------
    datastack_name : string
        Datastack name
    server_address : string or None
        Local server address
    All additional keyword arguments are passed to the CAVEclient.

    Returns
    -------
    CAVEclient
    """
    kwargs["info_cache"] = stored_info_cache(datastack_name, server_address, **kwargs)
    return CAVEclient(
        datastack_name=datastack_name, server_address=server_address, **kwargs
    )
