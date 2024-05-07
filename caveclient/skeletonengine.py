
from .auth import AuthClient
from .base import (
    ClientBase,
    _api_endpoints,
    handle_response,
)
from .endpoints import (
    default_global_server_address,
    skeletonservice_api_versions,
    skeletonservice_common,
)
from .endpoints import skeleton_api_versions, skeleton_common

server_key = "skeleton_server_address"

SERVER_KEY = "ske_server_address"


def SkeletonizationClient(
    server_address,
    datastack_name=None,
    auth_client=None,
    api_version="latest",
    version=None,
    verify=True,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
) -> "SkeletonizationClient":
    """Factory for returning SkeletonizationClient

    Parameters
    ----------
    server_address : str
        server_address to use to connect to (i.e. https://minniev1.microns-daf.com)
    datastack_name : str
        Name of the datastack.
    auth_client : AuthClient or None, optional
        Authentication client to use to connect to server. If None, do not use authentication.
    api_version : str or int (default: latest)
        What version of the api to use, 0: Legacy client (i.e www.dynamicannotationframework.com)
        2: new api version, (i.e. minniev1.microns-daf.com)
        'latest': default to the most recent (current 2)
    version : default version to query
        if None will default to latest version

    Returns
    -------
    ClientBaseWithDatastack
        List of datastack names for available datastacks on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(
        api_version,
        SERVER_KEY,
        server_address,
        skeleton_common,
        skeleton_api_versions,
        auth_header,
        fallback_version=2,
        verify=verify,
    )

    return SkeletonizationClient(
        server_address,
        auth_header,
        api_version,
        endpoints,
        SERVER_KEY,
        datastack_name,
        version=version,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
    )