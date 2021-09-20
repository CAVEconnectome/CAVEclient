import requests
from urllib3.util.retry import Retry

DEFAULT_RETRIES = requests.adapters.DEFAULT_RETRIES
DEFAULT_POOLSIZE = requests.adapters.DEFAULT_POOLSIZE
DEFAULT_POOLBLOCK = requests.adapters.DEFAULT_POOLBLOCK


def patch_session(
    session,
    max_retries=None,
    pool_block=None,
    pool_maxsize=None,
):
    """Patch session to configure retry and poolsize options

    Parameters
    ----------
    session : requests session
        Session to modify
    max_retries : Int or None, optional
        Set the number of retries per request, by default None. If None, defaults to requests package default.
    pool_block : Bool or None, optional
        If True, restricts pool of threads to max size, by default None. If None, defaults to requests package default.
    pool_maxsize : Int or None, optional
        Sets the max number of threads in the pool, by default None. If None, defaults to requests package default.
    """
    if max_retries is None:
        retries = DEFAULT_RETRIES
    else:
        retries = Retry(
            total=max_retries,
            backoff_factor=0.1,
            status_forcelist=[502, 503, 504],
        )
    if pool_block is None:
        pool_block = DEFAULT_POOLBLOCK
    if pool_maxsize is None:
        pool_maxsize = DEFAULT_POOLSIZE

    http = requests.adapters.HTTPAdapter(
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        max_retries=retries,
    )
    session.mount("http://", http)
    session.mount("https://", http)

    pass