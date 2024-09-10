from typing import Collection, Optional, Union

import requests
from urllib3.util.retry import Retry

SESSION_DEFAULTS = {}


def set_session_defaults(
    max_retries: int = 3,
    pool_block: bool = False,
    pool_maxsize: int = 10,
    backoff_factor: Union[float, int] = 0.1,
    backoff_max: Union[float, int] = 120,
    status_forcelist: Optional[Collection] = (502, 503, 504),
) -> None:
    """Set global default values to configure how all clients will communicate with
    servers. Should be done prior to initializing a client.

    Note that these values are only used when not set at the client level.

    Parameters
    ----------
    max_retries :
        The maximum number of retries each connection should attempt. Set to 0 to fail
        on the first retry.
    pool_block :
        Whether the connection pool should block for connections.
    pool_maxsize :
        The maximum number of connections to save in the pool.
    backoff_factor :
        A backoff factor to apply between attempts after the second try (most errors
        are resolved immediately by a second try without a delay). The query will sleep
        for:
            ```{backoff factor} * (2 ^ ({number of total retries} - 1))``` seconds.
        For example, if the `backoff_factor` is 0.1, then will sleep for
        [0.0s, 0.2s, 0.4s, 0.8s, …] between retries. No backoff will ever be longer than
        `backoff_max`.
    backoff_max :
        The maximum backoff time.
    status_forcelist :
        A set of integer HTTP status codes that we should force a retry on.

    Usage
    -----

        from caveclient import set_session_defaults

        set_session_defaults(
            max_retries=5, # would increase the default number of retries
            backoff_factor=0.5, # would increase the default backoff factor between retries
            backoff_max=240, # would increase the default maximum backoff time
            status_forcelist=(502, 503, 504, 505), # would add 505 to the default list
        )

        set_session_defaults() # would revert all defaults to their original values

    Notes
    -----
    Calling this function will set the default values for all clients created after the
    call.

    Calling this function with any arguments missing will reset that value to the
    default value.

    See Also:
    ---------

    [urllib3.util.Retry][]

    [requests.adapters.HTTPAdapter][]

    """
    SESSION_DEFAULTS["max_retries"] = max_retries
    SESSION_DEFAULTS["pool_block"] = pool_block
    SESSION_DEFAULTS["pool_maxsize"] = pool_maxsize
    SESSION_DEFAULTS["backoff_factor"] = backoff_factor
    SESSION_DEFAULTS["backoff_max"] = backoff_max
    SESSION_DEFAULTS["status_forcelist"] = status_forcelist


set_session_defaults()


def get_session_defaults() -> dict:
    """Get the current default values for session configuration.

    Returns
    -------
    :
        Dictionary of current default values for session configuration.
    """
    return SESSION_DEFAULTS


def _patch_session(
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
        max_retries = SESSION_DEFAULTS["max_retries"]
    if pool_block is None:
        pool_block = SESSION_DEFAULTS["pool_block"]
    if pool_maxsize is None:
        pool_maxsize = SESSION_DEFAULTS["pool_maxsize"]

    retries = Retry(
        total=max_retries,
        backoff_factor=SESSION_DEFAULTS["backoff_factor"],
        status_forcelist=SESSION_DEFAULTS["status_forcelist"],
        allowed_methods=frozenset(["GET", "POST"]),
        backoff_max=SESSION_DEFAULTS["backoff_max"],
        raise_on_status=False,
    )

    http = requests.adapters.HTTPAdapter(
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        max_retries=retries,
    )
    session.mount("http://", http)
    session.mount("https://", http)

    pass
