import json
import logging
import os

from . import auth

logger = logging.getLogger(__name__)

DEFAULT_LOCATION = auth.default_token_location
DEFAULT_DATASTACK_FILE = "cave_datastack_to_server_map.json"


def read_map(filename=None):
    if filename is None:
        filename = os.path.join(DEFAULT_LOCATION, DEFAULT_DATASTACK_FILE)
    try:
        with open(os.path.expanduser(filename), "r") as f:
            data = json.load(f)
        return data
    except:  # noqa E722
        return {}


def is_writable(filename):
    # File exists but is not writeable
    if os.path.exists(os.path.expanduser(filename)):
        if not os.access(os.path.expanduser(filename), os.W_OK):
            return False
    else:
        try:
            # File does not exist so make the directories if possible
            if not os.path.exists(os.path.expanduser(DEFAULT_LOCATION)):
                os.makedirs(os.path.expanduser(DEFAULT_LOCATION))
            with open(os.path.expanduser(filename), "w") as f:
                if not f.writable():
                    return False
        except IOError:
            return False
    return True


def write_map(data, filename=None):
    if filename is None:
        filename = os.path.join(DEFAULT_LOCATION, DEFAULT_DATASTACK_FILE)

    if is_writable(filename):
        with open(os.path.expanduser(filename), "w") as f:
            json.dump(data, f)
        return True
    else:
        logging.warn(
            f"Did not write cache — file {os.path.expanduser(filename)} is not writeable"
        )
        return False


def handle_server_address(datastack, server_address, filename=None, write=False):
    data = read_map(filename)
    if server_address is not None and datastack is not None:
        if write and server_address != data.get(datastack):
            data[datastack] = server_address
            wrote = write_map(data, filename)
            if wrote:
                logger.warning(
                    f"Updated datastack-to-server cache — '{server_address}' will now be used by default for datastack '{datastack}'"
                )
        return server_address
    else:
        return data.get(datastack)


def get_datastack_cache(filename=None):
    return read_map(filename)


def reset_server_address_cache(datastack, filename=None):
    """Remove one or more datastacks from the datastack-to-server cache.

    Parameters
    ----------
    datastack : str or list of str, optional
        Datastack names to remove from the cache, by default None
    filename : str, optional
        Name of the cache file, by default None
    """
    data = read_map(filename)
    if isinstance(datastack, str):
        datastack = [datastack]
    for ds in datastack:
        data.pop(ds, None)
        logger.warning(f"Wiping '{ds}' from datastack-to-server cache")
    write_map(data, filename)
