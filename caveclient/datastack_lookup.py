import os
import json
from . import auth
import logging
logger = logging.getLogger(__name__)

DEFAULT_LOCATION = auth.default_token_location
DEFAULT_DATASTACK_FILE = 'cave_datastack_to_server_map.json'

def read_map(filename = None):
    if filename is None:
        filename = os.path.join(DEFAULT_LOCATION, DEFAULT_DATASTACK_FILE)
    try:
        with open(os.path.expanduser(filename), 'r') as f:
            data = json.load(f)
        return data
    except:
        return {}

def write_map(data, filename = None):
    if filename is None:
        filename = os.path.join(DEFAULT_LOCATION, DEFAULT_DATASTACK_FILE)
    if not os.path.exists(os.path.expanduser(DEFAULT_LOCATION)):
        os.makedirs(os.path.expanduser(DEFAULT_LOCATION)) 
    with open(os.path.expanduser(filename), 'w') as f:
        json.dump(data, f)

def handle_server_address(datastack, server_address, filename=None, write=False):
    data = read_map(filename)
    if server_address is not None:
        if write and server_address != data.get(datastack):
            data[datastack] = server_address
            write_map(data, filename)
            logger.warning(f"Updated datastack-to-server cache — '{server_address}' will now be used by default for datastack '{datastack}'")
        return server_address
    else:
        return data.get(datastack)

def get_datastack_cache(filename=None):
    return read_map(filename)

def reset_server_address_cache(datastack, filename=None):
    """Remove one or more datastacks from the datastack-to-server cache.

    Parameters
    ----------
    datastacks : str or list of str, optional
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
    