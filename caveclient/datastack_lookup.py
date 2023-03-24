import os
import json
from . import auth

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
    if not os.path.exists(DEFAULT_LOCATION):
        os.makedirs( os.path.expanduser(DEFAULT_LOCATION)) 
    with open(os.path.expanduser(filename), 'w') as f:
        json.dump(data, f)
    print("Updated stored datastack server map")

def handle_server_address(datastack, server_address, filename=None, overwrite=False):
    data = read_map(filename)
    if server_address is not None:
        if (datastack in data and overwrite) or datastack not in data:
            data[datastack] = server_address
            write_map(data, filename)
        return server_address
    else:
        return data.get(datastack)

def get_datastack_map(filename=None):
    return read_map(filename)

