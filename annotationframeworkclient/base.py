import requests
from urllib.parse import urlparse
from annotationframeworkclient.endpoints import default_server_address 

class ClientBase(object):
    def __init__(self, dataset_name=None, server_address=None):
        if server_address is None:
            self._server_adddress = default_server_address
        else:
            self._server_adddress = server_address

        self._dataset_name = dataset_name

        self.session = requests.Session()
        self._default_url_mapping = {}

    @property
    def default_url_mapping(self):
        return self._default_url_mapping
   
    @property
    def server_address(self):
        return self._server_adddress

    @property
    def dataset_name(self):
        return self._dataset_name
    
    @property
    def default_url_mapping(self):
        return self._default_url_mapping
    