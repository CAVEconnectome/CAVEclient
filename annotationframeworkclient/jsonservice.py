import requests
from annotationframeworkclient.endpoints import jsonservice_endpoints as jse
from annotationframeworkclient import endpoints
import json
import re


class JSONService(object):
    def __init__(self, server_address=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address

        self.session = requests.Session()
        self._default_url_mapping = {'json_server_address': self._server_address}

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @property
    def server_address(self):
        return self._server_address
    
    @server_address.setter
    def server_address(self, val):
        self._server_address = val
        self._default_url_mapping['json_server_address'] = value

    def get_state_json(self, state_id):
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        url = jse['get_state'].format_map(url_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return json.loads(response.content)

    def upload_state_json(self, json_state):
        url_mapping = self.default_url_mapping
        url = jse['upload_state'].format_map(url_mapping)
        response = self.session.post(url, data=json.dumps(json_state))
        assert(response.status_code == 200)
        response_re = re.search('.*\/(\d+)', str(response.content))
        return int(response_re.groups()[0])

    def build_neuroglancer_url(self, state_id, ngl_url):
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        get_state_url = jse['get_state'].format_map(url_mapping)
        url = ngl_url + '/?json_url=' + get_state_url
        return url

