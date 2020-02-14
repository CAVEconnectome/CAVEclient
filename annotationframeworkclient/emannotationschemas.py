import requests

from annotationframeworkclient.endpoints import schema_endpoints
from annotationframeworkclient import endpoints
from .auth import AuthClient

class SchemaClient(object):
    def __init__(self, server_address=None, auth_client=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address

        if auth_client is None:
            auth_client = AuthClient()

        self.session = requests.Session()
        self.session.headers.update(auth_client.request_header)

        self._default_url_mapping = {
            'emas_server_address': self._server_address
        }     

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def schema(self):
        endpoint_mapping = self.default_url_mapping
        url = schema_endpoints['schema'].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def schema_definition(self, schema_type):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['schema_type'] = schema_type
        url = schema_endpoints['schema_definition'].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()
