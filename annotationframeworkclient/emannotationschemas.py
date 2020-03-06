import requests

from annotationframeworkclient.endpoints import schema_endpoints
from annotationframeworkclient import endpoints
from .auth import AuthClient


class SchemaClient(object):
    """Client to interface with the Schema service

    Parameters
    ----------
    server_address : str or None, optional
        Server hosting the Schema service. If None, use the default server address. By default, None.
    auth_client : auth.AuthClient or None, optional
        AuthClient with a token to use authenticated endpoints. If None, use no token. By default, None.
    """

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
        """Get the available schema types

        Returns
        -------
        list
            List of schema types available on the Schema service.
        """
        endpoint_mapping = self.default_url_mapping
        url = schema_endpoints['schema'].format_map(endpoint_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def schema_definition(self, schema_type):
        """Get the definition of a specified schema_type

        Parameters
        ----------
        schema_type : str
            Name of a schema_type

        Returns
        -------
        json
            Schema definition
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['schema_type'] = schema_type
        url = schema_endpoints['schema_definition'].format_map(endpoint_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
