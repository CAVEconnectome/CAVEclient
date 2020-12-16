from .base import ClientBase, _api_endpoints, handle_response
from .endpoints import schema_common, schema_api_versions, schema_endpoints_common
from .auth import AuthClient
import requests

server_key = "emas_server_address"


def SchemaClient(server_address=None,
                 auth_client=None,
                 api_version='latest'):
    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(api_version, server_key, server_address,
                                            schema_endpoints_common, schema_api_versions, auth_header)
    SchemaClient = client_mapping[api_version]
    return SchemaClient(server_address=server_address,
                        auth_header=auth_header,
                        api_version=api_version,
                        endpoints=endpoints,
                        server_name=server_key)


class SchemaClientLegacy(ClientBase):
    def __init__(self, server_address, auth_header, api_version, endpoints, server_name):
        super(SchemaClientLegacy, self).__init__(server_address,
                                                 auth_header, api_version, endpoints, server_name)

    def get_schemas(self):
        """Get the available schema types

        Returns
        -------
        list
            List of schema types available on the Schema service.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['schema'].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

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
        url = self._endpoints['schema_definition'].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)


client_mapping = {1: SchemaClientLegacy,
                  2: SchemaClientLegacy,
                  'latest': SchemaClientLegacy}