from .base import ClientBase, _api_versions, _api_endpoints
from .auth import AuthClient
from .endpoints import jsonservice_common, jsonservice_api_versions, default_global_server_address
import requests
import json
import re

server_key = 'json_server_address'


def JSONService(server_address=None,
                auth_client=None,
                api_version='latest',
                ):
    """Client factory to interface with the JSON state service.

    Parameters
    ----------
    server_address : str, optional
        URL to the JSON state server.
        If None, set to the default global server address.
        By default None.
    auth_client : An Auth client, optional
        An auth client with a token for the same global server, by default None
    api_version : int or 'latest', optional
        Which endpoint API version to use or 'latest'. By default, 'latest' tries to ask
        the server for which versions are available, if such functionality exists, or if not
        it defaults to the latest version for which there is a client. By default 'latest'
    """
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header

    endpoints, api_version = _api_endpoints(api_version, server_key, server_address,
                                            jsonservice_common, jsonservice_api_versions, auth_header)

    JSONClient = client_mapping[api_version]
    return JSONClient(server_address=server_address,
                      auth_header=auth_header,
                      api_version=api_version,
                      endpoints=endpoints,
                      server_name=server_key)


class JSONServiceV1(ClientBase):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name):
        super(JSONServiceV1, self).__init__(server_address,
                                            auth_header, api_version, endpoints, server_name)

    @property
    def state_service_endpoint(self):
        """Endpoint URL for posting JSON state
        """
        return self._endpoints['upload_state']

    def get_state_json(self, state_id):
        """Download a Neuroglancer JSON state

        Parameters
        ----------
        state_id : int
            ID of a JSON state uploaded to the state service.

        Returns
        -------
        dict
            JSON specifying a Neuroglancer state.
        """
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        url = self._endpoints['get_state'].format_map(url_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return json.loads(response.content)

    def upload_state_json(self, json_state):
        """Upload a Neuroglancer JSON state

        Parameters
        ----------
        json_state : dict
            JSON-formatted Neuroglancer state

        Returns
        -------
        int
            state_id of the uploaded JSON state
        """
        url_mapping = self.default_url_mapping
        url = self._endpoints['upload_state'].format_map(url_mapping)
        response = self.session.post(url, data=json.dumps(json_state))
        response.raise_for_status()
        response_re = re.search('.*\/(\d+)', str(response.content))
        return int(response_re.groups()[0])

    def build_neuroglancer_url(self, state_id, ngl_url):
        """Build a URL for a Neuroglancer deployment that will automatically retrieve specified state.

        Parameters
        ----------
        state_id : int
            State id to retrieve
        ngl_url : str
            Base url of a neuroglancer deployment. For example, 'https://neuromancer-seung-import.appspot.com'. 

        Returns
        -------
        str
            The full URL requested
        """
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        get_state_url = self._endpoints['get_state'].format_map(url_mapping)
        url = ngl_url + '/?json_url=' + get_state_url
        return url


class JSONServiceLegacy(ClientBase):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name):
        super(JSONServiceLegacy, self).__init__(server_address,
                                                auth_header, api_version, endpoints, server_name)

    @property
    def state_service_endpoint(self):
        """Endpoint URL for posting JSON state
        """
        return self._endpoints['upload_state']

    def get_state_json(self, state_id):
        """Download a Neuroglancer JSON state

        Parameters
        ----------
        state_id : int
            ID of a JSON state uploaded to the state service.

        Returns
        -------
        dict
            JSON specifying a Neuroglancer state.
        """
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        url = self._endpoints['get_state'].format_map(url_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return json.loads(response.content)

    def upload_state_json(self, json_state):
        """Upload a Neuroglancer JSON state

        Parameters
        ----------
        json_state : dict
            JSON-formatted Neuroglancer state

        Returns
        -------
        int
            state_id of the uploaded JSON state
        """
        url_mapping = self.default_url_mapping
        url = self._endpoints['upload_state'].format_map(url_mapping)
        response = self.session.post(url, data=json.dumps(json_state))
        response.raise_for_status()
        response_re = re.search('.*\/(\d+)', str(response.content))
        return int(response_re.groups()[0])

    def build_neuroglancer_url(self, state_id, ngl_url):
        """Build a URL for a Neuroglancer deployment that will automatically retrieve specified state.

        Parameters
        ----------
        state_id : int
            State id to retrieve
        ngl_url : str
            Base url of a neuroglancer deployment. For example, 'https://neuromancer-seung-import.appspot.com'. 

        Returns
        -------
        str
            The full URL requested
        """
        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        get_state_url = self._endpoints['get_state'].format_map(url_mapping)
        if ngl_url[-1] == '/':
            spacer = '?json_url='
        else:
            spacer = '/?json_url='
        url = ngl_url + '/?json_url=' + get_state_url
        return url


client_mapping = {0: JSONServiceLegacy,
                  1: JSONServiceV1,
                  }
