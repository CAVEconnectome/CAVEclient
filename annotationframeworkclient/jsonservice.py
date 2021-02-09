from .base import ClientBase, _api_versions, _api_endpoints, handle_response
from .auth import AuthClient
from .endpoints import jsonservice_common, jsonservice_api_versions, default_global_server_address
import requests
import json
import re

server_key = 'json_server_address'


def JSONService(server_address=None,
                auth_client=None,
                api_version='latest',
                ngl_url=None,
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
    ngl_url : str or None, optional
        Default neuroglancer deployment URL. Only used for V1 and later.
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
                      server_name=server_key,
                      ngl_url=ngl_url)


class JSONServiceV1(ClientBase):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name,
                 ngl_url):
        super(JSONServiceV1, self).__init__(server_address,
                                            auth_header, api_version, endpoints, server_name)
        self._ngl_url = ngl_url

    @property
    def state_service_endpoint(self):
        """Endpoint URL for posting JSON state
        """
        url_mapping = self.default_url_mapping
        return self._endpoints['upload_state'].format_map(url_mapping)

    @property
    def ngl_url(self):
        return self._ngl_url

    @ngl_url.setter
    def ngl_url(self, new_ngl_url):
        self._ngl_url = new_ngl_url

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
        handle_response(response, as_json=False)
        return json.loads(response.content)

    def upload_state_json(self, json_state, state_id=None, 
                          timestamp=None):
        """Upload a Neuroglancer JSON state

        Parameters
        ----------
        json_state : dict
            JSON-formatted Neuroglancer state
        state_id : int 
            ID of a JSON state uploaded to the state service.
            Using a state_id is an admin feature.
        timestamp: time.time
            Timestamp for json state date. Requires state_id.

        Returns
        -------
        int
            state_id of the uploaded JSON state
        """
        url_mapping = self.default_url_mapping

        if state_id is None:
            url = self._endpoints['upload_state'].format_map(url_mapping)
        else:
            url_mapping = self.default_url_mapping
            url_mapping['state_id'] = state_id
            url = self._endpoints['upload_state_w_id'].format_map(url_mapping)

        response = self.session.post(url, data=json.dumps(json_state))
        handle_response(response, as_json=False)
        response_re = re.search('.*\/(\d+)', str(response.content))
        return int(response_re.groups()[0])

    def build_neuroglancer_url(self, state_id, ngl_url=None):
        """Build a URL for a Neuroglancer deployment that will automatically retrieve specified state.
        If the datastack is specified, this is prepopulated from the info file field "viewer_site".
        If no ngl_url is specified in either the function or the client, only the JSON state url is returned.

        Parameters
        ----------
        state_id : int
            State id to retrieve
        ngl_url : str
            Base url of a neuroglancer deployment. If None, defaults to the value for the datastack or the client.
            If no value is found, only the URL to the JSON state is returned.

        Returns
        -------
        str
            The full URL requested
        """
        if ngl_url is None:
            ngl_url = self.ngl_url
        if ngl_url is None:
            ngl_url = ''
            parameter_text = ''
        elif ngl_url[-1] == '/':
            parameter_text = '?json_url='
        else:
            parameter_text = '/?json_url='

        url_mapping = self.default_url_mapping
        url_mapping['state_id'] = state_id
        get_state_url = self._endpoints['get_state'].format_map(url_mapping)
        url = ngl_url + parameter_text + get_state_url
        return url


class JSONServiceLegacy(ClientBase):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name,
                 ngl_url):
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
        handle_response(response, as_json=False)
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
        handle_response(response, as_json=False)
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
