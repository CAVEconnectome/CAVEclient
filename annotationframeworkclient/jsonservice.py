import requests
from annotationframeworkclient.endpoints import jsonservice_endpoints as jse
from annotationframeworkclient import endpoints
import json
import re
from .auth import AuthClient


class JSONService(object):
    """ Client for interfacing with the Neuroglancer state server.

    Parameters
    ----------
    server_address : str or None, optional
        Location of the state server. If None, uses www.dynamicannotationframework.com

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

        self._default_url_mapping = {'json_server_address': self._server_address}

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @property
    def server_address(self):
        return self._server_address

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
        url = jse['get_state'].format_map(url_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
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
        url = jse['upload_state'].format_map(url_mapping)
        response = self.session.post(url, data=json.dumps(json_state))
        assert(response.status_code == 200)
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
        get_state_url = jse['get_state'].format_map(url_mapping)
        url = ngl_url + '/?json_url=' + get_state_url
        return url
