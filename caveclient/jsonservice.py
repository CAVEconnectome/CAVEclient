from .base import (
    ClientBase,
    BaseEncoder,
    _api_versions,
    _api_endpoints,
    handle_response,
)
from .auth import AuthClient
from .endpoints import (
    jsonservice_common,
    jsonservice_api_versions,
    default_global_server_address,
    ngl_endpoints_common,
)
import os
import requests
import numpy as np
import numbers
import json
import re

server_key = "json_server_address"


def neuroglancer_json_encoder(obj):
    """JSON encoder for neuroglancer states.
    Differs from normal in that it expresses ints as strings"""
    if isinstance(obj, numbers.Integral):
        return str(obj)
    if isinstance(obj, np.integer):
        return str(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return list(obj)
    elif isinstance(obj, (set, frozenset)):
        return list(obj)
    raise TypeError

def JSONService(
    server_address=None,
    auth_client=None,
    api_version="latest",
    ngl_url=None,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
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

    endpoints, api_version = _api_endpoints(
        api_version,
        server_key,
        server_address,
        jsonservice_common,
        jsonservice_api_versions,
        auth_header,
    )

    JSONClient = client_mapping[api_version]
    return JSONClient(
        server_address=server_address,
        auth_header=auth_header,
        api_version=api_version,
        endpoints=endpoints,
        server_name=server_key,
        ngl_url=ngl_url,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
    )


class JSONServiceV1(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        ngl_url,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None
    ):
        super(JSONServiceV1, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )
        self._ngl_url = ngl_url

    @property
    def state_service_endpoint(self):
        """Endpoint URL for posting JSON state"""
        url_mapping = self.default_url_mapping
        return self._endpoints["upload_state"].format_map(url_mapping)

    @property
    def ngl_url(self):
        return self._ngl_url

    @ngl_url.setter
    def ngl_url(self, new_ngl_url):
        self._ngl_url = new_ngl_url

    def get_neuroglancer_info(self, ngl_url=None):
        """Get the info field from a Neuroglancer deployment

        Parameters
        ----------
        ngl_url : str (optional)
            URL to a Neuroglancer deployment.
            If None, defaults to the value for the datastack or the client.

        Returns
        -------
        dict
            JSON-formatted info field from the Neuroglancer deployment
        """
        if ngl_url is None:
            ngl_url = self.ngl_url

        url_mapping = self.default_url_mapping
        url_mapping["ngl_url"] = ngl_url
        url = ngl_endpoints_common.get('get_info').format_map(url_mapping)
        response = self.session.get(url)
        # Not all neuroglancer deployments have a version.json,
        # so return empty if not found rather than throw error.
        if response.status_code == 404:
            return {}

        handle_response(response, as_json=False)
        return json.loads(response.content)


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
        url_mapping["state_id"] = state_id
        url = self._endpoints["get_state"].format_map(url_mapping)
        response = self.session.get(url)
        handle_response(response, as_json=False)
        return json.loads(response.content)

    def upload_state_json(self, json_state, state_id=None, timestamp=None):
        """Upload a Neuroglancer JSON state

        Parameters
        ----------
        json_state : dict
            Dict representation of a neuroglancer state 
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
            url = self._endpoints["upload_state"].format_map(url_mapping)
        else:
            url_mapping = self.default_url_mapping
            url_mapping["state_id"] = state_id
            url = self._endpoints["upload_state_w_id"].format_map(url_mapping)

        response = self.session.post(
            url,
            data=json.dumps(
                json_state,
                default=neuroglancer_json_encoder,
            )
        )
        handle_response(response, as_json=False)
        response_re = re.search(".*\/(\d+)", str(response.content))
        return int(response_re.groups()[0])

    def save_state_json_local(self, json_state, filename, overwrite=False):
        """Save a Neuroglancer JSON state to a JSON file locally.
        
        Parameters
        ----------
        json_state : dict
            Dict representation of a neuroglancer state
        filename : str
            Filename to save the state to
        overwrite : bool
            Whether to overwrite the file if it exists. Default False.

        Returns
        -------
        None
        """
        if os.path.exists(filename) and not overwrite:
            raise ValueError("File exists and overwrite is False")
        with open(filename, "w") as f:
            json.dump(json_state, f, default=neuroglancer_json_encoder)

    def build_neuroglancer_url(
            self,
            state_id,
            ngl_url=None,
            target_site=None,
            static_url=False,
        ):
        """Build a URL for a Neuroglancer deployment that will automatically retrieve specified state.
        If the datastack is specified, this is prepopulated from the info file field "viewer_site".
        If no ngl_url is specified in either the function or the client, a fallback neuroglancer deployment is used.

        Parameters
        ----------
        state_id : int
            State id to retrieve
        ngl_url : str
            Base url of a neuroglancer deployment. If None, defaults to the value for the datastack or the client.
            As a fallback, a default deployment is used.
        target_site : 'seunglab' or 'cave-explorer' or 'mainline' or None
            Set this to 'seunglab' for a seunglab deployment, or either 'cave-explorer'/'mainline' for a google main branch deployment.
            If None, checks the info field of the neuroglancer endpoint to determine which to use.
            Default is None.
        static_url : bool
            If True, treats "state_id" as a static URL directly to the JSON and does not use the state service.

        Returns
        -------
        str
            The full URL requested
        """
        if ngl_url is None:
            if self.ngl_url is not None:
                ngl_url = self.ngl_url
            else:
                ngl_url = ngl_endpoints_common['fallback_ngl_url']

        if target_site is None and ngl_url is not None:
            ngl_info = self.get_neuroglancer_info(ngl_url)
            if len(ngl_info) > 0:
                target_site = 'cave-explorer'
            else:
                target_site = "seunglab"

        if target_site == "seunglab":
            if ngl_url[-1] == "/":
                parameter_text = "?json_url="
            else:
                parameter_text = "/?json_url="
            auth_text = ""
        elif target_site == "cave-explorer" or target_site == "mainline":
            if ngl_url[-1] == "/":
                parameter_text = "#!"
            else:
                parameter_text = "/#!"
            auth_text = "middleauth+"
        else:
            target_site_error = "A specified target_site must be one of 'seunglab', 'cave-explorer' or 'mainline'"
            raise ValueError(target_site_error)

        if static_url:
            url = ngl_url + parameter_text + state_id
        else:
            url_mapping = self.default_url_mapping
            url_mapping["state_id"] = state_id
            get_state_url = self._endpoints["get_state"].format_map(url_mapping)
            url = ngl_url + parameter_text + auth_text + get_state_url
        return url


client_mapping = {
    1: JSONServiceV1,
}
