from .base import (
    ClientBase,
    BaseEncoder,
    _api_versions,
    _api_endpoints,
    handle_response,
)
from .auth import AuthClient
from .endpoints import (
    functionmodel_endpoints_common,
    functionalmodel_api_versions,
    default_global_server_address,
)
import requests
import json
import re
import pyarrow as pa

server_key = "functionalmodel_server_address"


def deserialize_dataframe_response(response):
    """Deserialize pyarrow responses"""
    content_type = response.headers.get("Content-Type")
    if content_type == "data.arrow":
        with pa.ipc.open_stream(response.content) as reader:
            df = reader.read_pandas()
        return df
    else:
        raise ValueError("Response content type {} not recognized".format(content_type))


def FunctionModelClient(
    server_address=None,
    auth_client=None,
    api_version="latest",
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
    verify=True,
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
        functionmodel_endpoints_common,
        functionalmodel_api_versions,
        auth_header,
    )

    FunctionModelClientVersion = client_mapping[api_version]
    return FunctionModelClientVersion(
        server_address=server_address,
        auth_header=auth_header,
        api_version=api_version,
        endpoints=endpoints,
        server_name=server_key,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
        verify=verify,
    )


class FunctionModelClientV1(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
        verify=False,
    ):
        super(FunctionModelClientV1, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
            verify=verify,
        )

    # @property
    # def state_service_endpoint(self):
    #     """Endpoint URL for posting JSON state"""
    #     url_mapping = self.default_url_mapping
    #     return self._endpoints["upload_state"].format_map(url_mapping)

    # @property
    # def ngl_url(self):
    #     return self._ngl_url

    # @ngl_url.setter
    # def ngl_url(self, new_ngl_url):
    #     self._ngl_url = new_ngl_url

    def get_dataset_units(self, dataset_name):
        """Download a dataset units metadata dataframe

        Parameters
        ----------
        dataset_name : str
            Name of dataset to retrieve

        Returns
        -------
        pd.DataFrame
            Metadata dataframe
        """
        url_mapping = self.default_url_mapping
        url_mapping["dataset_name"] = dataset_name
        url = self._endpoints["dataset_units"].format_map(url_mapping)
        headers = {"Accept-Encoding": "gzip"}

        response = self.session.get(
            url,
            headers=headers,
        )
        self.raise_for_status(response)
        df = deserialize_dataframe_response(response)

        return df


client_mapping = {
    1: FunctionModelClientV1,
}
