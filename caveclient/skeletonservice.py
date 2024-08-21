from typing import Optional

from .auth import AuthClient
from .base import (
    ClientBase,
    _api_endpoints,
    handle_response,
)
from .endpoints import skeletonservice_api_versions, skeletonservice_common

SERVER_KEY = "skeleton_server_address"


'''
Usage
'''

class SkeletonClient(ClientBase):
    def __init__(
        self,
        server_address,
        auth_client=None,
        api_version="latest",
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
    ):
        if auth_client is None:
            auth_client = AuthClient()

        auth_header = auth_client.request_header
        endpoints, api_version = _api_endpoints(
            api_version,
            SERVER_KEY,
            server_address,
            skeletonservice_common,
            skeletonservice_api_versions,
            auth_header,
            fallback_version=1,
            verify=verify,
        )

        super(SkeletonClient, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            SERVER_KEY,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )
    
    def get_skeleton(
        self,
        root_id: int,
        datastack_name:   Optional[str] =None,
        skeleton_version: Optional[int] =0,
        output_format:    Optional[str] ='precomputed',
    ):
        """Gets basic skeleton information for a datastack

        Parameters
        ----------
        root_id : int
            The root id of the skeleton to retrieve
        datastack_name : str
            The name of the datastack to check
        skeleton_version : int
            The skeleton version to generate and retrieve. Options are documented in SkeletonService. Use 0 for latest.
        output_format : string
            The format to retrieve. Options are documented in SkeletonService.

        Returns
        -------
        bool
            A skeleton in indicated format
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack"] = datastack_name
        endpoint_mapping["skeleton_version"] = skeleton_version
        endpoint_mapping["root_id"] = root_id
        endpoint_mapping["output_format"] = output_format

        url = self._endpoints["get_skeleton"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response, False)
    