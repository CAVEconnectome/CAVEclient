from __future__ import annotations

from typing import Literal, Optional

from .auth import AuthClient
from .base import ClientBase, _api_endpoints, handle_response
from .endpoints import skeletonservice_api_versions, skeletonservice_common

SERVER_KEY = "skeleton_server_address"


"""
Usage
"""


class SkeletonClient(ClientBase):
    def __init__(
        self,
        server_address: str,
        datastack_name=None,
        auth_client: Optional[AuthClient] = None,
        api_version: str = "latest",
        verify: bool = True,
        max_retries: int = None,
        pool_maxsize: int = None,
        pool_block: bool = None,
        over_client: Optional[CAVEclientFull] = None,  # noqa: F821 # type: ignore
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

        self._datastack_name = datastack_name

    def run_endpoint_tests(self):
        if self._datastack_name is not None:
            # I could write a complicated test that confirms that an AssertionError is raised
            # when datastack_name and self._datastack_name are both None, but I'm just don't want to at the moment.
            # The combinatorial explosion of test varieties is getting out of hand.
            url = self.build_endpoint(123456789, None, None, "precomputed"
            ).split("/", 6)[-1]
            assert url == f"{self._datastack_name}/precomputed/skeleton/123456789"

            url = self.build_endpoint(123456789, None, None, "json"
            ).split("/", 6)[-1]
            assert (
                url == f"{self._datastack_name}/precomputed/skeleton/0/123456789/json"
            )

        url = self.build_endpoint(123456789, "test_datastack", None, "precomputed"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/123456789"

        url = self.build_endpoint(123456789, "test_datastack", None, "json"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/0/123456789/json"

        url = self.build_endpoint(123456789, "test_datastack", 0, "precomputed"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/0/123456789"

        url = self.build_endpoint(123456789, "test_datastack", 0, "json"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/0/123456789/json"

        url = self.build_endpoint(123456789, "test_datastack", 1, "precomputed"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/1/123456789"

        url = self.build_endpoint(123456789, "test_datastack", 1, "json"
        ).split("/", 6)[-1]
        assert url == "test_datastack/precomputed/skeleton/1/123456789/json"

    def build_endpoint(
        self,
        root_id: int,
        datastack_name: str,
        skeleton_version: int,
        output_format: str,
    ):
        """
        Building the URL in a separate function facilities testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_id"] = root_id

        if skeleton_version is None:
            # Pylance incorrectly thinks that skeleton_version cannot be None here,
            # but it most certainly can, and that is precisely how I intended it.
            # Google searching revealed this as a known problem with Pylance and Selenium,
            # but I have not been successful in solving it yet.
            if output_format == "precomputed":
                endpoint = "get_skeleton_via_rid"
            else:
                # Note that there isn't currently an endpoint for this scenario,
                # so we'll just use the skvn_rid_fmt endpoint with skvn set to the default value of 0
                endpoint_mapping["skeleton_version"] = 0
                endpoint_mapping["output_format"] = output_format
                endpoint = "get_skeleton_via_skvn_rid_fmt"
        else:
            endpoint_mapping["skeleton_version"] = skeleton_version
            if output_format == "precomputed":
                endpoint = "get_skeleton_via_skvn_rid"
            else:
                endpoint_mapping["output_format"] = output_format
                endpoint = "get_skeleton_via_skvn_rid_fmt"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        return url

    def get_skeleton(
        self,
        root_id: int,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = None,
        output_format: Literal[
            "none", "h5", "swc", "json", "arrays", "precomputed"
        ] = "precomputed",
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
            The format to retrieve. Options are 'none', 'h5', 'swc', 'json', 'arrays', 'precomputed'

        Returns
        -------
        bool
            A skeleton in indicated format
        """
        url = self.build_endpoint(
            root_id, datastack_name, skeleton_version, output_format
        )

        response = self.session.get(url)
        return handle_response(response, False)
