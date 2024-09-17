from __future__ import annotations

import logging
from io import BytesIO, StringIO
from typing import Literal, Optional

import pandas as pd
from packaging.version import Version

try:
    import cloudvolume

    CLOUDVOLUME_AVAILABLE = True
except ImportError:
    logging.warning(
        "cloudvolume not installed. Some output formats will not be available."
    )

    CLOUDVOLUME_AVAILABLE = False

from .auth import AuthClient
from .base import ClientBase, _api_endpoints
from .endpoints import skeletonservice_api_versions, skeletonservice_common

SERVER_KEY = "skeleton_server_address"


"""
Usage
"""


class NoL2CacheException(Exception):
    def __init__(self, value=""):
        """
        Parameters:
        value (str) [optional]: A more detailed description of the error, if desired.
        """
        super().__init__(f"No L2Cache found. {value}".strip())


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
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )

        self._datastack_name = datastack_name

    def _test_get_version(self) -> Optional[Version]:
        print("_test_get_version()")
        endpoint_mapping = self.default_url_mapping
        endpoint = self._endpoints.get("get_version_test", None)
        print(f"endpoint: {endpoint}")
        if endpoint is None:
            return None

        url = endpoint.format_map(endpoint_mapping)
        print(f"url: {url}")
        response = self.session.get(url)
        print(f"response: {response}")
        if response.status_code == 404:  # server doesn't have this endpoint yet
            print("404")
            return None
        else:
            version_str = response.json()
            print(f"version_str: {type(version_str)} {version_str}")
            version = Version(version_str)
            print(f"version: {version}")
            return version

    def _test_l2cache_exception(self):
        raise NoL2CacheException(
            "This is a test of SkeletonClient's behavior when no L2Cache is found."
        )

    def _test_endpoints(self):
        def parse(url):
            return url.split("/", 6)[-1]

        rid = 123456789
        ds = "test_datastack"
        innards = "/precomputed/skeleton/"

        if self._datastack_name is not None:
            # I could write a complicated test that confirms that an AssertionError is raised
            # when datastack_name and self._datastack_name are both None, but I'm just don't want to at the moment.
            # The combinatorial explosion of test varieties is getting out of hand.
            url = parse(self.build_endpoint(rid, None, None, "precomputed"))
            assert url == f"{self._datastack_name}{innards}{rid}"

            url = parse(self.build_endpoint(rid, None, None, "json"))
            assert url == f"{self._datastack_name}{innards}0/{rid}/json"

        url = parse(self.build_endpoint(rid, ds, None, "precomputed"))
        assert url == f"{ds}{innards}{rid}"

        url = parse(self.build_endpoint(rid, ds, None, "json"))
        assert url == f"{ds}{innards}0/{rid}/json"

        url = parse(self.build_endpoint(rid, ds, 0, "precomputed"))
        assert url == f"{ds}{innards}0/{rid}"

        url = parse(self.build_endpoint(rid, ds, 0, "json"))
        assert url == f"{ds}{innards}0/{rid}/json"

        url = parse(self.build_endpoint(rid, ds, 1, "precomputed"))
        assert url == f"{ds}{innards}1/{rid}"

        url = parse(self.build_endpoint(rid, ds, 1, "json"))
        assert url == f"{ds}{innards}1/{rid}/json"

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
        ] = "none",
        log_warning: bool = True,
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
        The return type will vary greatly depending on the output_format parameter. The options are:
        - 'none': No return value (this can be used to generate a skeleton without retrieving it)
        - 'precomputed': A cloudvolume.Skeleton object
        - 'json': A dictionary
        - 'arrays': A dictionary (literally a subset of the json response)
        - 'swc': A pandas DataFrame
        - 'h5': An BytesIO object containing bytes for an h5 file
        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        url = self.build_endpoint(
            root_id, datastack_name, skeleton_version, output_format
        )

        if skeleton_version is None:
            # I need code in this repo to access defaults defined in the SkeletonService repo, but wihout necesssarily importing it.
            skeleton_version = 2

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        if output_format == "none":
            return
        if output_format == "precomputed":
            if not CLOUDVOLUME_AVAILABLE:
                raise ImportError(
                    "'precomputed' output format requires cloudvolume, which is not available."
                )
            vertex_attributes = []
            if skeleton_version == 2:
                # I need code in this repo to access defaults defined in the SkeletonService repo, but wihout necesssarily importing it.
                vertex_attributes.append(
                    {"id": "radius", "data_type": "float32", "num_components": 1}
                )
                vertex_attributes.append(
                    {"id": "compartment", "data_type": "float32", "num_components": 1}
                )
            return cloudvolume.Skeleton.from_precomputed(
                response.content, vertex_attributes=vertex_attributes
            )
        if output_format == "json":
            return response.json()
        if output_format == "arrays":
            return response.json()
        if output_format == "swc":
            # I got the SWC column header from skeleton_plot.skel_io.py
            return pd.read_csv(
                StringIO(response.content.decode()),
                sep=" ",
                names=["id", "type", "x", "y", "z", "radius", "parent"],
            )
        if output_format == "h5":
            skeleton_bytesio = BytesIO(response.content)
            return skeleton_bytesio

        raise ValueError(f"Unknown output format: {output_format}")
