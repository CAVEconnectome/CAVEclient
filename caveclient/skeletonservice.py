from __future__ import annotations

import gzip
import json
import logging
from io import BytesIO, StringIO
from typing import Literal, Optional

import pandas as pd
from cachetools import TTLCache, cached
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


class NoL2CacheException(Exception):
    def __init__(self, value=""):
        """
        Parameters:
        value (str) [optional]: A more detailed description of the error, if desired.
        """
        super().__init__(f"No L2Cache found. {value}".strip())


class SkeletonClient(ClientBase):
    """Client for interacting with the skeleton service."""

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

    @staticmethod
    def compressStringToBytes(inputString):
        """
        Shamelessly copied from SkeletonService to avoid importing the entire repo. Consider pushing these utilities to a separate module.
        REF: https://stackoverflow.com/questions/15525837/which-is-the-best-way-to-compress-json-to-store-in-a-memory-based-store-like-red
        read the given string, encode it in utf-8, compress the data and return it as a byte array.
        """
        bio = BytesIO()
        bio.write(inputString.encode("utf-8"))
        bio.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode="w")
        while True:  # until EOF
            chunk = bio.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)

    @staticmethod
    def compressDictToBytes(inputDict, remove_spaces=True):
        """
        Shamelessly copied from SkeletonService to avoid importing the entire repo. Consider pushing these utilities to a separate module.
        """
        inputDictStr = json.dumps(inputDict)
        if remove_spaces:
            inputDictStr = inputDictStr.replace(" ", "")
        inputDictStrBytes = SkeletonClient.compressStringToBytes(inputDictStr)
        return inputDictStrBytes

    @staticmethod
    def decompressBytesToString(inputBytes):
        """
        Shamelessly copied from SkeletonService to avoid importing the entire repo. Consider pushing these utilities to a separate module.
        REF: https://stackoverflow.com/questions/15525837/which-is-the-best-way-to-compress-json-to-store-in-a-memory-based-store-like-red
        decompress the given byte array (which must be valid compressed gzip data) and return the decoded text (utf-8).
        """
        bio = BytesIO()
        stream = BytesIO(inputBytes)
        decompressor = gzip.GzipFile(fileobj=stream, mode="r")
        while True:  # until EOF
            chunk = decompressor.read(8192)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read().decode("utf-8")
            bio.write(chunk)
        return None

    @staticmethod
    def decompressBytesToDict(inputBytes):
        """
        Shamelessly copied from SkeletonService to avoid importing the entire repo. Consider pushing these utilities to a separate module.
        """
        inputBytesStr = SkeletonClient.decompressBytesToString(inputBytes)
        inputBytesStrDict = json.loads(inputBytesStr)
        return inputBytesStrDict

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

    @cached(TTLCache(maxsize=32, ttl=3600))
    def get_precomputed_skeleton_info(
        self,
        skvn: int = 0,
        datastack_name: Optional[str] = None,
    ):
        """get's the precomputed skeleton information
        Args:
            datastack_name (Optional[str], optional): _description_. Defaults to None.
        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["skvn"] = skvn
        url = self._endpoints["skeleton_info_versioned"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_skeleton(
        self,
        root_id: int,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 0,
        output_format: Literal[
            "none",
            "h5",
            "swc",
            "json",
            "jsoncompressed",
            "arrays",
            "arrayscompressed",
            "precomputed",
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
            The format to retrieve. Options are:

            - 'none': No return value (this can be used to generate a skeleton without retrieving it)
            - 'precomputed': A cloudvolume.Skeleton object
            - 'json': A dictionary
            - 'jsoncompressed': A dictionary using compression for transmission (generally faster than 'json')
            - 'arrays': A dictionary (literally a subset of the json response)
            - 'arrayscompressed': A dictionary using compression for transmission (generally faster than 'arrays')
            - 'swc': A pandas DataFrame
            - 'h5': An BytesIO object containing bytes for an h5 file

        Returns
        -------
        :
            Skeleton of the requested type. See `output_format` for details.

        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        url = self.build_endpoint(
            root_id, datastack_name, skeleton_version, output_format
        )

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        if output_format == "none":
            return
        if output_format == "precomputed":
            if not CLOUDVOLUME_AVAILABLE:
                raise ImportError(
                    "'precomputed' output format requires cloudvolume, which is not available."
                )
            metadata = self.get_precomputed_skeleton_info(
                skeleton_version, datastack_name
            )
            vertex_attributes = metadata["vertex_attributes"]
            return cloudvolume.Skeleton.from_precomputed(
                response.content, vertex_attributes=vertex_attributes
            )
        if output_format == "json":
            return response.json()
        if output_format == "jsoncompressed":
            return SkeletonClient.decompressBytesToDict(response.content)
        if output_format == "arrays":
            return response.json()
        if output_format == "arrayscompressed":
            return SkeletonClient.decompressBytesToDict(response.content)
        if output_format == "swc":
            # I got the SWC column header from skeleton_plot.skel_io.py
            df = pd.read_csv(
                StringIO(response.content.decode()),
                sep=" ",
                names=["id", "type", "x", "y", "z", "radius", "parent"],
            )

            # Reduce 'id' and 'parent' columns from int64 to int16, and 'type' column from int64 to int8
            df = df.apply(pd.to_numeric, downcast="integer")
            # Convert 'type' column from int8 to uint8
            df["type"] = df["type"].astype("uint8")

            # Reduce float columns from float64 to float32. This sacrifies precision and therefore is perhaps undesirable.
            # I have it left here, commented out, for demonstration purposes, should it be deemed desirable in the future.
            # df = df.apply(pd.to_numeric, downcast='float')

            return df
        if output_format == "h5":
            skeleton_bytesio = BytesIO(response.content)
            return skeleton_bytesio

        raise ValueError(f"Unknown output format: {output_format}")
