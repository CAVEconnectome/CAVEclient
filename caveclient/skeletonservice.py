from __future__ import annotations

import binascii
import gzip
import io
import json
import logging
from io import BytesIO, StringIO
from typing import List, Literal, Optional, Union

import pandas as pd
from cachetools import TTLCache, cached
from packaging.version import Version

from .auth import AuthClient
from .base import (
    ClientBase,
    _api_endpoints,
    _check_version_compatibility,
)
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
            url = parse(self._build_endpoint(rid, None, None, "precomputed"))
            assert url == f"{self._datastack_name}{innards}{rid}"

            url = parse(self._build_endpoint(rid, None, None, "json"))
            assert url == f"{self._datastack_name}{innards}0/{rid}/json"

        url = parse(self._build_endpoint(rid, ds, None, "precomputed"))
        assert url == f"{ds}{innards}{rid}"

        url = parse(self._build_endpoint(rid, ds, None, "json"))
        assert url == f"{ds}{innards}0/{rid}/json"

        url = parse(self._build_endpoint(rid, ds, 0, "precomputed"))
        assert url == f"{ds}{innards}0/{rid}"

        url = parse(self._build_endpoint(rid, ds, 0, "json"))
        assert url == f"{ds}{innards}0/{rid}/json"

        url = parse(self._build_endpoint(rid, ds, 1, "precomputed"))
        assert url == f"{ds}{innards}1/{rid}"

        url = parse(self._build_endpoint(rid, ds, 1, "json"))
        assert url == f"{ds}{innards}1/{rid}/json"

    def get_version(self):
        logging.info("get_version()")
        endpoint_mapping = self.default_url_mapping
        endpoint = self._endpoints.get("get_version", None)
        logging.info(f"endpoint: {endpoint}")

        url = endpoint.format_map(endpoint_mapping)
        logging.info(f"url: {url}")
        response = self.session.get(url)
        logging.info(f"response: {response}")
        if response.status_code == 404:  # server doesn't have this endpoint yet
            logging.info("404")
            return None
        else:
            version_str = response.json()
            logging.info(f"version_str: {type(version_str)} {version_str}")
            version = Version(version_str)
            logging.info(f"version: {version}")
            return version

    def get_versions(self):
        logging.info("get_versions()")
        endpoint_mapping = self.default_url_mapping
        endpoint = self._endpoints.get("get_versions", None)
        logging.info(f"endpoint: {endpoint}")

        url = endpoint.format_map(endpoint_mapping)
        logging.info(f"url: {url}")
        response = self.session.get(url)
        logging.info(f"response: {response}")
        if response.status_code == 404:  # server doesn't have this endpoint yet
            logging.info("404")
            return None
        else:
            versions = response.json()
            logging.info(f"versions: {type(versions)} {versions}")
            return versions

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

    def _build_endpoint(
        self,
        root_id: int,
        datastack_name: str,
        skeleton_version: int,
        output_format: str,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_id"] = root_id

        if not skeleton_version:
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

    def _build_bulk_endpoint(
        self,
        root_ids: List,
        datastack_name: str,
        skeleton_version: int,
        output_format: str,
        generate_missing_sks: bool,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_ids"] = ",".join([str(v) for v in root_ids])
        endpoint_mapping["output_format"] = output_format
        endpoint_mapping["gen_missing_sks"] = generate_missing_sks

        if not skeleton_version:
            endpoint = "get_bulk_skeletons_via_rids"
        else:
            endpoint_mapping["skeleton_version"] = skeleton_version
            endpoint = "get_bulk_skeletons_via_skvn_rids"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        return url

    def _build_bulk_async_endpoint(
        self,
        root_ids: List,
        datastack_name: str,
        skeleton_version: int,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_ids"] = ",".join([str(v) for v in root_ids])

        if not skeleton_version:
            endpoint = "gen_bulk_skeletons_via_rids"
        else:
            endpoint_mapping["skeleton_version"] = skeleton_version
            endpoint = "gen_bulk_skeletons_via_skvn_rids"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        return url

    @_check_version_compatibility(method_constraint=">=0.5.9")
    def get_cache_contents(
        self,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 0,
        root_id_prefixes: Union[int, str, List] = 0,
        limit: Optional[int] = 0,
        log_warning: bool = True,
    ):
        """
        Mirror CloudFiles.list() for skeletons as a pass-through interface to the underlying service and bucket.
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        if isinstance(root_id_prefixes, int):
            root_id_prefixes = str(root_id_prefixes)
        elif isinstance(root_id_prefixes, List):
            root_id_prefixes = ",".join([str(v) for v in root_id_prefixes])

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_id_prefixes"] = root_id_prefixes
        endpoint_mapping["limit"] = limit

        if not skeleton_version:
            url = self._endpoints["get_cache_contents_via_ridprefixes"].format_map(
                endpoint_mapping
            )
        else:
            endpoint_mapping["skeleton_version"] = skeleton_version
            url = self._endpoints["get_cache_contents_via_skvn_ridprefixes"].format_map(
                endpoint_mapping
            )

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        return response.json()

    @_check_version_compatibility(method_constraint=">=0.5.10")
    def skeletons_exist(
        self,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 0,
        root_ids: Union[int, str, List] = 0,
        log_warning: bool = True,
    ):
        """
        Confirm or deny that a set of root ids have H5 skeletons in the cache.
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        if isinstance(root_ids, int):
            root_ids = str(root_ids)
        elif isinstance(root_ids, List):
            root_ids = ",".join([str(v) for v in root_ids])

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_ids"] = root_ids

        if not skeleton_version:
            url = self._endpoints["skeletons_exist_via_rids"].format_map(
                endpoint_mapping
            )
        else:
            endpoint_mapping["skeleton_version"] = skeleton_version
            url = self._endpoints["skeletons_exist_via_skvn_rids"].format_map(
                endpoint_mapping
            )

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        result_json = response.json()
        if isinstance(result_json, bool):
            # When investigating a single root id, this returns a single bool, not a dict, list, etc.
            return result_json
        result_json_w_ints = {int(key): value for key, value in result_json.items()}
        return result_json_w_ints

    @cached(TTLCache(maxsize=32, ttl=3600))
    def get_precomputed_skeleton_info(
        self,
        skvn,
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
        skeleton_version: Optional[int] = 3,
        output_format: Literal[
            "dict",
            "swc",
        ] = "dict",
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """Gets basic skeleton information for a datastack

        Parameters
        ----------
        root_id : int
            The root id of the skeleton to retrieve
        datastack_name : str
            The name of the datastack to check
        skeleton_version : int
            The skeleton version to generate and retrieve. Options are documented in SkeletonService. Use 0 for Neuroglancer-compatibility. Use -1 for latest.
        output_format : string
            The format to retrieve. Options are:
            - 'dict': A dictionary
            - 'swc': A pandas DataFrame

        Returns
        -------
        :
            Skeleton of the requested type. See `output_format` for details.

        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        valid_output_formats = ["dict", "swc"]
        if output_format not in valid_output_formats:
            raise ValueError(
                f"Unknown output format: {output_format}. Valid options: {valid_output_formats}"
            )

        # The output formats were changed in server v0.6.0 and must be handled differently by the client
        if output_format == "dict":
            if self._server_version < Version("0.6.0"):
                endpoint_format = "jsoncompressed"
            else:
                endpoint_format = "flatdict"
        elif output_format == "swc":
            endpoint_format = "swccompressed"

        valid_skeleton_versions = [-1, 0, 1, 2, 3]
        if skeleton_version not in valid_skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {valid_skeleton_versions}"
            )

        if verbose_level >= 1:
            logging.info(f"SkeletonService version: {self._server_version}")
        if self._server_version < Version("0.6.0"):
            logging.warning(
                "The optional nature of the 'skeleton_version' parameter will be deprecated in the future. Please specify a skeleton version."
            )

        # -1, to specify the latest version, was only added in server v0.6.1
        if self._server_version < Version("0.6.1") and skeleton_version == -1:
            skeleton_versions = self.get_versions()
            skeleton_version = sorted(skeleton_versions)[-1]

        url = self._build_endpoint(
            root_id, datastack_name, skeleton_version, endpoint_format
        )

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        if verbose_level >= 1:
            logging.info(
                f"get_skeleton() response contains content of size {len(response.content)} bytes"
            )

        if endpoint_format == "jsoncompressed":
            assert self._server_version < Version("0.6.0")
            sk_json = SkeletonClient.decompressBytesToDict(response.content)
            if "vertex_properties" in sk_json.keys():
                for key in sk_json["vertex_properties"].keys():
                    # Radius was redundantly store both as a top-level parameter and in vertex_properties.
                    # We could either check for it (or any such redundancy key) and skip over it, or we could overwrite it.
                    # Since they were created as duplicates anyway, it doesn't matter which approach is used.
                    sk_json[key] = sk_json["vertex_properties"][key]
                del sk_json["vertex_properties"]
            return sk_json
        if endpoint_format == "flatdict":
            assert self._server_version >= Version("0.6.0")
            return SkeletonClient.decompressBytesToDict(response.content)
        if endpoint_format == "swccompressed":
            file_content = SkeletonClient.decompressBytesToString(response.content)

            # I got the SWC column header from skeleton_plot.skel_io.py
            df = pd.read_csv(
                StringIO(file_content),
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

        raise ValueError(f"Unknown output format: {output_format}")

    @_check_version_compatibility(method_constraint=">=0.5.9")
    def get_bulk_skeletons(
        self,
        root_ids: List,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 3,
        output_format: Literal[
            "dict",
            "swc",
        ] = "dict",
        generate_missing_skeletons: bool = False,
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """Generates skeletons for a list of root ids without retrieving them.

        Parameters
        ----------
        root_ids : List
            A list of root ids of the skeletons to generate
        datastack_name : str
            The name of the datastack to check
        skeleton_version : int
            The skeleton version to generate. Use 0 for Neuroglancer-compatibility. Use -1 for latest.
        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        valid_output_formats = ["dict", "swc"]
        if output_format not in valid_output_formats:
            raise ValueError(
                f"Unknown output format: {output_format}. Valid options: {valid_output_formats}"
            )

        # The output formats were changed in server v0.6.0 and must be handled differently by the client
        if output_format == "dict":
            if self._server_version < Version("0.6.0"):
                endpoint_format = "jsoncompressed"
            else:
                endpoint_format = "flatdict"
        elif output_format == "swc":
            endpoint_format = "swccompressed"

        valid_skeleton_versions = [-1, 0, 1, 2, 3]
        if skeleton_version not in valid_skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {valid_skeleton_versions}"
            )

        url = self._build_bulk_endpoint(
            root_ids,
            datastack_name,
            skeleton_version,
            endpoint_format,
            generate_missing_skeletons,
        )
        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        if verbose_level >= 1:
            logging.info(
                f"Generated skeletons for root_ids {root_ids} (with generate_missing_skeletons={generate_missing_skeletons})"
            )

        if endpoint_format == "flatdict":
            sk_jsons = {}
            for rid, swc_bytes in response.json().items():
                try:
                    sk_json = SkeletonClient.decompressBytesToDict(
                        io.BytesIO(binascii.unhexlify(swc_bytes)).getvalue()
                    )
                    sk_jsons[rid] = sk_json
                except Exception as e:
                    logging.error(
                        f"Error decompressing skeleton for root_id {rid}: {e}"
                    )
            return sk_jsons
        elif endpoint_format == "swc":
            sk_dfs = {}
            for rid, swc_bytes in response.json().items():
                try:
                    sk_csv = (
                        io.BytesIO(binascii.unhexlify(swc_bytes)).getvalue().decode()
                    )
                    # I got the SWC column header from skeleton_plot.skel_io.py
                    sk_df = pd.read_csv(
                        StringIO(sk_csv),
                        sep=" ",
                        names=["id", "type", "x", "y", "z", "radius", "parent"],
                    )
                    sk_dfs[rid] = sk_df
                except Exception as e:
                    logging.error(
                        f"Error decompressing skeleton for root_id {rid}: {e}"
                    )
            return sk_dfs

    @_check_version_compatibility(method_constraint=">=0.5.9")
    def generate_bulk_skeletons_async(
        self,
        root_ids: List,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = None,
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """Generates skeletons for a list of root ids without retrieving them.

        Parameters
        ----------
        root_ids : List
            A list of root ids of the skeletons to generate
        datastack_name : str
            The name of the datastack to check
        skeleton_version : int
            The skeleton version to generate. Use 0 for Neuroglancer-compatibility. Use -1 for latest.
        """
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        if skeleton_version is None:
            logging.warning(
                "The optional nature of the 'skeleton_version' parameter will be deprecated in the future. Please specify a skeleton version."
            )
            skeleton_version = -1

        url = self._build_bulk_async_endpoint(
            root_ids, datastack_name, skeleton_version
        )
        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        estimated_async_time_secs_upper_bound = float(response.text)

        if verbose_level >= 1:
            logging.info(
                f"Queued asynchronous skeleton generation for root_ids: {root_ids}"
            )
            logging.info(
                f"Upper estimate to generate {len(root_ids)} skeletons: {estimated_async_time_secs_upper_bound} seconds"
            )

        return f"Upper estimate to generate {len(root_ids)} skeletons: {estimated_async_time_secs_upper_bound} seconds"
