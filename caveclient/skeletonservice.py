from __future__ import annotations

import binascii
import gzip
import io
import json
import logging
from io import BytesIO, StringIO
from timeit import default_timer
from typing import List, Literal, Optional, Union

import numpy as np
import pandas as pd
from cachetools import TTLCache, cached
from packaging.version import Version

from .auth import AuthClient
from .base import (
    ClientBase,
    _api_endpoints,
    _check_version_compatibility,
    handle_response,
)
from .endpoints import skeletonservice_api_versions, skeletonservice_common

SERVER_KEY = "skeleton_server_address"

MAX_SKELETONS_EXISTS_QUERY_SIZE = 1000
MAX_BULK_SYNCHRONOUS_SKELETONS = 10
MAX_BULK_ASYNCHRONOUS_SKELETONS = 10000
BULK_SKELETONS_BATCH_SIZE = 1000


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

    def _build_skeletons_exist_endpoint(
        self,
        root_ids: List,
        datastack_name: str,
        skeleton_version: int,
        verbose_level: int,
        post: bool,
    ):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        if not post:
            endpoint_mapping["root_ids"] = ",".join([str(v) for v in root_ids])
            endpoint_mapping["skeleton_version"] = skeleton_version
            endpoint = "skeletons_exist_via_skvn_rids"
        else:
            endpoint = "skeletons_exist_via_skvn_rids_as_post"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        url += f"?verbose_level={verbose_level}"
        return url

    def _build_get_skeleton_endpoint(
        self,
        root_id: int,
        datastack_name: str,
        skeleton_version: int,
        output_format: str,
        async_: bool,
        verbose_level: int,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        assert skeleton_version is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_id"] = root_id
        endpoint_mapping["skeleton_version"] = skeleton_version
        endpoint_mapping["output_format"] = output_format

        if not async_:
            endpoint = "get_skeleton_via_skvn_rid_fmt"
        else:
            endpoint = "get_skeleton_async_via_skvn_rid_fmt"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        url += f"?verbose_level={verbose_level}"
        return url

    def _build_bulk_endpoint(
        self,
        root_ids: List,
        datastack_name: str,
        skeleton_version: int,
        output_format: str,
        generate_missing_sks: bool,
        verbose_level: int,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        assert skeleton_version is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_ids"] = ",".join([str(v) for v in root_ids])
        endpoint_mapping["output_format"] = output_format
        endpoint_mapping["gen_missing_sks"] = generate_missing_sks
        endpoint_mapping["skeleton_version"] = skeleton_version

        endpoint = "get_bulk_skeletons_via_skvn_rids"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        url += f"?verbose_level={verbose_level}"
        return url

    def _build_bulk_async_endpoint(
        self,
        root_ids: List,
        datastack_name: str,
        skeleton_version: int,
        verbose_level: int,
        post: bool,
    ):
        """
        Building the URL in a separate function facilitates testing
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        assert skeleton_version is not None

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        if not post:
            # TODO: DEPRECATED: This endpoint is deprecated and will be removed in the future.
            # Please use the POST endpoint in the future.
            endpoint_mapping["root_ids"] = ",".join([str(v) for v in root_ids])

            endpoint_mapping["skeleton_version"] = skeleton_version

            endpoint = "gen_bulk_skeletons_via_skvn_rids"
        else:
            endpoint = "gen_bulk_skeletons_via_skvn_rids_as_post"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        url += f"?verbose_level={verbose_level}"
        return url

    @_check_version_compatibility(method_constraint=">=0.5.9")
    def get_cache_contents(
        self,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 3,
        root_id_prefixes: Union[int, str, List] = 0,
        limit: Optional[int] = 0,
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """
        Mirror CloudFiles.list() for skeletons as a pass-through interface to the underlying service and bucket.
        """
        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        skeleton_versions = self.get_versions()
        if skeleton_version not in skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {skeleton_versions}"
            )

        if isinstance(root_id_prefixes, int):
            root_id_prefixes = str(root_id_prefixes)
        elif isinstance(root_id_prefixes, List):
            root_id_prefixes = ",".join([str(v) for v in root_id_prefixes])

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["root_id_prefixes"] = root_id_prefixes
        endpoint_mapping["limit"] = limit
        endpoint_mapping["skeleton_version"] = skeleton_version

        endpoint = "get_cache_contents_via_skvn_ridprefixes_limit"

        url = self._endpoints[endpoint].format_map(endpoint_mapping)
        url += f"?verbose_level={verbose_level}"

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        logging.info(
            f"get_cache_contents() response contains content of size {len(response.content)} bytes"
        )

        return response.json()

    @_check_version_compatibility(method_constraint=">=0.5.10")
    def skeletons_exist(
        self,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 3,
        root_ids: Union[int, str, List] = 0,
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """
        Confirm or deny that a set of root ids have H5 skeletons in the cache.
        """
        logging.info(f"SkeletonService version: {self._server_version}")
        if self._server_version < Version("0.9.0"):
            logging.warning(
                "Server version is old and only supports GET interactions for bulk async skeletons. Consider upgrading to a newer server version to enable POST interactions."
            )

        if datastack_name is None:
            datastack_name = self._datastack_name
        assert datastack_name is not None

        skeleton_versions = self.get_versions()
        if skeleton_version not in skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {skeleton_versions}"
            )

        if isinstance(root_ids, int):
            root_ids = str(root_ids)
        if isinstance(root_ids, np.ndarray):
            root_ids = root_ids.tolist()
        if not isinstance(
            root_ids, List
        ):  # If not a list, it can only be a string at this point
            root_ids = [root_ids]

        if len(root_ids) > MAX_SKELETONS_EXISTS_QUERY_SIZE:
            logging.warning(
                f"The number of root_ids exceeds the current limit of {MAX_SKELETONS_EXISTS_QUERY_SIZE}. Only the first {MAX_SKELETONS_EXISTS_QUERY_SIZE} will be processed."
            )
            root_ids = root_ids[:MAX_SKELETONS_EXISTS_QUERY_SIZE]

        results = {}
        for batch in range(0, len(root_ids), BULK_SKELETONS_BATCH_SIZE):
            rids_one_batch = root_ids[batch : batch + BULK_SKELETONS_BATCH_SIZE]

            use_post = self._server_version >= Version("0.9.0")
            url = self._build_skeletons_exist_endpoint(
                rids_one_batch,
                datastack_name,
                skeleton_version,
                verbose_level,
                use_post,
            )

            if self._server_version < Version("0.9.0"):
                response = self.session.get(url)
                self.raise_for_status(response, log_warning=log_warning)

                logging.info(
                    f"skeletons_exist() response contains content of size {len(response.content)} bytes"
                )
            else:
                data = {
                    "root_ids": rids_one_batch,
                    "skeleton_version": skeleton_version,
                    "verbose_level": verbose_level,
                }
                response = self.session.post(url, json=data)
                response = handle_response(response, as_json=False)

            result_json = response.json()
            if isinstance(result_json, dict):
                # Convert string keys to ints
                results.update({int(key): value for key, value in result_json.items()})
            elif isinstance(result_json, bool):
                assert len(rids_one_batch) == 1
                results[int(rids_one_batch[0])] = result_json
            else:
                raise ValueError(f"Unexpected response type: {type(result_json)}")

        if len(results) == 1:
            # When investigating a single root id, this returns a single bool, not a dict, list, etc.
            return list(results.values())[0]
        return results

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
        endpoint_mapping["skeleton_version"] = skvn
        url = self._endpoints["skeleton_info_versioned"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)

        logging.info(
            f"get_precomputed_skeleton_info() response contains content of size {len(response.content)} bytes"
        )

        return response.json()

    def get_skeleton(
        self,
        root_id: int,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 4,
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

        skeleton_versions = self.get_versions()
        if skeleton_version not in skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {skeleton_versions}"
            )

        logging.info(f"SkeletonService version: {self._server_version}")
        if self._server_version < Version("0.6.0"):
            logging.warning(
                "The optional nature of the 'skeleton_version' parameter will be deprecated in the future. Please specify a skeleton version."
            )

        # -1, to specify the latest version, was only added in server v0.6.1
        if self._server_version < Version("0.6.1") and skeleton_version == -1:
            skeleton_versions = self.get_versions()
            skeleton_version = sorted(skeleton_versions)[-1]

        async_ = True
        if self._server_version < Version("0.13.7"):
            async_ = False
            logging.warning(
                "Skeleton version is old and does not support asynchronous skeletonization. Please specify a skeleton version."
            )
        
        cv = self.fc.info.segmentation_cloudvolume()
        if cv and cv.meta.decode_layer_id(root_id) != cv.meta.n_layers:
            raise ValueError(f"Invalid root id: {root_id} (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)")
        if not self.fc.chunkedgraph.is_valid_nodes(root_id):
            raise ValueError(f"Invalid root id: {root_id} (perhaps it doesn't exist; the error is unclear)")
        
        url = self._build_get_skeleton_endpoint(
            root_id,
            datastack_name,
            skeleton_version,
            endpoint_format,
            async_,
            verbose_level,
        )

        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

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
            sk_json = SkeletonClient.decompressBytesToDict(response.content)
            if "edges" in sk_json.keys():
                sk_json["edges"] = np.array(sk_json["edges"])
            if "mesh_to_skel_map" in sk_json.keys():
                sk_json["mesh_to_skel_map"] = np.array(sk_json["mesh_to_skel_map"])
            if "vertices" in sk_json.keys():
                sk_json["vertices"] = np.array(sk_json["vertices"])
            if "lvl2_ids" in sk_json.keys():
                sk_json["lvl2_ids"] = np.array(sk_json["lvl2_ids"])
            if "radius" in sk_json.keys():
                sk_json["radius"] = np.array(sk_json["radius"])
            if "compartment" in sk_json.keys():
                sk_json["compartment"] = np.array(sk_json["compartment"])
            return sk_json
        if endpoint_format == "swccompressed":
            file_content = SkeletonClient.decompressBytesToString(response.content)

            # I got the SWC column header from skeleton_plot.skel_io.py
            df = pd.read_csv(
                StringIO(file_content),
                sep=" ",
                names=["id", "type", "x", "y", "z", "radius", "parent"],
            )
            
            return df

        raise ValueError(f"Unknown output format: {output_format}")

    @_check_version_compatibility(method_constraint=">=0.5.9")
    def get_bulk_skeletons(
        self,
        root_ids: List,
        datastack_name: Optional[str] = None,
        skeleton_version: Optional[int] = 4,
        output_format: Literal[
            "dict",
            "swc",
        ] = "dict",
        generate_missing_skeletons: bool = False,
        log_warning: bool = True,
        verbose_level: Optional[int] = 0,
    ):
        """Generates skeletons for a list of root ids in a "small" bulk (ten at the time of this writing). Use the async interface for larger bulk requests.

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

        logging.info(f"SkeletonService version: {self._server_version}")

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

        skeleton_versions = self.get_versions()
        if skeleton_version not in skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {skeleton_versions}"
            )

        valid_rids = []
        cv = self.fc.info.segmentation_cloudvolume()
        for rid in root_ids:
            if cv and cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
                logging.warning(f"Invalid root id: {rid} (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id). It won't be processed.")
                continue
            if not self.fc.chunkedgraph.is_valid_nodes(rid):
                logging.warning(f"Invalid root id: {rid} (perhaps it doesn't exist; the error is unclear). It won't be processed.")
                continue
            valid_rids.append(rid)
        if not valid_rids:
            logging.error("No valid root ids were submitted.")
            return {}
        root_ids = valid_rids

        if len(root_ids) > MAX_BULK_SYNCHRONOUS_SKELETONS:
            root_ids = root_ids[:MAX_BULK_SYNCHRONOUS_SKELETONS]
            if verbose_level >= 1:
                logging.warning(f"Truncating bulk skeleton list to {MAX_BULK_SYNCHRONOUS_SKELETONS}")

        url = self._build_bulk_endpoint(
            root_ids,
            datastack_name,
            skeleton_version,
            endpoint_format,
            generate_missing_skeletons,
            verbose_level,
        )
        response = self.session.get(url)
        self.raise_for_status(response, log_warning=log_warning)

        logging.info(
            f"get_bulk_skeletons() response contains content of size {len(response.content)} bytes"
        )

        logging.info(
            f"Generated skeletons for root_ids {root_ids} (with generate_missing_skeletons={generate_missing_skeletons})"
        )

        if endpoint_format == "flatdict":
            sk_jsons = {}
            for rid, dict_bytes in response.json().items():
                if dict_bytes not in ["async", "invalid_rid", "invalid_layer_rid"]:
                    try:
                        sk_json = SkeletonClient.decompressBytesToDict(
                            io.BytesIO(binascii.unhexlify(dict_bytes)).getvalue()
                        )
                        sk_jsons[rid] = sk_json
                    except Exception as e:
                        logging.error(
                            f"Error decompressing skeleton for root_id {rid}: {e}"
                        )
            return sk_jsons
        elif endpoint_format == "swccompressed":
            sk_dfs = {}
            for rid, swc_bytes in response.json().items():
                if swc_bytes not in ["async", "invalid_rid", "invalid_layer_rid"]:
                    try:
                        sk_csv = (
                            io.BytesIO(binascii.unhexlify(swc_bytes))
                            .getvalue()
                            .decode()
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

        Returns
        -------
        float
            The estimated time in seconds to generate all skeletons (a comparable message will be output to the console prior to return).
        """
        t0 = default_timer()
        
        if not self.fc.l2cache.has_cache():
            raise NoL2CacheException("SkeletonClient requires an L2Cache.")

        logging.info(f"SkeletonService version: {self._server_version}")
        if self._server_version < Version("0.8.0"):
            logging.warning(
                "Server version is old and only supports GET interactions for bulk async skeletons. Consider upgrading to a newer server version to enable POST interactions."
            )

        if skeleton_version is None:
            logging.warning(
                "The optional nature of the 'skeleton_version' parameter will be deprecated in the future. Please specify a skeleton version."
            )
            skeleton_version = 4

        skeleton_versions = self.get_versions()
        if skeleton_version not in skeleton_versions:
            raise ValueError(
                f"Unknown skeleton version: {skeleton_version}. Valid options: {skeleton_versions}"
            )

        if isinstance(root_ids, np.ndarray):
            root_ids = root_ids.tolist()
        if not isinstance(root_ids, list):
            raise ValueError(
                f"root_ids must be a list or numpy array of root_ids, not a {type(root_ids)}"
            )

        t1 = default_timer()

        valid_rids = []
        cv = self.fc.info.segmentation_cloudvolume()
        for rid in root_ids:
            if cv and cv.meta.decode_layer_id(rid) != cv.meta.n_layers:
                logging.warning(f"Invalid root id: {rid} (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id). It won't be processed.")
                continue
            # The following test has been removed, due to its serialized and time-intensive cost.
            # The same test will be performed by the parallelized skeletonization worker later anyway.
            if False:  # not self.fc.chunkedgraph.is_valid_nodes(rid):
                logging.warning(f"Invalid root id: {rid} (perhaps it doesn't exist; the error is unclear). It won't be processed.")
                continue
            valid_rids.append(rid)
        if not valid_rids:
            logging.error("No valid root ids were submitted.")
            return {}
        root_ids = valid_rids

        if len(root_ids) > MAX_BULK_ASYNCHRONOUS_SKELETONS:
            logging.warning(
                f"The number of root_ids exceeds the current limit of {MAX_BULK_ASYNCHRONOUS_SKELETONS}. Only the first {MAX_BULK_ASYNCHRONOUS_SKELETONS} will be processed."
            )
            root_ids = root_ids[:MAX_BULK_ASYNCHRONOUS_SKELETONS]

        # TODO: I recently converted this function to a batched approach to alleviate sending a long URL of root_ids via GET,
        # but have since converted the call to POST, which probably obviates the need for the considerably more complex batch handling.
        # So consider reverting to the unbatched approach in the future.

        t2 = default_timer()

        t3_et = 0
        t4_et = 0
        
        estimated_async_time_secs_upper_bound_sum = 0
        for batch in range(0, len(root_ids), BULK_SKELETONS_BATCH_SIZE):
            t3_0 = default_timer()
            
            rids_one_batch = root_ids[batch : batch + BULK_SKELETONS_BATCH_SIZE]

            use_post = self._server_version >= Version("0.8.0")
            url = self._build_bulk_async_endpoint(
                rids_one_batch,
                datastack_name,
                skeleton_version,
                verbose_level,
                use_post,
            )

            t3 = default_timer()
            t3_et += t3 - t3_0
            
            if self._server_version < Version("0.8.0"):
                response = self.session.get(url)
                self.raise_for_status(response, log_warning=log_warning)

                logging.info(
                    f"generate_bulk_skeletons_async() response contains content of size {len(response.content)} bytes"
                )
            else:
                data = {
                    "root_ids": rids_one_batch,
                    "skeleton_version": skeleton_version,
                    "verbose_level": verbose_level,
                }
                response = self.session.post(url, json=data)
                response = handle_response(response, as_json=False)

            t4 = default_timer()
            t4_et += t4 - t3

            estimated_async_time_secs_upper_bound = float(response.text)
            estimated_async_time_secs_upper_bound_sum += (
                estimated_async_time_secs_upper_bound
            )

            logging.info(
                f"Queued asynchronous skeleton generation for one batch of root_ids: {rids_one_batch}"
            )
            logging.info(
                f"Upper estimate to generate one batch of {len(rids_one_batch)} skeletons: {estimated_async_time_secs_upper_bound} seconds"
            )

        t5_0 = default_timer()

        if estimated_async_time_secs_upper_bound_sum < 60:
            estimate_time_str = (
                f"{estimated_async_time_secs_upper_bound_sum:.0f} seconds"
            )
        elif estimated_async_time_secs_upper_bound_sum < 3600:
            estimate_time_str = (
                f"{(estimated_async_time_secs_upper_bound_sum / 60):.1f} minutes"
            )
        # With a 10000 skeleton limit, the maximum time about 12 hours, so we don't need to check for more than that.
        # elif estimated_async_time_secs_upper_bound_sum < 86400:
        else:
            estimate_time_str = (
                f"{(estimated_async_time_secs_upper_bound_sum / 3600):.1f} hours"
            )
        # else:
        #     estimate_time_str = f"{(estimated_async_time_secs_upper_bound_sum / 86400):.2f} days"

        logging.info(
            f"Upper estimate to generate all {len(root_ids)} skeletons: {estimate_time_str}"
        )

        t5 = default_timer()

        t1_et = t1 - t0
        t2_et = t2 - t1
        t5_et = t5 - t5_0
        logging.info(
            f"generate_bulk_skeletons_async elapsed time: {t1_et:.3f}s {t2_et:.3f}s {t3_et:.3f}s {t4_et:.3f}s {t5_et:.3f}s"
        )

        return estimated_async_time_secs_upper_bound_sum
