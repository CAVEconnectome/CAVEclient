import json
from typing import Optional, Union
from urllib.parse import urlparse
from warnings import warn

import numpy as np
import pandas as pd
from requests.exceptions import HTTPError

from .auth import AuthClient
from .base import BaseEncoder, ClientBase, _api_endpoints, handle_response
from .endpoints import (
    l2cache_api_versions,
    l2cache_endpoints_common,
)

SERVER_KEY = "l2cache_server_address"


def _flatten_rep_coord(x):
    if isinstance(x, float):
        x = np.empty(3)
    return pd.Series(x, index=["rep_coord_nm_x", "rep_coord_nm_y", "rep_coord_nm_z"])


def _flatten_pca(x):
    if isinstance(x, float):
        x = np.empty((3, 3))
    return pd.Series(
        np.array(x).flatten(),
        index=[
            "pca_0_x",
            "pca_0_y",
            "pca_0_z",
            "pca_1_x",
            "pca_1_y",
            "pca_1_z",
            "pca_2_x",
            "pca_2_y",
            "pca_2_z",
        ],
    )


def _flatten_pca_val(x):
    if isinstance(x, float):
        x = np.empty(3)
    return pd.Series(x, index=["pca_val_0", "pca_val_1", "pca_val_2"])


def _flatten_chunk_intersect_count(x):
    if isinstance(x, float):
        x = np.empty((2, 3))
    return pd.Series(
        np.array(x).flatten(),
        index=[
            "chunk_intersect_count_x_bottom",
            "chunk_intersect_count_y_bottom",
            "chunk_intersect_count_z_bottom",
            "chunk_intersect_count_x_top",
            "chunk_intersect_count_y_top",
            "chunk_intersect_count_z_top",
        ],
    )


class L2CacheClient(ClientBase):
    """Client for interacting with the level2 cache service."""

    def __init__(
        self,
        server_address=None,
        table_name=None,
        auth_client=None,
        api_version="latest",
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
        verify=True,
    ):
        if auth_client is None:
            auth_client = AuthClient()

        auth_header = auth_client.request_header
        endpoints, api_version = _api_endpoints(
            api_version,
            SERVER_KEY,
            server_address,
            l2cache_endpoints_common,
            l2cache_api_versions,
            auth_header,
        )
        super(L2CacheClient, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            SERVER_KEY,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
            verify=verify,
        )
        self._default_url_mapping["table_id"] = table_name
        self._available_attributes = None
        self._table_mapping = None

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def get_l2data(
        self, l2_ids: Union[list, np.ndarray], attributes: Optional[list] = None
    ) -> dict:
        """
        Gets the attributed statistics data for L2 ids.

        Parameters
        ----------
        l2_ids : list or np.ndarray
            A list of level 2 ids.
        attributes : list, optional
            A list of attributes to retrieve. Defaults to `None`, which will return all
            that are available. Available stats are:

            - `area_nm2`
            - `chunk_intersect_count`
            - `max_dt_nm`
            - `mean_dt_nm`
            - `pca`
            - `pca_val`
            - `rep_coord_nm`
            - `size_nm3`

            See [the tutorial](../../tutorials/l2cache) for more description.

        Returns
        -------
        dict
            Keys are l2 ids, values are data.
        """

        query_d = {"int64_as_str": False}

        if attributes is not None:
            query_d["attribute_names"] = ",".join(attributes)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["l2cache_data"].format_map(endpoint_mapping)

        response = self.session.post(
            url,
            data=json.dumps(
                {"l2_ids": l2_ids},
                cls=BaseEncoder,
            ),
            params=query_d,
        )
        return handle_response(response)

    def get_l2data_table(
        self,
        l2_ids: Union[list, np.ndarray],
        attributes: Optional[list] = None,
        split_columns: bool = True,
    ) -> pd.DataFrame:
        """
        Gets the attributed statistics data for L2 ids, returned as a dataframe.

        Parameters
        ----------
        l2_ids : list or np.ndarray
            A list of level 2 ids.
        attributes : list, optional
            A list of attributes to retrieve. Defaults to `None`, which will return all
            that are available. Available stats are:

            - `area_nm2`
            - `chunk_intersect_count`
            - `max_dt_nm`
            - `mean_dt_nm`
            - `pca`
            - `pca_val`
            - `rep_coord_nm`
            - `size_nm3`

            See [the tutorial](../../tutorials/l2cache) for more description.

        split_columns : bool, optional
            Whether to split columns with multiple values into separate columns.

        Returns
        -------
        :
            A pandas dataframe with the requested attributes as columns and indexed by l2_id.
        """

        data = self.get_l2data(l2_ids, attributes=attributes)
        df = pd.DataFrame(data).T
        df.index.name = "l2_id"
        df.index = df.index.astype(int)

        if split_columns:
            append = pd.DataFrame(index=df.index)
            if "rep_coord_nm" in df.columns:
                append = append.join(df["rep_coord_nm"].apply(_flatten_rep_coord))
            if "pca" in df.columns:
                append = append.join(df["pca"].apply(_flatten_pca))
            if "pca_val" in df.columns:
                append = append.join(df["pca_val"].apply(_flatten_pca_val))
            if "chunk_intersect_count" in df.columns:
                append = append.join(
                    df["chunk_intersect_count"].apply(_flatten_chunk_intersect_count)
                )
            df = df.join(append)
            df.drop(
                columns=df.columns.intersection(
                    ["rep_coord_nm", "pca", "pca_val" "chunk_intersect_count"]
                ),
                inplace=True,
            )

        return df

    def cache_metadata(self):
        """Retrieves the meta data for the cache

        Returns
        -------
        dict
            keys are attribute names, values are datatypes
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["l2cache_meta"].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    @property
    def attributes(self):
        if self._available_attributes is None:
            self._available_attributes = list(self.cache_metadata().keys())
        return self._available_attributes

    def table_mapping(self):
        """Retrieves table mappings for l2 cache.

        Returns
        -------
        dict
            keys are pcg table names, values are dicts with fields `l2cache_id` and `cv_path`.
        """
        if self._table_mapping is None:
            endpoint_mapping = self.default_url_mapping
            url = self._endpoints["l2cache_table_mapping"].format_map(endpoint_mapping)
            response = self.session.get(url)
            self._table_mapping = handle_response(response)
        return self._table_mapping

    def has_cache(self, datastack_name=None):
        """Checks if the l2 cache is available for the dataset

        Parameters
        ----------
        datastack_name : str, optional
            The name of the datastack to check, by default None (if None, uses the client's datastack)

        Returns
        -------
        bool
            True if the l2 cache is available, False otherwise
        """
        seg_source = self.fc.info.segmentation_source(datastack_name=datastack_name)
        if urlparse(seg_source).scheme != "graphene":
            return False
        table_name = self.fc.chunkedgraph.table_name
        try:
            table_mapping = self.table_mapping()
        except HTTPError as e:
            if e.response.status_code == 404:
                warn(
                    f"L2cache deployment '{self.server_address}/l2cache' does not have a l2 cache table mapping. Assuming no cache."
                )
                return False
            else:
                raise e
        return table_name in table_mapping
