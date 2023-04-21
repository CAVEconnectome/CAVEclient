from .base import ClientBase, _api_endpoints, handle_response, BaseEncoder
from .endpoints import (
    l2cache_api_versions,
    l2cache_endpoints_common,
)
from .auth import AuthClient
import json

server_key = "l2cache_server_address"


def L2CacheClient(
    server_address=None,
    table_name=None,
    auth_client=None,
    api_version="latest",
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
        server_key,
        server_address,
        l2cache_endpoints_common,
        l2cache_api_versions,
        auth_header,
    )
    L2client = client_mapping[api_version]
    return L2client(
        server_address=server_address,
        auth_header=auth_header,
        api_version=api_version,
        endpoints=endpoints,
        server_name=server_key,
        table_name=table_name,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
    )


class L2CacheClientLegacy(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        table_name=None,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
    ):
        super(L2CacheClientLegacy, self).__init__(
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
        self._default_url_mapping["table_id"] = table_name
        self._available_attributes = None

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def get_l2data(self, l2_ids, attributes=None):
        """Gets the attributed statistics data for L2 ids.

        Args:
            l2_ids (list or np.ndarray): a list of level 2 ids
            attributes (list, optional): a list of attributes to retrieve.
                Defaults to None which will return all that are available.
                Available stats are ['area_nm2', 'chunk_intersect_count', 'max_dt_nm', 'mean_dt_nm', 'pca', 'pca_val', 'rep_coord_nm', 'size_nm3']. See docs for more description.

        Returns:
            dict: keys are l2 ids, values are data
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

    def cache_metadata(self):
        """Retrieves the meta data for the cache

        Parameters
        ----------

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


client_mapping = {
    1: L2CacheClientLegacy,
    "latest": L2CacheClientLegacy,
}
