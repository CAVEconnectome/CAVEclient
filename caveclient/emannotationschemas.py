from __future__ import annotations

import logging

from requests import HTTPError

from .auth import AuthClient
from .base import ClientBase, _api_endpoints, handle_response
from .endpoints import schema_api_versions, schema_endpoints_common

logger = logging.getLogger(__name__)

SERVER_KEY = "emas_server_address"


class SchemaClient(ClientBase):
    """Client for interacting with the schema service."""

    def __init__(
        self,
        server_address=None,
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
            SERVER_KEY,
            server_address,
            schema_endpoints_common,
            schema_api_versions,
            auth_header,
        )
        super(SchemaClient, self).__init__(
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

    def get_schemas(self) -> list[str]:
        """Get the available schema types

        Returns
        -------
        list
            List of schema types available on the Schema service.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["schema"].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    def schema_definition(self, schema_type: str) -> dict[str]:
        """Get the definition of a specified schema_type

        Parameters
        ----------
        schema_type : str
            Name of a schema_type

        Returns
        -------
        json
            Schema definition
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["schema_type"] = schema_type
        url = self._endpoints["schema_definition"].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    def schema_definition_multi(self, schema_types: list[str]) -> dict:
        """Get the definition of multiple schema_types

        Parameters
        ----------
        schema_types : list
            List of schema names

        Returns
        -------
        dict
            Dictionary of schema definitions. Keys are schema names, values are definitions.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["schema_definition_multi"].format_map(endpoint_mapping)
        data = {"schema_names": ",".join(schema_types)}
        response = self.session.post(url, params=data)
        try:
            return handle_response(response)
        except HTTPError:
            logger.warning(
                'Client requested an schema service endpoint (see "schema_definition_multi") not yet available on your deployment. Please talk to your admin about updating your deployment'
            )
            return None

    def schema_definition_all(self) -> dict[str]:
        """Get the definition of all schema_types

        Returns
        -------
        dict
            Dictionary of schema definitions. Keys are schema names, values are definitions.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["schema_definition_all"].format_map(endpoint_mapping)
        response = self.session.get(url)
        try:
            return handle_response(response)
        except HTTPError:
            logger.warning(
                'Client requested an schema service endpoint (see "schema_definition_all") not yet available on your deployment. Please talk to your admin about updating your deployment'
            )
            return None
