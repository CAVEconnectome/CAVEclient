from __future__ import annotations

from typing import Any, Optional

from .auth import AuthClient
from .base import ClientBase, _api_endpoints, handle_response
from .endpoints import catalogservice_api_versions, catalogservice_common

SERVER_KEY = "catalog_server_address"


class CatalogClient(ClientBase):
    """Client for interacting with the CAVE Catalog service.

    Provides asset registration, discovery, credential vending, and view resolution.
    """

    def __init__(
        self,
        server_address: str,
        datastack_name: str | None = None,
        auth_client: Optional[AuthClient] = None,
        api_version: str = "latest",
        verify: bool = True,
        max_retries: int = None,
        pool_maxsize: int = None,
        pool_block: bool = None,
        over_client=None,
    ):
        if auth_client is None:
            auth_client = AuthClient()

        auth_header = auth_client.request_header
        endpoints, api_version = _api_endpoints(
            api_version,
            SERVER_KEY,
            server_address,
            catalogservice_common,
            catalogservice_api_versions,
            auth_header,
            fallback_version=1,
            verify=verify,
        )

        super().__init__(
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

    @property
    def datastack_name(self) -> str:
        return self._datastack_name

    def _build_url(self, endpoint_key: str, **kwargs) -> str:
        url_mapping = self.default_url_mapping
        url_mapping.update(kwargs)
        return self._endpoints[endpoint_key].format_map(url_mapping)

    def register_asset(
        self,
        name: str,
        uri: str,
        format: str,
        asset_type: str,
        is_managed: bool,
        mat_version: int | None = None,
        revision: int = 1,
        mutability: str = "static",
        maturity: str = "stable",
        properties: dict[str, Any] | None = None,
        access_group: str | None = None,
        expires_at: str | None = None,
    ) -> dict:
        """Register a new asset in the catalog.

        Parameters
        ----------
        name : str
            Asset name.
        uri : str
            Cloud storage URI (e.g. ``gs://bucket/path/``).
        format : str
            Data format (e.g. ``"delta"``, ``"parquet"``, ``"lance"``).
        asset_type : str
            Asset type (e.g. ``"table"``, ``"view"``).
        is_managed : bool
            Whether credentials are managed by the catalog.
        mat_version : int or None
            CAVE materialization version, if applicable.
        revision : int
            Asset revision (default 1).
        mutability : str
            ``"static"`` or ``"mutable"``.
        maturity : str
            ``"stable"``, ``"draft"``, or ``"deprecated"``.
        properties : dict or None
            Arbitrary metadata.
        access_group : str or None
            Optional access control group.
        expires_at : str or None
            ISO-8601 expiry timestamp.

        Returns
        -------
        dict
            The created asset record.
        """
        url = self._build_url("register")
        body = {
            "datastack": self._datastack_name,
            "name": name,
            "mat_version": mat_version,
            "revision": revision,
            "uri": uri,
            "format": format,
            "asset_type": asset_type,
            "is_managed": is_managed,
            "mutability": mutability,
            "maturity": maturity,
            "properties": properties or {},
        }
        if access_group is not None:
            body["access_group"] = access_group
        if expires_at is not None:
            body["expires_at"] = expires_at
        response = self.session.post(url, json=body)
        return handle_response(response)

    def validate_asset(
        self,
        name: str,
        uri: str,
        format: str,
        asset_type: str,
        is_managed: bool,
        mat_version: int | None = None,
        revision: int = 1,
        mutability: str = "static",
        maturity: str = "stable",
        properties: dict[str, Any] | None = None,
        access_group: str | None = None,
        expires_at: str | None = None,
    ) -> dict:
        """Dry-run validation of an asset without creating it.

        Accepts the same parameters as :meth:`register_asset`.

        Returns
        -------
        dict
            Validation report with pass/fail status for each check.
        """
        url = self._build_url("validate")
        body = {
            "datastack": self._datastack_name,
            "name": name,
            "mat_version": mat_version,
            "revision": revision,
            "uri": uri,
            "format": format,
            "asset_type": asset_type,
            "is_managed": is_managed,
            "mutability": mutability,
            "maturity": maturity,
            "properties": properties or {},
        }
        if access_group is not None:
            body["access_group"] = access_group
        if expires_at is not None:
            body["expires_at"] = expires_at
        response = self.session.post(url, json=body)
        return handle_response(response)

    def list_assets(
        self,
        name: str | None = None,
        mat_version: int | None = None,
        revision: int | None = None,
        format: str | None = None,
        asset_type: str | None = None,
        mutability: str | None = None,
        maturity: str | None = None,
    ) -> list[dict]:
        """List assets for the configured datastack.

        Parameters
        ----------
        name : str, optional
            Filter by asset name.
        mat_version : int, optional
            Filter by materialization version.
        revision : int, optional
            Filter by revision.
        format : str, optional
            Filter by format.
        asset_type : str, optional
            Filter by asset type.
        mutability : str, optional
            Filter by mutability.
        maturity : str, optional
            Filter by maturity.

        Returns
        -------
        list[dict]
            List of asset records.
        """
        url = self._build_url("list_assets")
        params = {"datastack": self._datastack_name}
        if name is not None:
            params["name"] = name
        if mat_version is not None:
            params["mat_version"] = mat_version
        if revision is not None:
            params["revision"] = revision
        if format is not None:
            params["format"] = format
        if asset_type is not None:
            params["asset_type"] = asset_type
        if mutability is not None:
            params["mutability"] = mutability
        if maturity is not None:
            params["maturity"] = maturity
        response = self.session.get(url, params=params)
        return handle_response(response)

    def get_asset(self, asset_id: str) -> dict:
        """Get a single asset by ID.

        Parameters
        ----------
        asset_id : str
            UUID of the asset.

        Returns
        -------
        dict
            The asset record.
        """
        url = self._build_url("get_asset", asset_id=asset_id)
        response = self.session.get(url)
        return handle_response(response)

    def delete_asset(self, asset_id: str) -> None:
        """Delete an asset from the catalog.

        Parameters
        ----------
        asset_id : str
            UUID of the asset to delete.
        """
        url = self._build_url("delete_asset", asset_id=asset_id)
        response = self.session.delete(url)
        handle_response(response, as_json=False)

    def get_access(self, asset_id: str) -> dict:
        """Get credentials for accessing an asset's data.

        Parameters
        ----------
        asset_id : str
            UUID of the asset.

        Returns
        -------
        dict
            Contains ``uri``, ``format``, ``token`` (or None), ``token_type``,
            ``expires_in``, and ``storage_provider``.
        """
        url = self._build_url("access", asset_id=asset_id)
        response = self.session.post(url)
        return handle_response(response)

    def resolve_view(self, asset_id: str) -> dict:
        """Resolve a view asset's SQL template.

        Parameters
        ----------
        asset_id : str
            UUID of the view asset.

        Returns
        -------
        dict
            Contains ``resolved_query``, ``credentials``, ``dialect``,
            and ``resolved_references``.
        """
        url = self._build_url("resolve", asset_id=asset_id)
        response = self.session.post(url)
        return handle_response(response)

    def to_duckdb_sql(self, asset_id: str) -> str:
        """Resolve a view and return ready-to-execute DuckDB SQL.

        Includes credential setup commands (e.g. ``SET`` statements for GCS tokens)
        prepended to the resolved query.

        Parameters
        ----------
        asset_id : str
            UUID of the view asset.

        Returns
        -------
        str
            SQL string ready for ``duckdb.sql()``.
        """
        resolved = self.resolve_view(asset_id)
        setup_lines = []
        for cred in resolved.get("credentials", []):
            provider = cred.get("storage_provider")
            if provider == "gcs" and cred.get("token"):
                setup_lines.append("INSTALL httpfs;")
                setup_lines.append("LOAD httpfs;")
                setup_lines.append("SET s3_access_key_id='';")
                setup_lines.append(
                    f"CREATE SECRET gcs_secret (TYPE GCS, KEY_ID '', SECRET '', TOKEN '{cred['token']}');"
                )
            elif provider == "s3" and cred.get("access_key_id"):
                setup_lines.append("INSTALL httpfs;")
                setup_lines.append("LOAD httpfs;")
                setup_lines.append(
                    f"CREATE SECRET s3_secret (TYPE S3, KEY_ID '{cred['access_key_id']}', SECRET '{cred['secret_access_key']}', SESSION_TOKEN '{cred['session_token']}');"
                )

        query = resolved.get("resolved_query", "")
        if setup_lines:
            return "\n".join(setup_lines) + "\n" + query
        return query
