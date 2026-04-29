"""Tests for CatalogClient Phase 0 methods (task 2.11).

Tests are written against a mocked HTTP layer using the `responses` library,
following the CAVEclient testing convention.
"""

from __future__ import annotations

import uuid

import pytest
import responses

from caveclient.catalogservice import CatalogClient
from caveclient.endpoints import catalogservice_endpoints_v1

TEST_LOCAL_SERVER = "https://local.cave.com"
TEST_DATASTACK = "test_stack"

ASSET_ID = str(uuid.uuid4())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CATALOG_V1 = catalogservice_endpoints_v1
MAPPING = {"catalog_server_address": TEST_LOCAL_SERVER}


def _url(key: str, **kwargs) -> str:
    m = {**MAPPING, **kwargs}
    return CATALOG_V1[key].format_map(m)


def _asset_record(**overrides) -> dict:
    base = {
        "id": ASSET_ID,
        "datastack": TEST_DATASTACK,
        "name": "synapses",
        "mat_version": 943,
        "revision": 1,
        "uri": "gs://bucket/minnie65/synapses/",
        "format": "delta",
        "asset_type": "table",
        "owner": "test@example.com",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
        "access_group": None,
        "created_at": "2026-04-01T00:00:00Z",
        "expires_at": None,
    }
    base.update(overrides)
    return base


@pytest.fixture()
def catalog_client():
    """CatalogClient that skips version detection (no get_version endpoint)."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        # The catalogservice_common has no get_api_versions key, so _api_endpoints
        # will fall back to version 1.  No mock needed for that.
        client = CatalogClient(
            server_address=TEST_LOCAL_SERVER,
            datastack_name=TEST_DATASTACK,
        )
        yield client, rsps


# ---------------------------------------------------------------------------
# list_assets
# ---------------------------------------------------------------------------


@responses.activate
def test_list_assets_returns_records():
    records = [_asset_record()]
    responses.add(
        responses.GET,
        url=_url("list_assets"),
        json=records,
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.list_assets()
    assert isinstance(result, list)
    assert result[0]["name"] == "synapses"
    assert result[0]["id"] == ASSET_ID


@responses.activate
def test_list_assets_passes_filters():
    responses.add(
        responses.GET,
        url=_url("list_assets"),
        json=[],
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.list_assets(name="synapses", mat_version=943, maturity="stable")
    # Verify the request included the filter params
    req = responses.calls[0].request
    assert "name=synapses" in req.url
    assert "mat_version=943" in req.url
    assert "maturity=stable" in req.url


# ---------------------------------------------------------------------------
# get_asset
# ---------------------------------------------------------------------------


@responses.activate
def test_get_asset_success():
    responses.add(
        responses.GET,
        url=_url("get_asset", asset_id=ASSET_ID),
        json=_asset_record(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.get_asset(ASSET_ID)
    assert result["id"] == ASSET_ID
    assert result["name"] == "synapses"


@responses.activate
def test_get_asset_404_raises():
    responses.add(
        responses.GET,
        url=_url("get_asset", asset_id=ASSET_ID),
        json={"detail": "Asset not found"},
        status=404,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    with pytest.raises(Exception):
        client.get_asset(ASSET_ID)


# ---------------------------------------------------------------------------
# register_asset
# ---------------------------------------------------------------------------


@responses.activate
def test_register_asset_returns_record():
    responses.add(
        responses.POST,
        url=_url("register"),
        json=_asset_record(),
        status=201,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.register_asset(
        name="synapses",
        uri="gs://bucket/minnie65/synapses/",
        format="delta",
        asset_type="table",
        is_managed=True,
        mat_version=943,
    )
    assert result["id"] == ASSET_ID
    assert result["mat_version"] == 943


@responses.activate
def test_register_asset_sends_correct_body():
    responses.add(
        responses.POST,
        url=_url("register"),
        json=_asset_record(),
        status=201,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.register_asset(
        name="synapses",
        uri="gs://bucket/path/",
        format="delta",
        asset_type="table",
        is_managed=True,
        mat_version=943,
        revision=2,
        properties={"source": "materialization"},
    )
    req = responses.calls[0].request
    import json

    body = json.loads(req.body)
    assert body["datastack"] == TEST_DATASTACK
    assert body["name"] == "synapses"
    assert body["mat_version"] == 943
    assert body["revision"] == 2
    assert body["properties"]["source"] == "materialization"


# ---------------------------------------------------------------------------
# validate_asset
# ---------------------------------------------------------------------------


@responses.activate
def test_validate_asset_returns_report():
    report = {
        "auth_check": {"passed": True},
        "duplicate_check": {"passed": True},
        "uri_reachable": {"passed": True},
        "format_sniff": {"passed": True},
    }
    responses.add(
        responses.POST,
        url=_url("validate"),
        json=report,
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.validate_asset(
        name="synapses",
        uri="gs://bucket/path/",
        format="delta",
        asset_type="table",
        is_managed=True,
    )
    assert result["uri_reachable"]["passed"] is True


# ---------------------------------------------------------------------------
# delete_asset
# ---------------------------------------------------------------------------


@responses.activate
def test_delete_asset_returns_none():
    responses.add(
        responses.DELETE,
        url=_url("delete_asset", asset_id=ASSET_ID),
        status=204,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.delete_asset(ASSET_ID)
    assert result is None


# ---------------------------------------------------------------------------
# get_access (task 5.1)
# ---------------------------------------------------------------------------


def _access_response(**overrides) -> dict:
    base = {
        "uri": "gs://bucket/minnie65/synapses/",
        "format": "delta",
        "token": "ya29.fake-downscoped-token",
        "token_type": "Bearer",
        "expires_in": 3600,
        "storage_provider": "gcs",
        "is_managed": True,
    }
    base.update(overrides)
    return base


@responses.activate
def test_get_access_managed_returns_token():
    responses.add(
        responses.POST,
        url=_url("access", asset_id=ASSET_ID),
        json=_access_response(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.get_access(ASSET_ID)

    assert result["token"] == "ya29.fake-downscoped-token"
    assert result["token_type"] == "Bearer"
    assert result["expires_in"] == 3600
    assert result["storage_provider"] == "gcs"
    assert result["uri"] == "gs://bucket/minnie65/synapses/"
    assert result["format"] == "delta"


@responses.activate
def test_get_access_unmanaged_returns_passthrough():
    responses.add(
        responses.POST,
        url=_url("access", asset_id=ASSET_ID),
        json=_access_response(
            uri="gs://publicbucket/path/",
            token=None,
            token_type=None,
            expires_in=None,
            storage_provider=None,
            is_managed=False,
        ),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.get_access(ASSET_ID)

    assert result["token"] is None
    assert result["is_managed"] is False
    assert result["uri"] == "gs://publicbucket/path/"


@responses.activate
def test_get_access_missing_asset_raises():
    responses.add(
        responses.POST,
        url=_url("access", asset_id=ASSET_ID),
        json={"detail": "Asset not found"},
        status=404,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    with pytest.raises(Exception):
        client.get_access(ASSET_ID)


@responses.activate
def test_get_access_unauthorized_raises():
    responses.add(
        responses.POST,
        url=_url("access", asset_id=ASSET_ID),
        json={"detail": "Access denied"},
        status=403,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    with pytest.raises(Exception):
        client.get_access(ASSET_ID)


@responses.activate
def test_get_access_posts_to_correct_url():
    responses.add(
        responses.POST,
        url=_url("access", asset_id=ASSET_ID),
        json=_access_response(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.get_access(ASSET_ID)

    assert len(responses.calls) == 1
    req = responses.calls[0].request
    assert f"/assets/{ASSET_ID}/access" in req.url


# ---------------------------------------------------------------------------
# Phase 6: Table-specific methods
# ---------------------------------------------------------------------------

TABLE_ID = str(uuid.uuid4())


def _table_record(**overrides) -> dict:
    base = {
        "id": TABLE_ID,
        "datastack": TEST_DATASTACK,
        "name": "my_table",
        "mat_version": 943,
        "revision": 0,
        "uri": "gs://bucket/tables/my_table/",
        "format": "delta",
        "asset_type": "table",
        "owner": 1,
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
        "access_group": None,
        "created_at": "2026-04-01T00:00:00Z",
        "expires_at": None,
        "source": "user",
        "cached_metadata": {
            "n_rows": 100,
            "n_columns": 2,
            "n_bytes": 5000,
            "columns": [
                {"name": "a", "dtype": "int64"},
                {"name": "b", "dtype": "string"},
            ],
            "partition_columns": [],
        },
        "metadata_cached_at": "2026-04-01T00:00:00Z",
        "column_annotations": [],
        "columns": [
            {"name": "a", "dtype": "int64", "description": None, "links": []},
            {"name": "b", "dtype": "string", "description": None, "links": []},
        ],
    }
    base.update(overrides)
    return base


# --- preview_table ---


@responses.activate
def test_preview_table_returns_metadata():
    preview = {
        "metadata": {
            "n_rows": 100,
            "n_columns": 2,
            "n_bytes": 5000,
            "columns": [
                {"name": "a", "dtype": "int64"},
                {"name": "b", "dtype": "string"},
            ],
            "partition_columns": [],
        }
    }
    responses.add(
        responses.POST,
        url=_url("preview_table"),
        json=preview,
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.preview_table(uri="gs://bucket/tables/my_table/", format="delta")
    assert result["metadata"]["n_rows"] == 100
    assert len(result["metadata"]["columns"]) == 2


@responses.activate
def test_preview_table_sends_correct_body():
    responses.add(
        responses.POST,
        url=_url("preview_table"),
        json={"metadata": {}},
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.preview_table(uri="gs://bucket/path/", format="parquet")

    import json

    req = responses.calls[0].request
    body = json.loads(req.body)
    assert body["uri"] == "gs://bucket/path/"
    assert body["format"] == "parquet"
    assert body["datastack"] == TEST_DATASTACK


# --- register_table ---


@responses.activate
def test_register_table_returns_record():
    responses.add(
        responses.POST,
        url=_url("register_table"),
        json=_table_record(),
        status=201,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.register_table(
        name="my_table",
        uri="gs://bucket/tables/my_table/",
        format="delta",
        is_managed=True,
        mat_version=943,
    )
    assert result["id"] == TABLE_ID
    assert result["asset_type"] == "table"
    assert result["cached_metadata"]["n_rows"] == 100


@responses.activate
def test_register_table_sends_correct_body():
    responses.add(
        responses.POST,
        url=_url("register_table"),
        json=_table_record(),
        status=201,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    annotations = [{"column_name": "a", "description": "ID column", "links": []}]
    client.register_table(
        name="my_table",
        uri="gs://bucket/tables/my_table/",
        format="delta",
        is_managed=True,
        mat_version=943,
        source="materialization",
        column_annotations=annotations,
    )

    import json

    req = responses.calls[0].request
    body = json.loads(req.body)
    assert body["datastack"] == TEST_DATASTACK
    assert body["name"] == "my_table"
    assert body["asset_type"] == "table"
    assert body["format"] == "delta"
    assert body["source"] == "materialization"
    assert len(body["column_annotations"]) == 1
    assert body["column_annotations"][0]["column_name"] == "a"


# --- list_tables ---


@responses.activate
def test_list_tables_returns_records():
    records = [_table_record()]
    responses.add(
        responses.GET,
        url=_url("list_tables"),
        json=records,
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.list_tables()
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["asset_type"] == "table"
    assert result[0]["cached_metadata"]["n_rows"] == 100


@responses.activate
def test_list_tables_passes_filters():
    responses.add(
        responses.GET,
        url=_url("list_tables"),
        json=[],
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.list_tables(name="my_table", format="delta", source="user")
    req = responses.calls[0].request
    assert "name=my_table" in req.url
    assert "format=delta" in req.url
    assert "source=user" in req.url


# --- update_annotations ---


@responses.activate
def test_update_annotations_returns_record():
    responses.add(
        responses.PATCH,
        url=_url("update_annotations", table_id=TABLE_ID),
        json=_table_record(
            column_annotations=[
                {"column_name": "a", "description": "ID column", "links": []}
            ]
        ),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.update_annotations(
        TABLE_ID,
        column_annotations=[
            {"column_name": "a", "description": "ID column", "links": []}
        ],
    )
    assert result["column_annotations"][0]["column_name"] == "a"


@responses.activate
def test_update_annotations_sends_patch():
    responses.add(
        responses.PATCH,
        url=_url("update_annotations", table_id=TABLE_ID),
        json=_table_record(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.update_annotations(TABLE_ID, column_annotations=[])

    import json

    req = responses.calls[0].request
    assert req.method == "PATCH"
    body = json.loads(req.body)
    assert body["column_annotations"] == []


# --- refresh_metadata ---


@responses.activate
def test_refresh_metadata_returns_record():
    responses.add(
        responses.POST,
        url=_url("refresh_metadata", table_id=TABLE_ID),
        json=_table_record(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    result = client.refresh_metadata(TABLE_ID)
    assert result["id"] == TABLE_ID
    assert result["cached_metadata"] is not None


@responses.activate
def test_refresh_metadata_posts_to_correct_url():
    responses.add(
        responses.POST,
        url=_url("refresh_metadata", table_id=TABLE_ID),
        json=_table_record(),
        status=200,
    )
    client = CatalogClient(
        server_address=TEST_LOCAL_SERVER, datastack_name=TEST_DATASTACK
    )
    client.refresh_metadata(TABLE_ID)

    req = responses.calls[0].request
    assert f"/tables/{TABLE_ID}/refresh" in req.url
