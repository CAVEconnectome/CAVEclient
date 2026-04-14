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
