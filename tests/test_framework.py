import datetime

import responses
from pytest import raises as assert_raises
from responses.matchers import query_param_matcher

from caveclient import CAVEclient, endpoints, set_session_defaults

from .conftest import (
    datastack_dict,
    global_client,  # noqa: F401
    mat_apiv2_specified_client,  # noqa: F401
    server_versions,
    test_info,
    version_specified_client,  # noqa: F401
)

default_mapping = {
    "me_server_address": datastack_dict["local_server"],
    "cg_server_address": datastack_dict["local_server"],
    "table_id": test_info["segmentation_source"].split("/")[-1],
    "datastack_name": datastack_dict["datastack_name"],
    "table_name": test_info["synapse_table"],
    "version": 1,
}


url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
mapping = {
    "i_server_address": datastack_dict["global_server"],
    "datastack_name": datastack_dict["datastack_name"],
}
info_url = url_template.format_map(mapping)


def test_global_client(global_client):  # noqa: F811
    assert global_client.info.datastack_name is None
    assert global_client.info.server_address == datastack_dict["global_server"]
    assert "Authorization" in global_client.auth.request_header
    assert (
        global_client.state.server_version
        == server_versions["json_service_server_version"]
    )


def test_versioned_client(version_specified_client):  # noqa: F811
    correct_date = datetime.datetime.strptime(
        "2024-06-05T10:10:01.203215", "%Y-%m-%dT%H:%M:%S.%f"
    ).replace(tzinfo=datetime.timezone.utc)

    assert version_specified_client.timestamp == correct_date
    assert version_specified_client.materialize.version == 3
    assert version_specified_client.chunkedgraph.timestamp == correct_date


def test_api_version(mat_apiv2_specified_client):  # noqa: F811
    assert "api/v2" in mat_apiv2_specified_client.materialize._endpoints["simple_query"]


class TestFrameworkClient:
    default_mapping = {
        "me_server_address": datastack_dict["local_server"],
        "cg_server_address": datastack_dict["local_server"],
        "table_id": test_info["segmentation_source"].split("/")[-1],
        "datastack_name": datastack_dict["datastack_name"],
        "table_name": test_info["synapse_table"],
        "version": 1,
    }

    @responses.activate
    def test_create_client(self):
        responses.add(responses.GET, info_url, json=test_info, status=200)
        _ = CAVEclient(
            datastack_dict["datastack_name"],
            server_address=datastack_dict["global_server"],
            write_server_cache=False,
        )

    @responses.activate
    def test_create_versioned_client(self):
        responses.add(responses.GET, info_url, json=test_info, status=200)

        endpoint_mapping = self.default_mapping
        endpoint_mapping["emas_server_address"] = datastack_dict["global_server"]

        api_versions_url = endpoints.chunkedgraph_endpoints_common[
            "get_api_versions"
        ].format_map(endpoint_mapping)
        responses.add(responses.GET, url=api_versions_url, json=[0, 1], status=200)

        cg_version_url = endpoints.chunkedgraph_endpoints_common[
            "get_version"
        ].format_map(endpoint_mapping)
        responses.add(responses.GET, cg_version_url, json="2.15.0", status=200)

        mat_version_url = endpoints.materialization_common["get_version"].format_map(
            endpoint_mapping
        )
        responses.add(responses.GET, mat_version_url, json="4.30.1", status=200)

        responses.add(
            responses.GET,
            url=endpoints.materialization_common["get_api_versions"].format_map(
                endpoint_mapping
            ),
            json=[3],
            status=200,
        )

        versionurl = endpoints.materialization_endpoints_v3["versions"].format_map(
            endpoint_mapping
        )
        responses.add(
            responses.GET,
            url=versionurl,
            json=[1, 2],
            status=200,
            match=[query_param_matcher({"expired": True})],
        )

        version_metadata_url = endpoints.materialization_endpoints_v3[
            "version_metadata"
        ].format_map(endpoint_mapping)

        responses.add(
            responses.GET,
            url=version_metadata_url,
            json={
                "time_stamp": "2024-06-05T10:10:01.203215",
                "expires_on": "2080-06-05T10:10:01.203215",
            },
            status=200,
        )

        with assert_raises(ValueError):
            _ = CAVEclient(
                datastack_dict["datastack_name"],
                server_address=datastack_dict["global_server"],
                write_server_cache=False,
                version=10,
            )

        versioned_client = CAVEclient(
            datastack_dict["datastack_name"],
            server_address=datastack_dict["global_server"],
            write_server_cache=False,
            version=1,
        )

        correct_date = datetime.datetime.strptime(
            "2024-06-05T10:10:01.203215", "%Y-%m-%dT%H:%M:%S.%f"
        ).replace(tzinfo=datetime.timezone.utc)

        assert versioned_client.timestamp == correct_date

        assert versioned_client.materialize.version == 1

        assert versioned_client.chunkedgraph.timestamp == correct_date

    @responses.activate
    def test_set_session_defaults(self):
        responses.add(responses.GET, info_url, json=test_info, status=200)

        pool_maxsize = 21
        pool_block = True
        max_retries = 5
        backoff_factor = 0.5
        status_forcelist = (502, 503, 504, 505)

        set_session_defaults(
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        client = CAVEclient(
            datastack_dict["datastack_name"],
            server_address=datastack_dict["global_server"],
            write_server_cache=False,
        )
        assert client.l2cache.session.adapters["https://"]._pool_maxsize == pool_maxsize
        assert client.l2cache.session.adapters["https://"]._pool_block
        assert (
            client.l2cache.session.adapters["https://"].max_retries.total == max_retries
        )
        assert (
            client.l2cache.session.adapters["https://"].max_retries.backoff_factor
            == 0.5
        )
        assert client.l2cache.session.adapters[
            "https://"
        ].max_retries.status_forcelist == (
            502,
            503,
            504,
            505,
        )
