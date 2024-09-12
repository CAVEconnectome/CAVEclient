import datetime

import responses
from pytest import raises as assert_raises
from responses.matchers import query_param_matcher

from caveclient import CAVEclient, endpoints, set_session_defaults

from .conftest import TEST_DATASTACK, TEST_GLOBAL_SERVER, TEST_LOCAL_SERVER, test_info

default_mapping = {
    "me_server_address": TEST_LOCAL_SERVER,
    "cg_server_address": TEST_LOCAL_SERVER,
    "table_id": test_info["segmentation_source"].split("/")[-1],
    "datastack_name": TEST_DATASTACK,
    "table_name": test_info["synapse_table"],
    "version": 1,
}


url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
mapping = {"i_server_address": TEST_GLOBAL_SERVER, "datastack_name": TEST_DATASTACK}
info_url = url_template.format_map(mapping)


class TestFrameworkClient:
    default_mapping = {
        "me_server_address": TEST_LOCAL_SERVER,
        "cg_server_address": TEST_LOCAL_SERVER,
        "table_id": test_info["segmentation_source"].split("/")[-1],
        "datastack_name": TEST_DATASTACK,
        "table_name": test_info["synapse_table"],
        "version": 1,
    }

    @responses.activate
    def test_create_client(self):
        responses.add(responses.GET, info_url, json=test_info, status=200)
        _ = CAVEclient(
            TEST_DATASTACK, server_address=TEST_GLOBAL_SERVER, write_server_cache=False
        )

    @responses.activate
    def test_create_versioned_client(self):
        responses.add(responses.GET, info_url, json=test_info, status=200)

        endpoint_mapping = self.default_mapping
        endpoint_mapping["emas_server_address"] = TEST_GLOBAL_SERVER

        print(endpoints.chunkedgraph_endpoints_common.keys())
        api_versions_url = endpoints.chunkedgraph_endpoints_common[
            "get_api_versions"
        ].format_map(endpoint_mapping)
        responses.add(responses.GET, url=api_versions_url, json=[0, 1], status=200)

        version_url = endpoints.chunkedgraph_endpoints_common["get_version"].format_map(
            endpoint_mapping
        )
        responses.add(responses.GET, version_url, json="2.15.0", status=200)

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
                TEST_DATASTACK,
                server_address=TEST_GLOBAL_SERVER,
                write_server_cache=False,
                version=10,
            )

        versioned_client = CAVEclient(
            TEST_DATASTACK,
            server_address=TEST_GLOBAL_SERVER,
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
        backoff_max = 240
        status_forcelist = (502, 503, 504, 505)

        set_session_defaults(
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            backoff_max=backoff_max,
            status_forcelist=status_forcelist,
        )
        client = CAVEclient(
            TEST_DATASTACK, server_address=TEST_GLOBAL_SERVER, write_server_cache=False
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
        assert (
            client.l2cache.session.adapters["https://"].max_retries.backoff_max == 240
        )
        assert client.l2cache.session.adapters[
            "https://"
        ].max_retries.status_forcelist == (
            502,
            503,
            504,
            505,
        )
