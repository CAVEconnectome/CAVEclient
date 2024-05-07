import os

import pytest
import responses

from caveclient import CAVEclient, endpoints

TEST_GLOBAL_SERVER = os.environ.get("TEST_SERVER", "https://test.cave.com")
TEST_LOCAL_SERVER = os.environ.get("TEST_LOCAL_SERVER", "https://local.cave.com")
TEST_DATASTACK = os.environ.get("TEST_DATASTACK", "test_stack")

test_info = {
    "viewer_site": "http://neuromancer-seung-import.appspot.com/",
    "aligned_volume": {
        "name": "test_volume",
        "image_source": f"precomputed://https://{TEST_LOCAL_SERVER}/test-em/v1",
        "id": 1,
        "description": "This is a test only dataset.",
    },
    "synapse_table": "test_synapse_table",
    "description": "This is the first test datastack. ",
    "local_server": TEST_LOCAL_SERVER,
    "segmentation_source": f"graphene://https://{TEST_LOCAL_SERVER}/segmentation/table/test_v1",
    "soma_table": "test_soma",
    "analysis_database": None,
    "viewer_resolution_x": 4.0,
    "viewer_resolution_y": 4.0,
    "viewer_resolution_z": 40,
}
url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
mapping = {"i_server_address": TEST_GLOBAL_SERVER, "datastack_name": TEST_DATASTACK}
url = url_template.format_map(mapping)


@pytest.fixture()
@responses.activate
def myclient():
    responses.add(responses.GET, url, json=test_info, status=200)

    client = CAVEclient(
        TEST_DATASTACK, server_address=TEST_GLOBAL_SERVER, write_server_cache=False
    )

    # need to mock the response of the version checking code for each sub-client which
    # wants that information, and then create the sub-client here since the mock is
    # narrowly scoped to this function
    version_url = f"{TEST_LOCAL_SERVER}/segmentation/api/version"
    responses.add(responses.GET, version_url, json="2.15.0", status=200)

    client.chunkedgraph  # this will trigger the version check

    return client


@pytest.fixture()
@responses.activate
def old_chunkedgraph_client():
    responses.add(responses.GET, url, json=test_info, status=200)

    client = CAVEclient(
        TEST_DATASTACK, server_address=TEST_GLOBAL_SERVER, write_server_cache=False
    )

    # need to mock the response of the version checking code for each sub-client which
    # wants that information, and then create the sub-client here since the mock is
    # narrowly scoped to this function
    version_url = f"{TEST_LOCAL_SERVER}/segmentation/api/version"
    responses.add(responses.GET, version_url, json="1.0.0", status=200)

    client.chunkedgraph  # this will trigger the version check

    return client
