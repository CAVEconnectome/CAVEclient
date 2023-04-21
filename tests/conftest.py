import pytest
import responses
from caveclient import CAVEclient
import os
from caveclient import endpoints


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


@pytest.fixture()
@responses.activate
def myclient():
    url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
    mapping = {"i_server_address": TEST_GLOBAL_SERVER, "datastack_name": TEST_DATASTACK}
    url = url_template.format_map(mapping)

    responses.add(responses.GET, url, json=test_info, status=200)

    client = CAVEclient(TEST_DATASTACK, server_address=TEST_GLOBAL_SERVER, write_server_cache=False)
    return client
