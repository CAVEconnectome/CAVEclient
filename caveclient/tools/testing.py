from caveclient import endpoints, CAVEclient
import os
import numpy as np
import pandas as pd

try:
    import pytest
    import responses

    imports_worked = True
except ImportError:
    imports_worked = False

TEST_GLOBAL_SERVER = os.environ.get("TEST_SERVER", "https://test.cave.com")
TEST_LOCAL_SERVER = os.environ.get("TEST_LOCAL_SERVER", "https://local.cave.com")
TEST_DATASTACK = os.environ.get("TEST_DATASTACK", "test_stack")

default_info = {
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


def info_url(
    datastack_name,
    global_server,
):
    url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
    mapping = {"i_server_address": global_server, "datastack_name": datastack_name}
    return url_template.format_map(mapping)


def mocked_caveclient(
    datastack_name=None,
    global_server=None,
    local_server=None,
    info_file=None,
    chunkedgraph=False,
    chunkedgraph_server_version=None,
    materialization=False,
    materialization_server_version=None,
    annotation=False,
):
    if datastack_name is None:
        datastack_name = TEST_DATASTACK
    if global_server is None:
        global_server = TEST_GLOBAL_SERVER
    if local_server is None:
        local_server = TEST_LOCAL_SERVER
    if info_file is None:
        info_file = default_info

    @responses.activate()
    def test_client():
        responses.add(
            info_url(datastack_name, global_server), json=info_file, status=200
        )
        client = CAVEclient(
            datastack_name, server_address=global_server, write_server_cache=False
        )
        return client
