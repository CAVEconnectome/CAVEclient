import os
from ..frameworkclient import CAVEclient
from .. import endpoints

try:
    import responses

    imports_worked = True
except ImportError:
    imports_worked = False

TEST_GLOBAL_SERVER = os.environ.get("TEST_SERVER", "https://test.cave.com")
TEST_LOCAL_SERVER = os.environ.get("TEST_LOCAL_SERVER", "https://local.cave.com")
TEST_DATASTACK = os.environ.get("TEST_DATASTACK", "test_stack")
DEFAULT_MATERIALIZATION_VERSONS = [1, 2, 4, 8]


def default_info(local_server):
    return {
        "viewer_site": "http://neuromancer-seung-import.appspot.com/",
        "aligned_volume": {
            "name": "test_volume",
            "image_source": f"precomputed://https://{local_server}/test-em/v1",
            "id": 1,
            "description": "This is a test only dataset.",
        },
        "synapse_table": "test_synapse_table",
        "description": "This is the first test datastack.",
        "local_server": local_server,
        "segmentation_source": f"graphene://https://{local_server}/segmentation/table/test_v1",
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


def get_table_name(info_file):
    seg_source = info_file.get("segmentation_source")
    if seg_source:
        return seg_source.split("/")[-1]
    else:
        return "test_table"


def CAVEclientMock(
    datastack_name=None,
    global_server=None,
    local_server=None,
    info_file=None,
    chunkedgraph=False,
    chunkedgraph_server_version="2.15.0",
    materialization=False,
    materialization_server_version="4.30.1",
    available_materialization_versions=None,
    json_service=False,
    json_service_server_version="0.7.0",
    skeleton_service=False,
    skeleton_service_server_version="0.3.8",
    l2cache=False,
):
    if not imports_worked:
        raise ImportError(
            "Please install responses to use CAVEclientMock: 'pip install responses'}"
        )

    if datastack_name is None:
        datastack_name = TEST_DATASTACK
    if global_server is None:
        global_server = TEST_GLOBAL_SERVER
    if local_server is None:
        local_server = TEST_LOCAL_SERVER
    if info_file is None:
        info_file = default_info(local_server)
    if available_materialization_versions is None:
        available_materialization_versions = DEFAULT_MATERIALIZATION_VERSONS

    @responses.activate()
    def test_client():
        url = info_url(datastack_name, global_server)
        responses.add(responses.GET, url=url, json=info_file, status=200)
        client = CAVEclient(
            datastack_name,
            server_address=global_server,
            write_server_cache=False,
            auth_token="just_a_test",
            max_retries=0,
        )
        if chunkedgraph or l2cache:
            pcg_version_endpoint = endpoints.chunkedgraph_endpoints_common[
                "get_version"
            ]
            pcg_mapping = {"cg_server_address": local_server}
            version_url = pcg_version_endpoint.format_map(pcg_mapping)
            responses.add(
                responses.GET, version_url, json=chunkedgraph_server_version, status=200
            )
            client.chunkedgraph
        if materialization:
            mat_version_endpoint = endpoints.materialization_common["get_version"]
            mat_mapping = {"me_server_address": local_server}
            mat_version_url = mat_version_endpoint.format_map(mat_mapping)
            responses.add(
                responses.GET,
                mat_version_url,
                json=materialization_server_version,
                status=200,
            )

            mat_available_endpoint = endpoints.materialization_endpoints_v3["versions"]
            mat_mapping = {
                "me_server_address": local_server,
                "datastack_name": datastack_name,
            }
            mat_version_list_url = mat_available_endpoint.format_map(mat_mapping)
            responses.add(
                responses.GET,
                mat_version_list_url,
                json=available_materialization_versions,
                status=200,
            )
            client.materialize
        if json_service:
            js_version_endpoint = endpoints.mat_version_endpoint["get_version"]
            js_mapping = {"json_server_address": global_server}
            js_version_url = js_version_endpoint.format_map(js_mapping)
            responses.add(
                responses.GET,
                js_version_url,
                json=json_service_server_version,
                status=200,
            )
            client.state
        if skeleton_service:
            ss_version_endpoint = endpoints.skeletonservice_endpoints_v1["get_version"]
            ss_mapping = {"skeleton_server_address": local_server}
            ss_version_url = ss_version_endpoint.format_map(ss_mapping)
            responses.add(
                responses.GET,
                ss_version_url,
                json=skeleton_service_server_version,
                status=200,
            )
            client.skeleton
        if l2cache:
            table_mapping_endpoint = endpoints.l2cache_endpoints_v1[
                "l2cache_table_mapping"
            ]
            l2_mapping = {
                "l2cache_server_address": local_server,
                "table_id": client.chunkedgraph.table_name,
            }
            table_mapping_url = table_mapping_endpoint.format_map(l2_mapping)
            print("table_mapping_url:", table_mapping_url)
            responses.add(
                responses.GET,
                url=table_mapping_url,
                json={client.chunkedgraph.table_name: "test_table"},
                status=200,
            )
            print("has cache:", client.l2cache.has_cache())

        return client

    return test_client
