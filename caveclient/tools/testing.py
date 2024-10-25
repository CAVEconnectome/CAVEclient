import os
import warnings
from typing import Optional
from ..frameworkclient import CAVEclient
from .. import endpoints


try:
    import responses

    imports_worked = True
except ImportError:
    warnings.warn("Must install responses to use CAVEclientMock for testing")
    imports_worked = False

DEFAULT_CHUNKEDGRAPH_SERVER_VERSION = "2.15.0"
DEFAULT_MATERIALIZATION_SERVER_VERSON = "4.30.1"
DEFAULT_SKELETON_SERVICE_SERVER_VERSION = "0.3.8"
DEFAULT_JSON_SERVICE_SERVER_VERSION = "0.7.0"

TEST_GLOBAL_SERVER = os.environ.get("TEST_SERVER", "https://test.cave.com")
TEST_LOCAL_SERVER = os.environ.get("TEST_LOCAL_SERVER", "https://local.cave.com")
TEST_DATASTACK = os.environ.get("TEST_DATASTACK", "test_stack")
DEFAULT_MATERIALIZATION_VERSONS = [1, 2]


def get_server_information(
    datastack_name: str = TEST_DATASTACK,
    global_server: str = TEST_GLOBAL_SERVER,
    local_server: str = TEST_LOCAL_SERVER,
) -> dict:
    """Generate the datastack name and server locations used in testing.

    Parameters
    ----------
    datastack_name : str, optional
        Datastack value, by default the value in TEST_DATASTACK.
    global_server : str, optional
        Server for global services, by default TEST_GLOBAL_SERVER.
    local_server : str, optional
        Server for local services, by default TEST_LOCAL_SERVER.

    Returns
    -------
    dict
        Dictionary with keys: "datastack_name", "local_server", "global_server".
    """
    return {
        "datastack_name": datastack_name,
        "local_server": local_server,
        "global_server": global_server,
    }


def default_info(
    local_server: str = TEST_LOCAL_SERVER,
) -> dict:
    """Generate a info service info file for testing

    Parameters
    ----------
    local_server : str, optional
        Name of the local service, by default the value in TEST_LOCAL_SERVER.

    Returns
    -------
    dict
        Info file for the datastack.
    """
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
        "skeleton_source": f"precomputed://https://{local_server}/skeletoncache/api/v1/minnie65_phase3_v1/precomputed/skeleton/",
        "soma_table": "test_soma",
        "analysis_database": None,
        "viewer_resolution_x": 4.0,
        "viewer_resolution_y": 4.0,
        "viewer_resolution_z": 40,
    }


def info_url(
    datastack_name: str,
    global_server: str,
) -> str:
    """Gets the info service URL for getting the info dictionary

    Parameters
    ----------
    datastack_name : str
        datastack_name
    global_server : str
        Global server address

    Returns
    -------
    str
        URL for the info service
    """

    url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
    mapping = {"i_server_address": global_server, "datastack_name": datastack_name}
    return url_template.format_map(mapping)


def version_url(
    server_address: str,
    endpoint_dictionary: dict,
    service_key: str,
) -> str:
    version_endpoint = endpoint_dictionary["get_version"]
    mapping = {service_key: server_address}
    return version_endpoint.format_map(mapping)


def get_table_name(info_file):
    """Get the table name from the info file dictionary"""
    seg_source = info_file.get("segmentation_source")
    if seg_source:
        return seg_source.split("/")[-1]
    else:
        return "test_table"


def CAVEclientMock(
    datastack_name: Optional[str] = None,
    global_server: Optional[str] = None,
    local_server: Optional[str] = None,
    info_file: Optional[dict] = None,
    chunkedgraph: bool = False,
    chunkedgraph_server_version: str = DEFAULT_CHUNKEDGRAPH_SERVER_VERSION,
    materialization: bool = False,
    materialization_server_version: str = DEFAULT_MATERIALIZATION_SERVER_VERSON,
    available_materialization_versions: Optional[list] = None,
    json_service: bool = False,
    json_service_server_version: str = DEFAULT_JSON_SERVICE_SERVER_VERSION,
    skeleton_service: bool = False,
    skeleton_service_server_version: str = DEFAULT_SKELETON_SERVICE_SERVER_VERSION,
    l2cache: bool = False,
    l2cache_disabled: bool = False,
):
    """Created a mocked CAVEclient function for testing using the responses library to mock
    the server responses. This function returns a drop-in replacement for the `CAVEclient` function
    that will be able to initialize itself and selected individual service clients with the selected options.

    Note that the test configuration is intended to be purely for pytest purposes and should not
    actually result in calls to active endpoints.

    Parameters
    ----------
    datastack_name : str, optional
        Name of the test datastack, by default None
    global_server : str, optional
        Test global server address, by default None
    local_server : str, optional
        Test local server address, by default None
    info_file : dictionary, optional
        Info service dictionary for the datastack, by default None
    chunkedgraph : bool, optional
        If True, configures the client to initialize a chunkedgraph subclient, by default False
    chunkedgraph_server_version : str, optional
        Sets the value of the chunkedgraph server version as a three-element semenatic version (e.g "2.3.4"),
        by default the value in DEFAULT_CHUNKEDGRAPH_SERVER_VERSION.
    materialization : bool, optional
        If True, configures the client to initalize a materialization subclient, by default False
        Note that materialization being set to True will also configure the chunkedgraph client.
    materialization_server_version : str, optional
        Sets the value of the materialization server version as a three-element semenatic version (e.g "2.3.4"),
        by default the value in DEFAULT_MATERIALIZATION_SERVER_VERSON.
    available_materialization_versions : list, optional
        List of materialization database versions that the materialization client thinks exists, by default None.
        If None, returns the value in DEFAULT_MATERIALIZATION_VERSONS.
    json_service : bool, optional
        If True, configures the client to initalize a materialization subclient, by default False
    json_service_server_version : _type_, optional
        Sets the value of the json state server version as a three-element semenatic version (e.g "2.3.4"),
        by default the value in DEFAULT_JSON_SERVICE_SERVER_VERSION.
    skeleton_service : bool, optional
        If True, configures the client to initalize a skeleton service subclient, by default False
    skeleton_service_server_version : _type_, optional
        Sets the value of the skeleton service version as a three-element semenatic version (e.g "2.3.4"),
        by default the value in DEFAULT_SKELETON_SERVICE_SERVER_VERSION.
    l2cache : bool, optional
        If True, configures the client to initialize an l2cache subclient, by default False
        Note that l2cache being set to True will also configure the chunkedgraph client.
    l2cache_disabled : bool, optional
        If True, allows a subclient to be initialized, but emulates a situation without an L2 cache, by default False
        Only used if l2cache is True.

    Returns
    -------
    CAVEclient
        A mocked and initialized CAVEclient object for testing

    Examples
    --------
    To make a basic pytest fixture to test chunkedgraph features with an initialized CAVEclient object in your pytest conftest.py file:

    ```python
    import pytest
    from caveclient.tools.testing import CAVEclientMock

    test_datastack = "test_stack"
    test_global_server = "https://test.cave.com"
    test_local_server = "https://local.cave.com"

    @pytest.fixture()
    def test_client():
        return CAVEclientMock(
            datastack_name=test_datastack,
            global_server=test_global_server,
            local_server=test_local_server,
            chunkedgraph=True,
        )

    You can also create more complex fixtures with multiple services initialized and specific server versions:

    ```python
    @pytest.fixture()
    def fancier_test_client():
        return CAVEclientMock(
            datastack_name=test_datastack,
            global_server=test_global_server,
            local_server=test_local_server,
            chunkedgraph=True,
            chunkedgraph_server_version="3.0.2",
            materialization=True,
            materialization_server_version="4.21.4",
            l2cache=True,
        )
    ```
    """
    if not imports_worked:
        raise ImportError(
            "Please install responses to use CAVEclientMock: e.g. 'pip install responses'}"
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
    def mockedCAVEclient():
        url = info_url(datastack_name, global_server)
        responses.add(responses.GET, url=url, json=info_file, status=200)
        client = CAVEclient(
            datastack_name,
            server_address=global_server,
            write_server_cache=False,
            auth_token="just_a_test",
            write_local_auth=False,
        )
        if chunkedgraph or l2cache or materialization:
            pcg_version_url = version_url(
                local_server,
                endpoints.chunkedgraph_endpoints_common,
                "cg_server_address",
            )
            responses.add(
                responses.GET,
                pcg_version_url,
                json=chunkedgraph_server_version,
                status=200,
            )
            client.chunkedgraph
        if materialization:
            mat_version_url = version_url(
                local_server,
                endpoints.materialization_common,
                "me_server_address",
            )
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
            js_version_url = version_url(
                global_server,
                endpoints.jsonservice_endpoints_v1,
                "json_server_address",
            )
            responses.add(
                responses.GET,
                js_version_url,
                json=json_service_server_version,
                status=200,
            )
            client.state
        if skeleton_service:
            ss_version_url = version_url(
                local_server,
                endpoints.skeletonservice_endpoints_v1,
                "skeleton_server_address",
            )
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
            if l2cache_disabled:
                responses.add(
                    responses.GET,
                    url=table_mapping_url,
                    json={},
                    status=200,
                )
            else:
                responses.add(
                    responses.GET,
                    url=table_mapping_url,
                    json={client.chunkedgraph.table_name: "test_table"},
                    status=200,
                )

        return client

    return mockedCAVEclient()
