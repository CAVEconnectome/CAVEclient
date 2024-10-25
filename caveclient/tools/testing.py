import os
import warnings
from typing import Optional

from packaging.version import Version

from .. import endpoints
from ..frameworkclient import CAVEclient

try:
    import responses
    from responses.matchers import query_param_matcher

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

DEFAULT_MATERIALIZATION_VERSION_METADATA = {
    "time_stamp": "2024-06-05T10:10:01.203215",
    "expires_on": "2080-06-05T10:10:01.203215",
}

MATERIALIZATION_API_VERSIONS = [2, 3]
CHUNKEDGRAPH_API_VERSIONS = [0, 1]
SCHEMA_API_VERSIONS = [1, 2]


def get_materialiation_info(
    materialization_versions: list = DEFAULT_MATERIALIZATION_VERSONS,
    version_metadata: dict = DEFAULT_MATERIALIZATION_VERSION_METADATA,
):
    """Get the materialization versions and version metadata for the materialization service.

    Parameters
    ----------
    materialization_versions : list, optional
        List of materialization database versions that the materialization client thinks exists, by default DEFAULT_MATERIALIZATION_VERSONS.
    version_metadata : dict, optional
        Version metadata for the materialization service, by default DEFAULT_MATERIALIZATION_VERSION_METADATA.

    Returns
    -------
    dict
        Dictionary with keys: "materialization_versions", "version_metadata".
    """
    return {
        "materialization_versions": materialization_versions,
        "version_metadata": version_metadata,
    }


def get_server_versions(
    chunkedgraph_version: str = DEFAULT_CHUNKEDGRAPH_SERVER_VERSION,
    materialization_version: str = DEFAULT_MATERIALIZATION_SERVER_VERSON,
    skeleton_service_version: str = DEFAULT_SKELETON_SERVICE_SERVER_VERSION,
    json_service_version: str = DEFAULT_JSON_SERVICE_SERVER_VERSION,
) -> dict:
    """Get the server versions for the services used in testing.

    Parameters
    ----------
    chunkedgraph_version : str, optional
        Version of the chunkedgraph server, by default DEFAULT_CHUNKEDGRAPH_SERVER_VERSION.
    materialization_version : str, optional
        Version of the materialization server, by default DEFAULT_MATERIALIZATION_SERVER_VERSON.
    skeleton_service_version : str, optional
        Version of the skeleton service server, by default DEFAULT_SKELETON_SERVICE_SERVER_VERSION.
    json_service_version : str, optional
        Version of the json service server, by default DEFAULT_JSON_SERVICE_SERVER_VERSION.

    Returns
    -------
    dict
        Dictionary with keys: "chunkedgraph_version", "materialization_version", "skeleton_service_version", "json_service_version".
        Values are Version objects from packaging.versions.
    """
    return {
        "chunkedgraph_server_version": Version(chunkedgraph_version),
        "materialization_server_version": Version(materialization_version),
        "skeleton_service_server_version": Version(skeleton_service_version),
        "json_service_server_version": Version(json_service_version),
    }


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


def get_api_versions(
    chunkedgraph_api_versions: list = CHUNKEDGRAPH_API_VERSIONS,
    materialization_api_versions: list = MATERIALIZATION_API_VERSIONS,
    schema_api_versions: list = SCHEMA_API_VERSIONS,
):
    """Get the API versions for the services used in testing.

    Parameters
    ----------
    chunkedgraph_api_versions : list, optional
        List of chunkedgraph API versions that the chunkedgraph client thinks exists, by default CHUNKEDGRAPH_API_VERSIONS.
    materialization_api_versions : list, optional
        List of materialization API versions that the materialization client thinks exists, by default MATERIALIZATION_API_VERSIONS.
    schema_api_versions : list, optional
        List of schema API versions that the schema client thinks exists, by default SCHEMA_API_VERSIONS.

    Returns
    -------
    dict
        Dictionary with keys: "chunkedgraph_api_versions", "materialization_api_versions", "schema_api_versions".
    """
    return {
        "chunkedgraph_api_versions": chunkedgraph_api_versions,
        "materialization_api_versions": materialization_api_versions,
        "schema_api_versions": schema_api_versions,
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


def api_version_url(
    server_address: str,
    endpoint_dictionary: dict,
    service_key: str,
):
    return endpoint_dictionary["get_api_versions"].format_map(
        {service_key: server_address}
    )


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
    chunkedgraph_api_versions: Optional[list] = None,
    materialization: bool = False,
    materialization_server_version: str = DEFAULT_MATERIALIZATION_SERVER_VERSON,
    materialization_api_versions: Optional[list] = None,
    available_materialization_versions: Optional[list] = None,
    set_version: Optional[int] = None,
    set_version_metadata: Optional[dict] = None,
    json_service: bool = False,
    json_service_server_version: str = DEFAULT_JSON_SERVICE_SERVER_VERSION,
    skeleton_service: bool = False,
    skeleton_service_server_version: str = DEFAULT_SKELETON_SERVICE_SERVER_VERSION,
    schema_api_versions: Optional[list] = None,
    l2cache: bool = False,
    l2cache_disabled: bool = False,
    global_only: bool = False,
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
        Sets the value of the chunkedgraph server version as a three-element semanatic version (e.g "2.3.4"),
        by default the value in DEFAULT_CHUNKEDGRAPH_SERVER_VERSION.
    chunkedgraph_api_versions : list, optional
        List of chunkedgraph API versions that the chunkedgraph client thinks exists, by default None.
        If None, returns the value in CHUNKEDGRAPH_API_VERSIONS.
    materialization : bool, optional
        If True, configures the client to initalize a materialization subclient, by default False
        Note that materialization being set to True will also configure the chunkedgraph client.
    materialization_server_version : str, optional
        Sets the value of the materialization server version as a three-element semanatic version (e.g "2.3.4"),
        by default the value in DEFAULT_MATERIALIZATION_SERVER_VERSON.
    available_materialization_versions : list, optional
        List of materialization database versions that the materialization client thinks exists, by default None.
        If None, returns the value in DEFAULT_MATERIALIZATION_VERSONS.
    materialization_api_versions : list, optional
        List of materialization API versions that the materialization client thinks exists, by default None.
        If None, returns the value in MATERIALIZATION_API_VERSIONS.
    set_version: int, optional
        If set, will set the version of the materialization server to the value of set_version, by default None.
        To work, this version must be in the list of available materialization versions.
    set_version_metadata: dict, optional
        If set, will set the version metadata of the materialization server to the value of set_version_metadata.
        Default value is in DEFAULT_MATERIALIZATION_VERSION_METADATA.
    json_service : bool, optional
        If True, configures the client to initalize a materialization subclient, by default False
    json_service_server_version : _type_, optional
        Sets the value of the json state server version as a three-element semanatic version (e.g "2.3.4"),
        by default the value in DEFAULT_JSON_SERVICE_SERVER_VERSION.
    skeleton_service : bool, optional
        If True, configures the client to initalize a skeleton service subclient, by default False
    skeleton_service_server_version : _type_, optional
        Sets the value of the skeleton service version as a three-element semanatic version (e.g "2.3.4"),
        by default the value in DEFAULT_SKELETON_SERVICE_SERVER_VERSION.
    l2cache : bool, optional
        If True, configures the client to initialize an l2cache subclient, by default False
        Note that l2cache being set to True will also configure the chunkedgraph client.
    l2cache_disabled : bool, optional
        If True, allows a subclient to be initialized, but emulates a situation without an L2 cache, by default False
        Only used if l2cache is True.
    global_only : bool, optional
        If True, only initializes the global services and does not use a datastack, by default False.

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

    if global_server is None:
        global_server = TEST_GLOBAL_SERVER

    if global_only:
        datastack_name = None
        local_server = None
        chunkedgraph = False
        materialization = False
        skeleton_service = False
        l2cache = False
        set_version = None
    else:
        if datastack_name is None:
            datastack_name = TEST_DATASTACK
        if local_server is None:
            local_server = TEST_LOCAL_SERVER
        if info_file is None:
            info_file = default_info(local_server)
        if available_materialization_versions is None:
            available_materialization_versions = DEFAULT_MATERIALIZATION_VERSONS
        if set_version_metadata is None:
            set_version_metadata = DEFAULT_MATERIALIZATION_VERSION_METADATA
        if chunkedgraph_api_versions is None:
            chunkedgraph_api_versions = CHUNKEDGRAPH_API_VERSIONS
        if materialization_api_versions is None:
            materialization_api_versions = MATERIALIZATION_API_VERSIONS
        if schema_api_versions is None:
            schema_api_versions = SCHEMA_API_VERSIONS

    @responses.activate()
    def mockedCAVEclient():
        url = info_url(datastack_name, global_server)
        responses.add(responses.GET, url=url, json=info_file, status=200)

        if chunkedgraph or l2cache or materialization:
            pcg_version_url = version_url(
                local_server,
                endpoints.chunkedgraph_endpoints_common,
                "cg_server_address",
            )
            responses.add(
                responses.GET,
                pcg_version_url,
                json=str(chunkedgraph_server_version),
                status=200,
            )

            pcg_api_url = api_version_url(
                local_server,
                endpoints.chunkedgraph_endpoints_common,
                "cg_server_address",
            )
            responses.add(
                responses.GET,
                pcg_api_url,
                json=chunkedgraph_api_versions,
                status=200,
            )
        if materialization:
            mat_api_url = api_version_url(
                local_server,
                endpoints.materialization_common,
                "me_server_address",
            )
            responses.add(
                responses.GET,
                mat_api_url,
                json=materialization_api_versions,
            )
            mat_endpoints = endpoints.materialization_api_versions[
                max(materialization_api_versions)
            ]

            mat_version_url = version_url(
                local_server,
                endpoints.materialization_common,
                "me_server_address",
            )
            responses.add(
                responses.GET,
                mat_version_url,
                json=str(materialization_server_version),
                status=200,
            )

            mat_available_endpoint = mat_endpoints["versions"]
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
                match=[query_param_matcher({"expired": True})],
            )

            if set_version is not None:
                mat_mapping["version"] = set_version
                version_metadata_url = mat_endpoints["version_metadata"].format_map(
                    mat_mapping
                )
                responses.add(
                    responses.GET,
                    version_metadata_url,
                    json=set_version_metadata,
                    status=200,
                )
        if json_service:
            js_version_url = version_url(
                global_server,
                endpoints.jsonservice_endpoints_v1,
                "json_server_address",
            )
            responses.add(
                responses.GET,
                js_version_url,
                json=str(json_service_server_version),
                status=200,
            )
        if skeleton_service:
            ss_version_url = version_url(
                local_server,
                endpoints.skeletonservice_endpoints_v1,
                "skeleton_server_address",
            )
            responses.add(
                responses.GET,
                ss_version_url,
                json=str(skeleton_service_server_version),
                status=200,
            )

        client = CAVEclient(
            datastack_name=datastack_name,
            server_address=global_server,
            write_server_cache=False,
            auth_token="just_a_test",
            global_only=global_only,
            version=set_version,
        )
        client.info
        if chunkedgraph or l2cache or materialization:
            client.chunkedgraph
        if materialization:
            client.materialize
        if json_service:
            client.state
        if skeleton_service:
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

            client.l2cache
        return client

    return mockedCAVEclient()
