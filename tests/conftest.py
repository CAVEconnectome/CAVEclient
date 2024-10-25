import pytest

from caveclient.tools.testing import (
    CAVEclientMock,
    default_info,
    get_server_information,
    get_server_versions,
)


# These are convenience functions to get a mix of specified and default values
datastack_dict = get_server_information()
server_versions = get_server_versions()

test_info = default_info(datastack_dict["local_server"])


@pytest.fixture()
def myclient():
    return CAVEclientMock(
        chunkedgraph=True,
        materialization=True,
        json_service=True,
        skeleton_service=True,
        l2cache=True,
        **datastack_dict,
    )


@pytest.fixture()
def old_chunkedgraph_client():
    return CAVEclientMock(
        chunkedgraph=True, chunkedgraph_server_version="1.0.0", **datastack_dict
    )


@pytest.fixture()
def global_client():
    return CAVEclientMock(
        global_server=datastack_dict["global_server"],
        global_only=True,
        json_service=True,
    )


@pytest.fixture()
def versioned_client():
    return CAVEclientMock(
        chunkedgraph=True,
        materialization=True,
        json_service=True,
        skeleton_service=True,
        l2cache=True,
        available_materialization_versions=[1, 2, 3],
        set_version=3,
        **server_versions,
    )
