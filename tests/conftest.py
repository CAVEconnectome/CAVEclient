import pytest

from caveclient.tools.testing import (
    CAVEclientMock,
    default_info,
    get_server_information,
    get_server_versions,
)


datastack_dict = get_server_information()
server_versions = get_server_versions()
test_info = default_info(datastack_dict["local_server"])


@pytest.fixture()
def myclient():
    return CAVEclientMock(chunkedgraph=True, materialization=True, **datastack_dict)


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
