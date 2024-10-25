import os

import pytest
import responses

from caveclient.tools.testing import (
    CAVEclientMock,
    default_info,
    get_server_information,
)


datastack_dict = get_server_information()
test_info = default_info(datastack_dict["local_server"])


@pytest.fixture()
def myclient():
    return CAVEclientMock(chunkedgraph=True, materialization=True, **datastack_dict)


@pytest.fixture()
@responses.activate
def old_chunkedgraph_client():
    return CAVEclientMock(
        chunkedgraph=True, chunkedgraph_server_version="1.0.0", **datastack_dict
    )
