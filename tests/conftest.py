import pytest

from caveclient.tools.testing import (
    TEST_LOCAL_SERVER,
    CAVEclientMock,
    CloudVolumeMock,
    default_info,
    get_server_information,
    get_server_versions,
)

datastack_dict = get_server_information()
server_versions = get_server_versions()

test_info = default_info(datastack_dict["local_server"])


def mirror_info(
    local_server: str = TEST_LOCAL_SERVER,
):
    info = default_info(local_server)
    info["aligned_volume"] = image_mirrors(local_server)[1]
    return info


def image_mirrors(
    local_server: str = TEST_LOCAL_SERVER,
):
    return [
        {
            "name": "test_volume",
            "image_source": f"precomputed://https://{local_server}/test-em/v1",
            "id": 1,
            "description": "This is a test only dataset.",
        },
        {
            "name": "test_volume_mirror",
            "image_source": f"precomputed://https://{local_server}/test-em/mirror",
            "id": 2,
            "description": "This is a test mirror dataset.",
        },
    ]


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
def version_specified_client():
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


@pytest.fixture()
def mat_apiv2_specified_client():
    return CAVEclientMock(
        materialization=True,
        available_materialization_versions=[1, 2, 3],
        materialization_api_versions=[2],
        **server_versions,
    )


@pytest.fixture()
def my_cloudvolume():
    return CloudVolumeMock()
