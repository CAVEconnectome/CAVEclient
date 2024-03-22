import numpy as np
import responses

from caveclient.endpoints import infoservice_endpoints_v2

from .conftest import TEST_DATASTACK, TEST_GLOBAL_SERVER, test_info


def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert info == test_info


class TestInfoClient:
    default_mapping = {
        "datastack_name": TEST_DATASTACK,
        "i_server_address": TEST_GLOBAL_SERVER,
    }
    endpoints = infoservice_endpoints_v2

    @responses.activate
    def test_infoclient(self, myclient, mocker):
        endpoint_mapping = self.default_mapping  # noqa F841

        assert myclient.info.segmentation_source() == test_info["segmentation_source"]
        assert (
            myclient.info.image_source() == test_info["aligned_volume"]["image_source"]
        )

        viewer_res = np.array(
            [
                test_info["viewer_resolution_x"],
                test_info["viewer_resolution_y"],
                test_info["viewer_resolution_z"],
            ]
        )
        assert np.all(myclient.info.viewer_resolution() == viewer_res)

        assert myclient.info.viewer_site() == test_info["viewer_site"]

        # can't get the server mock to change
        # url_template = infoservice_endpoints_v2["datastack_info"]
        # url = url_template.format_map(endpoint_mapping)
        # new_info = test_info.copy()
        # new_info["voxel_resolution_z"] = 45
        # # change the server side mock to have new z resolution
        # responses.remove(responses.GET, url)
        # responses.add(responses.GET, url, json=new_info, status=200)
        # # see that caching worked and the viewer resolution
        # # is still the old one
        # assert np.all(myclient.info.viewer_resolution() == viewer_res)
        # # get the new data
        # myclient.info.refresh_stored_data()
        # # check that it changed
        # viewer_res[2] = 45
        # assert np.all(myclient.info.viewer_resolution() == viewer_res)
