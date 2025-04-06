import numpy as np
import responses
from responses import matchers

from caveclient.endpoints import infoservice_endpoints_v2

from .conftest import datastack_dict, image_mirrors, mirror_info, test_info


def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert info == test_info


class TestInfoClient:
    default_mapping = {
        "datastack_name": datastack_dict["datastack_name"],
        "i_server_address": datastack_dict["global_server"],
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

    @responses.activate
    def test_mirror_info(self, myclient):
        endpoint_map = myclient.info.default_url_mapping
        responses.add(
            responses.GET,
            url=self.endpoints.get("image_sources").format_map(endpoint_map),
            json=image_mirrors(),
            status=200,
        )
        list_of_imagery = myclient.info.get_image_mirrors()
        assert len(list_of_imagery) == 2

        imagery_names = myclient.info.get_image_mirror_names()
        assert imagery_names[1] == "test_volume_mirror"

        responses.add(
            responses.GET,
            url=self.endpoints.get("datastack_info").format_map(endpoint_map),
            match=[
                matchers.query_param_matcher(
                    {"image_source_name": "test_volume_mirror"}
                )
            ],
            json=mirror_info(),
            status=200,
        )
        info = myclient.info.get_datastack_info(
            image_mirror="test_volume_mirror",
        )
        assert (
            info["aligned_volume"]["image_source"] == list_of_imagery[1]["image_source"]
        )

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
