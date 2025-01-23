import responses
from packaging.version import Version
from unittest import mock

from caveclient import CAVEclient, endpoints
from caveclient.skeletonservice import SkeletonClient

from .conftest import (
    datastack_dict,
    global_client,  # noqa: F401
    mat_apiv2_specified_client,  # noqa: F401
    test_info,
    version_specified_client,  # noqa: F401
)

sk_mapping = {
    "skeleton_server_address": datastack_dict["local_server"],
    "datastack_name": datastack_dict["datastack_name"],
    "skeleton_version": 3,
    "root_id_prefixes": "0",
    "limit": 0,
    "root_ids": "0",
    "root_id": "0",
    "output_format": "flatdict",
}

info_mapping = {
    "i_server_address": datastack_dict["global_server"],
    "datastack_name": datastack_dict["datastack_name"],
}
url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
info_url = url_template.format_map(info_mapping)


class TestSkeletonsClient:
    sk_endpoints = endpoints.skeletonservice_endpoints_v1

    @responses.activate
    def test_create_client(self):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        _ = CAVEclient(
            datastack_dict["datastack_name"],
            server_address=datastack_dict["global_server"],
            write_server_cache=False,
        )

    @responses.activate
    def test_get_version(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get("get_version").format_map(sk_mapping)
        responses.add(responses.GET, url=metadata_url, json="0.1.2", status=200)

        result = myclient.skeleton.get_version()
        assert result == Version("0.1.2")

    @responses.activate
    def test_get_versions(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3], status=200
        )

        result = myclient.skeleton.get_versions()
        assert result == [-1, 0, 1, 2, 3]

    @responses.activate
    def test_get_cache_contents(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get(
            "get_cache_contents_via_skvn_ridprefixes"
        ).format_map(sk_mapping)
        responses.add(
            responses.GET,
            url=metadata_url,
            json={
                "num_found": 1,
                "files": ["filename"],
            },
            status=200,
        )
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3], status=200
        )

        result = myclient.skeleton.get_cache_contents(None, 3, 0, 0)
        assert result == {
            "num_found": 1,
            "files": ["filename"],
        }

    @responses.activate
    def test_skeletons_exist(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get(
            "skeletons_exist_via_skvn_rids_as_post"
        ).format_map(sk_mapping)
        data = {
            0: True,
        }
        responses.add(responses.POST, url=metadata_url, json=data, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3], status=200
        )

        result = myclient.skeleton.skeletons_exist(None, 3, 0)
        assert result == True

    @responses.activate
    def test_multiple_skeletons_exist(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get(
            "skeletons_exist_via_skvn_rids_as_post"
        ).format_map(sk_mapping)
        data = {
            0: True,
            1: False,
        }
        responses.add(responses.POST, url=metadata_url, json=data, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3], status=200
        )

        result = myclient.skeleton.skeletons_exist(None, 3, 0)
        assert result == {
            0: True,
            1: False,
        }

    @responses.activate
    def test_get_precomputed_skeleton_info(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get("skeleton_info_versioned").format_map(
            sk_mapping
        )

        info = {
            "@type": "neuroglancer_skeletons",
            "transform": [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0],
            "vertex_attributes": [
                {"id": "radius", "data_type": "float32", "num_components": 1},
                {"id": "compartment", "data_type": "uint8", "num_components": 1},
            ],
        }

        responses.add(responses.GET, url=metadata_url, json=info, status=200)
        result = myclient.skeleton.get_precomputed_skeleton_info(3, None)
        assert result == info

    @responses.activate
    def test_get_skeleton(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get(
            "get_skeleton_via_skvn_rid_fmt"
        ).format_map(sk_mapping)
        sk = {"test_skeleton": "data"}
        dict_bytes = SkeletonClient.compressDictToBytes(sk)
        responses.add(responses.GET, url=metadata_url, body=dict_bytes, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3], status=200
        )

        result = myclient.skeleton.get_skeleton(0, None, 3, "dict")
        assert result == sk
