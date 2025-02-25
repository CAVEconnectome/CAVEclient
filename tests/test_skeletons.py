import binascii
import copy

import deepdiff
import numpy as np
import responses
from packaging.version import Version

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
    "skeleton_version": 4,
    "root_id_prefixes": "0",
    "limit": 0,
    "root_ids": "0,1",
    "root_id": "0",
    "output_format": "flatdict",
    "gen_missing_sks": False,
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
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_versions()
        assert result == [-1, 0, 1, 2, 3, 4]

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
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_cache_contents(None, 4, 0, 0)
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
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.skeletons_exist(None, 4, 0)
        assert result

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
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.skeletons_exist(None, 4, 0)
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
        result = myclient.skeleton.get_precomputed_skeleton_info(4, None)
        assert result == info

    @responses.activate
    def test_get_meshwork(self, myclient, mocker):
        # TODO: Placeholder. Implement this test.
        pass

    @responses.activate
    def test_get_skeleton(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get(
            "get_skeleton_async_via_skvn_rid_fmt"
        ).format_map(sk_mapping)
        sk = {
            "meta": {
                "root_id": 864691135495137700,
                "soma_pt_x": 1134080,
                "soma_pt_y": 793664,
                "soma_pt_z": 867200,
                "soma_radius": 7500,
                "collapse_soma": True,
                "collapse_function": "sphere",
                "invalidation_d": 7500,
                "smooth_vertices": False,
                "compute_radius": False,
                "shape_function": "single",
                "smooth_iterations": 12,
                "smooth_neighborhood": 2,
                "smooth_r": 0.1,
                "cc_vertex_thresh": 0,
                "remove_zero_length_edges": True,
                "collapse_params": {},
                "timestamp": 1736881678.0623715,
                "skeleton_type": "pcg_skel",
                "meta": {
                "datastack": "minnie65_phase3_v1",
                "space": "l2cache"
                },
                "sk_dict_structure_version": 4,
                "skeleton_version": 4
            },
            "edges": [
                [
                1,
                0
                ],
            ],
            "mesh_to_skel_map": [
                0,
                1
            ],
            "root": 0,
            "vertices": [
                [
                1054848., 827272., 601920.
                ],
                [
                1054856., 827192., 601920.
                ],
            ],
            "compartment": [
                3,
                3
            ],
            "radius": [
                203.6853403, 203.6853403
            ],
            'lvl2_ids': [
                173056326983745934, 173126695727923522
            ]
        }

        sk_result = copy.deepcopy(sk)
        sk_result["edges"] = np.array(sk_result["edges"])
        sk_result["mesh_to_skel_map"] = np.array(sk_result["mesh_to_skel_map"])
        sk_result["vertices"] = np.array(sk_result["vertices"])
        sk_result["lvl2_ids"] = np.array(sk_result["lvl2_ids"])
        sk_result["radius"] = np.array(sk_result["radius"])
        sk_result["compartment"] = np.array(sk_result["compartment"])

        dict_bytes = SkeletonClient.compressDictToBytes(sk)
        responses.add(responses.GET, url=metadata_url, body=dict_bytes, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_skeleton(0, None, 4, "dict")
        assert not deepdiff.DeepDiff(result, sk_result)

    @responses.activate
    def test_get_bulk_skeletons(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get(
            "get_bulk_skeletons_via_skvn_rids"
        ).format_map(sk_mapping)
        sk = {
            "meta": {
                "root_id": 864691135495137700,
                "soma_pt_x": 1134080,
                "soma_pt_y": 793664,
                "soma_pt_z": 867200,
                "soma_radius": 7500,
                "collapse_soma": True,
                "collapse_function": "sphere",
                "invalidation_d": 7500,
                "smooth_vertices": False,
                "compute_radius": False,
                "shape_function": "single",
                "smooth_iterations": 12,
                "smooth_neighborhood": 2,
                "smooth_r": 0.1,
                "cc_vertex_thresh": 0,
                "remove_zero_length_edges": True,
                "collapse_params": {},
                "timestamp": 1736881678.0623715,
                "skeleton_type": "pcg_skel",
                "meta": {
                "datastack": "minnie65_phase3_v1",
                "space": "l2cache"
                },
                "sk_dict_structure_version": 4,
                "skeleton_version": 4
            },
            "edges": [
                [
                1,
                0
                ],
            ],
            "mesh_to_skel_map": [
                0,
                1
            ],
            "root": 0,
            "vertices": [
                [
                1054848., 827272., 601920.
                ],
                [
                1054856., 827192., 601920.
                ],
            ],
            "compartment": [
                3,
                3
            ],
            "radius": [
                203.6853403, 203.6853403
            ],
            'lvl2_ids': [
                173056326983745934, 173126695727923522
            ]
        }

        sks_result = {
            '0': sk,
            '1': sk,
        }

        json_content = {
            0: binascii.hexlify(SkeletonClient.compressDictToBytes(sk)).decode('ascii'),
            1: binascii.hexlify(SkeletonClient.compressDictToBytes(sk)).decode('ascii'),
        }
        responses.add(responses.GET, url=metadata_url, json=json_content, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_bulk_skeletons([0, 1], None, 4, "dict")
        assert not deepdiff.DeepDiff(result, sks_result)

    @responses.activate
    def test_generate_bulk_meshworks_async(self, myclient, mocker):
        # TODO: Placeholder. Implement this test.
        pass

    @responses.activate
    def test_generate_bulk_skeletons_async(self, myclient, mocker):
        # TODO: Placeholder. Implement this test.
        pass
