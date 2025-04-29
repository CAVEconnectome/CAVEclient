import binascii
import copy

import deepdiff
import numpy as np
import pandas as pd
import responses
from packaging.version import Version
from requests import HTTPError

from caveclient import CAVEclient, endpoints
from caveclient.skeletonservice import SkeletonClient

from .conftest import (
    datastack_dict,
    global_client,  # noqa: F401
    mat_apiv2_specified_client,  # noqa: F401
    server_versions,
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
    "gen_missing_sks": False,
}

sk_flatdict = copy.deepcopy(sk_mapping)
sk_flatdict["output_format"] = "flatdict"

sk_swc = copy.deepcopy(sk_mapping)
sk_swc["output_format"] = "swccompressed"

info_mapping = {
    "i_server_address": datastack_dict["global_server"],
    "datastack_name": datastack_dict["datastack_name"],
}
url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
info_url = url_template.format_map(info_mapping)
info_version_url = endpoints.infoservice_common["get_version"].format_map(info_mapping)


class TestSkeletonsClient:
    sk_endpoints = endpoints.skeletonservice_endpoints_v1

    @responses.activate
    def test_create_client(self):
        responses.add(
            responses.GET,
            url=info_version_url,
            json=str(server_versions["info_server_version"]),
            status=200,
        )

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
            "get_cache_contents_via_skvn_ridprefixes_limit"
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
    def test_get_skeleton__dict(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get(
            "get_skeleton_async_via_skvn_rid_fmt"
        ).format_map(sk_flatdict)
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
                "meta": {"datastack": "minnie65_phase3_v1", "space": "l2cache"},
                "sk_dict_structure_version": 4,
                "skeleton_version": 4,
            },
            "edges": [
                [1, 0],
            ],
            "mesh_to_skel_map": [0, 1],
            "root": 0,
            "vertices": [
                [1054848.0, 827272.0, 601920.0],
                [1054856.0, 827192.0, 601920.0],
            ],
            "compartment": [3, 3],
            "radius": [203.6853403, 203.6853403],
            "lvl2_ids": [173056326983745934, 173126695727923522],
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
    def test_get_skeleton__swc(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get(
            "get_skeleton_async_via_skvn_rid_fmt"
        ).format_map(sk_swc)

        sk_df = pd.DataFrame(
            [[0, 0, 0, 0, 0, 1, -1]],
            columns=["id", "type", "x", "y", "z", "radius", "parent"],
        )
        sk_csv_str = sk_df.to_csv(index=False, header=False, sep=" ")
        swc_bytes = SkeletonClient.compressStringToBytes(sk_csv_str)

        responses.add(responses.GET, url=metadata_url, body=swc_bytes, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_skeleton(0, None, 4, "swc")
        dif = result.compare(sk_df)
        assert dif.empty

    @responses.activate
    def test_get_skeleton__invalid_output_format(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        for output_format in [
            "",
            "asdf",
            "flatdict",
            "json",
            "jsoncompressed",
            "swccompressed",
        ]:
            try:
                myclient.skeleton.get_skeleton(0, None, 4, output_format)
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown output format: {output_format}. Valid options: ['dict', 'swc']"
                )

    @responses.activate
    def test_get_skeleton__invalid_skeleton_version(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        for skeleton_version in [-2, 999]:
            try:
                myclient.skeleton.get_skeleton(0, None, skeleton_version, "dict")
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown skeleton version: {skeleton_version}. Valid options: [-1, 0, 1, 2, 3, 4]"
                )

    @responses.activate
    def test_get_skeleton__invalid_layer(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        try:
            myclient.skeleton.get_skeleton(2, None, 4, "dict")
            assert False
        except ValueError as e:
            assert (
                e.args[0]
                == "Invalid root id: 2 (perhaps this is an id corresponding to a different level of the PCG, e.g., a supervoxel id)"
            )

    @responses.activate
    def test_get_skeleton__invalid_nodes(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=False)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        try:
            myclient.skeleton.get_skeleton(0, None, 4, "dict")
            assert False
        except ValueError as e:
            assert (
                e.args[0]
                == "Invalid root id: 0 (perhaps it doesn't exist; the error is unclear)"
            )

    @responses.activate
    def test_get_skeleton__refusal_list(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        metadata_url = self.sk_endpoints.get(
            "get_skeleton_async_via_skvn_rid_fmt"
        ).format_map(sk_flatdict)

        responses.add(
            responses.GET,
            url=metadata_url,
            json='"Error": "Problematic root id: 112233445566778899 is in the refusal list"',
            status=400,
        )

        try:
            myclient.skeleton.get_skeleton(0, None, 4, "dict")
            assert False
        except HTTPError as e:
            assert (
                e.args[0]
                == '400 Client Error: Bad Request for url: https://local.cave.com/skeletoncache/api/v1/test_stack/async/get_skeleton/4/0/flatdict?verbose_level=0 content: b\'"\\\\"Error\\\\": \\\\"Problematic root id: 112233445566778899 is in the refusal list\\\\""\''
            )

    @responses.activate
    def test_get_bulk_skeletons__dict(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get(
            "get_bulk_skeletons_via_skvn_rids"
        ).format_map(sk_flatdict)
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
                "meta": {"datastack": "minnie65_phase3_v1", "space": "l2cache"},
                "sk_dict_structure_version": 4,
                "skeleton_version": 4,
            },
            "edges": [
                [1, 0],
            ],
            "mesh_to_skel_map": [0, 1],
            "root": 0,
            "vertices": [
                [1054848.0, 827272.0, 601920.0],
                [1054856.0, 827192.0, 601920.0],
            ],
            "compartment": [3, 3],
            "radius": [203.6853403, 203.6853403],
            "lvl2_ids": [173056326983745934, 173126695727923522],
        }

        sks_result = {
            "0": sk,
            "1": sk,
        }

        json_content = {
            0: binascii.hexlify(SkeletonClient.compressDictToBytes(sk)).decode("ascii"),
            1: binascii.hexlify(SkeletonClient.compressDictToBytes(sk)).decode("ascii"),
        }
        responses.add(responses.GET, url=metadata_url, json=json_content, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_bulk_skeletons([0, 1], None, 4, "dict")
        assert not deepdiff.DeepDiff(result, sks_result)

    @responses.activate
    def test_get_bulk_skeletons__swc(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get(
            "get_bulk_skeletons_via_skvn_rids"
        ).format_map(sk_swc)

        sk_result = {}
        responses.add(responses.GET, url=metadata_url, json=sk_result, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.get_bulk_skeletons([0, 1], None, 4, "swc")
        assert not deepdiff.DeepDiff(result, sk_result)

    @responses.activate
    def test_get_bulk_skeletons__invalid_output_format(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        for output_format in [
            "",
            "asdf",
            "flatdict",
            "json",
            "jsoncompressed",
            "swccompressed",
        ]:
            try:
                myclient.skeleton.get_bulk_skeletons([0, 1], None, 4, output_format)
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown output format: {output_format}. Valid options: ['dict', 'swc']"
                )

    @responses.activate
    def test_get_bulk_skeletons__invalid_skeleton_version(self, myclient, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        for skeleton_version in [-2, 999]:
            try:
                myclient.skeleton.get_bulk_skeletons(
                    [0, 1], None, skeleton_version, "dict"
                )
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown skeleton version: {skeleton_version}. Valid options: [-1, 0, 1, 2, 3, 4]"
                )

    @responses.activate
    def test_generate_bulk_skeletons_async(self, myclient, my_cloudvolume, mocker):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)
        mocker.patch.object(myclient.chunkedgraph, "is_valid_nodes", return_value=True)
        mocker.patch.object(
            myclient.info, "segmentation_cloudvolume", return_value=my_cloudvolume
        )

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        metadata_url = self.sk_endpoints.get(
            "gen_bulk_skeletons_via_skvn_rids_as_post"
        ).format_map(sk_mapping)

        data = 60.0
        responses.add(responses.POST, url=metadata_url, json=data, status=200)

        result = myclient.skeleton.generate_bulk_skeletons_async(
            [0, 1], datastack_dict["datastack_name"], 4
        )
        assert result == 60.0

    @responses.activate
    def test_generate_bulk_skeletons_async__invalid_skeleton_version(
        self, myclient, mocker
    ):
        mocker.patch.object(myclient.l2cache, "has_cache", return_value=True)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        for skeleton_version in [-2, 999]:
            try:
                myclient.skeleton.generate_bulk_skeletons_async(
                    [0, 1], datastack_dict["datastack_name"], skeleton_version
                )
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown skeleton version: {skeleton_version}. Valid options: [-1, 0, 1, 2, 3, 4]"
                )
