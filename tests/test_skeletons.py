import binascii
import copy
import urllib.parse

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
    def test_get_refusal_list(self, myclient, mocker):
        refusal_rows1 = [
            ["datastack", 112233445566778899],
            ["datastack", 223344556677889900],
        ]
        refusal_rows2 = [[str(v) for v in row] for row in refusal_rows1]
        refusal_rows3 = [",".join(row) for row in refusal_rows2]
        refusal_list_str = "\n".join(refusal_rows3)
        refusal_list_compressed = SkeletonClient.compressStringToBytes(refusal_list_str)

        metadata_url = self.sk_endpoints.get("get_refusal_list").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, body=refusal_list_compressed, status=200
        )

        result_df = myclient.skeleton.get_refusal_list()

        df1 = pd.DataFrame(
            refusal_rows1,
            columns=["DATASTACK_NAME", "ROOT_ID"],
        )

        assert result_df.equals(df1)

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

    @responses.activate
    def test_fetch_skeletons__server__dict(self, myclient, mocker):
        sk = {
            "meta": {"root_id": 0, "skeleton_version": 4},
            "edges": [[1, 0]],
            "mesh_to_skel_map": [0, 1],
            "root": 0,
            "vertices": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        }

        # Server returns a flat {rid: hex_data} dict (rid 1 is absent — not in cache)
        json_content = {
            "0": binascii.hexlify(
                SkeletonClient.compressDictToBytes(sk)
            ).decode("ascii"),
        }

        bulk_mapping = copy.deepcopy(sk_mapping)
        bulk_mapping["output_format"] = "flatdict"
        metadata_url = self.sk_endpoints.get(
            "get_cached_skeletons_bulk_as_post"
        ).format_map(bulk_mapping)
        responses.add(responses.POST, url=metadata_url, json=json_content, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.fetch_skeletons([0, 1])
        assert "0" in result
        assert "1" not in result

    @responses.activate
    def test_fetch_skeletons__server__swc(self, myclient, mocker):
        sk_df = pd.DataFrame(
            [[0, 0, 0, 0, 0, 1, -1]],
            columns=["id", "type", "x", "y", "z", "radius", "parent"],
        )
        sk_csv_str = sk_df.to_csv(index=False, header=False, sep=" ")
        encoded = binascii.hexlify(sk_csv_str.encode()).decode("ascii")

        json_content = {"0": encoded}

        bulk_mapping = copy.deepcopy(sk_mapping)
        bulk_mapping["output_format"] = "swccompressed"
        metadata_url = self.sk_endpoints.get(
            "get_cached_skeletons_bulk_as_post"
        ).format_map(bulk_mapping)
        responses.add(responses.POST, url=metadata_url, json=json_content, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.fetch_skeletons([0], output_format="swc")
        assert "0" in result
        assert isinstance(result["0"], pd.DataFrame)

    @responses.activate
    def test_fetch_skeletons__truncation(self, myclient, mocker):
        json_content = {}

        bulk_mapping = copy.deepcopy(sk_mapping)
        bulk_mapping["output_format"] = "flatdict"
        metadata_url = self.sk_endpoints.get(
            "get_cached_skeletons_bulk_as_post"
        ).format_map(bulk_mapping)
        responses.add(responses.POST, url=metadata_url, json=json_content, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        # Should not raise, just truncate silently (with a warning)
        result = myclient.skeleton.fetch_skeletons(list(range(600)))
        assert result == {}

    @responses.activate
    def test_fetch_skeletons__invalid_output_format(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        for output_format in ["", "asdf", "flatdict", "json"]:
            try:
                myclient.skeleton.fetch_skeletons(
                    [0], output_format=output_format
                )
                assert False
            except ValueError as e:
                assert "output_format must be 'dict' or 'swc'" in e.args[0]

    @responses.activate
    def test_fetch_skeletons__invalid_skeleton_version(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        for skeleton_version in [-2, 999]:
            try:
                myclient.skeleton.fetch_skeletons(
                    [0], skeleton_version=skeleton_version
                )
                assert False
            except ValueError as e:
                assert (
                    e.args[0]
                    == f"Unknown skeleton version: {skeleton_version}. Valid options: [-1, 0, 1, 2, 3, 4]"
                )

    @responses.activate
    def test_fetch_skeletons__gcs(self, myclient, mocker):
        import gzip
        import io

        import h5py

        # Create a minimal gzip-compressed H5 file in memory
        h5_buf = io.BytesIO()
        with h5py.File(h5_buf, "w") as f:
            f.create_dataset("vertices", data=np.array([[1.0, 2.0, 3.0]]))
            f.create_dataset("edges", data=np.array([[0, 0]]))
        h5_bytes = h5_buf.getvalue()
        gz_bytes = gzip.compress(h5_bytes)

        bucket = "test-bucket"
        path_template = "skeletons/v4/skeleton__v4__rid-{rid}__ds-test.h5.gz"
        obj_path_0 = path_template.format(rid=0)
        encoded_path_0 = urllib.parse.quote(obj_path_0, safe="")
        gcs_url_0 = f"https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{encoded_path_0}?alt=media"

        token_mapping = copy.deepcopy(sk_mapping)
        token_url = self.sk_endpoints.get(
            "get_skeleton_token_as_post"
        ).format_map(token_mapping)
        token_response = {
            "token": "ya29.test_token",
            "token_type": "Bearer",
            "expiry": "2099-01-01T00:00:00+00:00",
            "bucket": bucket,
            "path_template": path_template,
        }
        responses.add(responses.POST, url=token_url, json=token_response, status=200)

        responses.add(responses.GET, url=gcs_url_0, body=gz_bytes, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.fetch_skeletons([0], method="gcs")
        assert "0" in result
        assert "vertices" in result["0"]
        assert np.array_equal(result["0"]["vertices"], np.array([[1.0, 2.0, 3.0]]))

    @responses.activate
    def test_fetch_skeletons__gcs__missing(self, myclient, mocker):
        """Test that a 404 skeleton is absent from result while others succeed."""
        import gzip
        import io

        import h5py

        # Create a valid gzip-compressed H5 file
        h5_buf = io.BytesIO()
        with h5py.File(h5_buf, "w") as f:
            f.create_dataset("vertices", data=np.array([[1.0, 2.0, 3.0]]))
            f.create_dataset("edges", data=np.array([[0, 0]]))
        h5_bytes = h5_buf.getvalue()
        gz_bytes = gzip.compress(h5_bytes)

        bucket = "test-bucket"
        path_template = "skeletons/v4/skeleton__v4__rid-{rid}__ds-test.h5.gz"

        obj_path_0 = path_template.format(rid=0)
        encoded_path_0 = urllib.parse.quote(obj_path_0, safe="")
        gcs_url_0 = f"https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{encoded_path_0}?alt=media"
        responses.add(responses.GET, url=gcs_url_0, status=404)

        obj_path_1 = path_template.format(rid=1)
        encoded_path_1 = urllib.parse.quote(obj_path_1, safe="")
        gcs_url_1 = f"https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{encoded_path_1}?alt=media"
        responses.add(responses.GET, url=gcs_url_1, body=gz_bytes, status=200)

        token_mapping = copy.deepcopy(sk_mapping)
        token_url = self.sk_endpoints.get(
            "get_skeleton_token_as_post"
        ).format_map(token_mapping)
        token_response = {
            "token": "ya29.test_token",
            "token_type": "Bearer",
            "expiry": "2099-01-01T00:00:00+00:00",
            "bucket": bucket,
            "path_template": path_template,
        }
        responses.add(responses.POST, url=token_url, json=token_response, status=200)

        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)
        responses.add(
            responses.GET, url=metadata_url, json=[-1, 0, 1, 2, 3, 4], status=200
        )

        result = myclient.skeleton.fetch_skeletons([0, 1], method="gcs")
        assert "0" not in result
        assert "1" in result
        assert np.array_equal(result["1"]["vertices"], np.array([[1.0, 2.0, 3.0]]))

    def test_fetch_skeletons__gcs__swc_raises(self, myclient, mocker):
        """method='gcs' should raise ValueError for output_format='swc'."""
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                url=metadata_url,
                json=[-1, 0, 1, 2, 3, 4],
                status=200,
            )
            try:
                myclient.skeleton.fetch_skeletons([0], method="gcs", output_format="swc")
                assert False
            except ValueError as e:
                assert "method='gcs' only supports output_format='dict'" in e.args[0]

    def test_fetch_skeletons__invalid_method(self, myclient, mocker):
        """Unknown method value should raise ValueError."""
        metadata_url = self.sk_endpoints.get("get_versions").format_map(sk_mapping)

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.GET,
                url=metadata_url,
                json=[-1, 0, 1, 2, 3, 4],
                status=200,
            )
            try:
                myclient.skeleton.fetch_skeletons([0], method="bogus")
                assert False
            except ValueError as e:
                assert "method must be 'server' or 'gcs'" in e.args[0]
