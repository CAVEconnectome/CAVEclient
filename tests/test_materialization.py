import pytest
from caveclient import materializationengine
from caveclient.endpoints import (
    materialization_endpoints_v2,
    chunkedgraph_endpoints_common,
)
import pandas as pd
import responses
import pyarrow as pa
from urllib.parse import urlencode
from .conftest import test_info, TEST_LOCAL_SERVER, TEST_DATASTACK
import datetime
import numpy as np


def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert info == test_info


def binary_body_match(body):
    def match(request_body):
        return body == request_body

    return match


class TestChunkedgraphException(Exception):
    """Error to raise is bad values make it to chunkedgraph"""


class TestMatclient:
    default_mapping = {
        "me_server_address": TEST_LOCAL_SERVER,
        "cg_server_address": TEST_LOCAL_SERVER,
        "table_id": test_info["segmentation_source"].split("/")[-1],
        "datastack_name": TEST_DATASTACK,
        "table_name": test_info["synapse_table"],
        "version": 1,
    }
    endpoints = materialization_endpoints_v2

    table_metadata = {
        "aligned_volume": "minnie65_phase3",
        "id": 473,
        "schema": "cell_type_local",
        "valid": True,
        "created": "2021-04-29T05:58:42.196350",
        "table_name": "allen_v1_column_types_slanted",
        "description": "Adaptation of the allen_v1_column_types_v2 table, but with a lower region that follows the natural curvature of the neurons. The direction was estimated from several PT cell axons from within the column. Only neurons are included in this table, not non-neuronal cells. Slanted region and new cell typing was initially done by Casey Schneider-Mizell, with help from Nuno da Costa and Agnes Bodor.",
        "flat_segmentation_source": "",
        "schema_type": "cell_type_local",
        "user_id": "56",
        "reference_table": "",
        "voxel_resolution_x": 4.0,
        "voxel_resolution_y": 4.0,
        "voxel_resolution_z": 40.0,
    }

    synapse_metadata = {
        "aligned_volume": test_info["aligned_volume"],
        "id": 474,
        "schema": "synapse",
        "valid": True,
        "created": "2021-04-29T05:58:42.196350",
        "table_name": test_info["synapse_table"],
        "description": "test synapse table",
        "flat_segmentation_source": "",
        "schema_type": "synapse",
        "user_id": "56",
        "reference_table": None,
        "voxel_resolution": [4.0, 4.0, 40.0],
    }

    @responses.activate
    def test_matclient(self, myclient, mocker):
        endpoint_mapping = self.default_mapping
        api_versions_url = chunkedgraph_endpoints_common["get_api_versions"].format_map(
            endpoint_mapping
        )
        responses.add(responses.GET, url=api_versions_url, json=[0, 1], status=200)

        versionurl = self.endpoints["versions"].format_map(endpoint_mapping)

        responses.add(responses.GET, url=versionurl, json=[1], status=200)

        url = self.endpoints["simple_query"].format_map(endpoint_mapping)
        syn_md_url = self.endpoints["metadata"].format_map(endpoint_mapping)

        responses.add(responses.GET, url=syn_md_url, json=self.synapse_metadata)

        query_d = {"return_pyarrow": True, "split_positions": True}
        query_string = urlencode(query_d)
        url = url + "?" + query_string
        correct_query_data = {
            "filter_in_dict": {test_info["synapse_table"]: {"pre_pt_root_id": [500]}},
            "filter_notin_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [501]}
            },
            "filter_equal_dict": {test_info["synapse_table"]: {"size": 100}},
            "offset": 0,
            "limit": 1000,
        }
        correct_query_data_with_desired_resolution = {
            "filter_in_dict": {test_info["synapse_table"]: {"pre_pt_root_id": [500]}},
            "filter_notin_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [501]}
            },
            "filter_equal_dict": {test_info["synapse_table"]: {"size": 100}},
            "offset": 0,
            "limit": 1000,
            "desired_resolution": [1, 1, 1],
        }
        df = pd.read_pickle("tests/test_data/synapse_query_split.pkl")
        df_pos = df.copy()
        pos_cols = ["pre_pt_position", "ctr_pt_position", "post_pt_position"]
        res = [4, 4, 40]
        xyz = ["_x", "_y", "_z"]
        for col in pos_cols:
            for r, d in zip(res, xyz):
                cx = col + d
                df_pos[cx] = df_pos[cx] * r

        context = pa.default_serialization_context()
        serialized = context.serialize(df)

        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )
        context = pa.default_serialization_context()
        pos_serialized = context.serialize(df_pos)
        responses.add(
            responses.POST,
            url=url,
            body=pos_serialized.to_buffer().to_pybytes(),
            headers={
                "content-type": "x-application/pyarrow",
                "dataframe_resolution": "1, 1, 1",
            },
            match=[
                responses.json_params_matcher(
                    correct_query_data_with_desired_resolution
                )
            ],
        )

        meta_url = self.endpoints["metadata"].format_map(endpoint_mapping)
        responses.add(responses.GET, url=meta_url, json=self.table_metadata)

        df = myclient.materialize.query_table(
            test_info["synapse_table"],
            filter_in_dict={"pre_pt_root_id": [500]},
            filter_out_dict={"post_pt_root_id": [501]},
            filter_equal_dict={"size": 100},
            limit=1000,
            offset=0,
        )
        assert len(df) == 1000
        assert type(df) == pd.DataFrame
        assert df.attrs["table_id"] == self.table_metadata["id"]

        correct_metadata = [
            {
                "version": 1,
                "expires_on": "2021-04-19T08:10:00.255735",
                "id": 84,
                "valid": True,
                "time_stamp": "2021-04-12T08:10:00.255735",
                "datastack": "minnie65_phase3_v1",
            }
        ]

        past_timestamp = materializationengine.convert_timestamp(
            datetime.datetime.strptime(
                correct_metadata[0]["time_stamp"], "%Y-%m-%dT%H:%M:%S.%f"
            )
        )

        md_url = self.endpoints["versions_metadata"].format_map(endpoint_mapping)
        responses.add(responses.GET, url=md_url, json=correct_metadata, status=200)

        meta_url = self.endpoints["metadata"].format_map(endpoint_mapping)
        responses.add(responses.GET, url=meta_url, json=self.table_metadata)

        bad_time = materializationengine.convert_timestamp(
            datetime.datetime(
                year=2020, month=4, day=19, hour=0, tzinfo=datetime.timezone.utc
            )
        )
        good_time = materializationengine.convert_timestamp(
            datetime.datetime(
                year=2021, month=4, day=19, hour=0, tzinfo=datetime.timezone.utc
            )
        )

        with pytest.raises(ValueError):
            df = myclient.materialize.live_query(
                test_info["synapse_table"],
                bad_time,
                filter_in_dict={"pre_pt_root_id": [600]},
                filter_out_dict={"post_pt_root_id": [601]},
                filter_equal_dict={"size": 100},
                limit=1000,
                offset=0,
            )

        ### live query test
        def my_get_roots(self, supervoxel_ids, timestamp=None, stop_layer=None):
            if 0 in supervoxel_ids:
                raise TestChunkedgraphException(
                    ("should not call get roots on svid =0")
                )
            if timestamp == good_time:
                sv_lookup = {
                    1: 200,
                    2: 200,
                    3: 201,
                    4: 201,
                    5: 203,
                    6: 203,
                    7: 203,
                    8: 103,
                    9: 103,
                    10: 103,
                    11: 200,
                    12: 103,
                    13: 203,
                    14: 201,
                    15: 201,
                }

            elif timestamp == past_timestamp:
                sv_lookup = {
                    1: 100,
                    2: 100,
                    3: 100,
                    4: 100,
                    5: 101,
                    6: 102,
                    7: 102,
                    8: 103,
                    9: 103,
                    10: 103,
                    11: 100,
                    12: 103,
                    13: 102,
                    14: 100,
                    15: 100,
                }
            else:
                raise ValueError("Mock is not defined at this time")
            return np.array([sv_lookup[sv] for sv in supervoxel_ids])

        def mocked_get_past_ids(
            self, root_ids, timestamp_past=None, timestamp_future=None
        ):
            if 0 in root_ids:
                raise TestChunkedgraphException(("should not past_ids on svid =0"))
            id_map = {201: [100], 103: [103], 203: [101, 102]}
            return {
                "future_id_map": {},
                "past_id_map": {k: id_map[k] for k in root_ids},
            }

        def mock_is_latest_roots(self, root_ids, timestamp=None):
            if 0 in root_ids:
                raise TestChunkedgraphException(
                    ("should not call is_latest on svid =0")
                )
            if timestamp == good_time:
                is_latest = {
                    100: False,
                    101: False,
                    102: False,
                    103: True,
                    200: True,
                    201: True,
                    202: True,
                    203: True,
                    303: True,
                }

            elif timestamp == past_timestamp:
                is_latest = {
                    100: True,
                    101: True,
                    102: True,
                    103: True,
                    200: False,
                    201: False,
                    202: False,
                    203: False,
                    303: True,
                }
            else:
                raise ValueError("Mock is not defined at this time")
            return np.array([is_latest[root_id] for root_id in root_ids])

        def mock_get_root_timestamps(self, root_ids):
            timestamp_dict = {
                100: bad_time - datetime.timedelta(days=1),
                101: bad_time - datetime.timedelta(days=1),
                102: bad_time - datetime.timedelta(days=1),
                103: bad_time - datetime.timedelta(days=1),
                200: good_time - datetime.timedelta(days=1),
                201: good_time - datetime.timedelta(days=1),
                202: good_time - datetime.timedelta(days=1),
                203: good_time - datetime.timedelta(days=1),
                303: good_time + datetime.timedelta(days=1),
            }
            return np.array([timestamp_dict[root_id] for root_id in root_ids])

        mocker.patch(
            "caveclient.chunkedgraph.ChunkedGraphClientV1.get_roots",
            my_get_roots,
        )
        mocker.patch(
            "caveclient.chunkedgraph.ChunkedGraphClientV1.get_past_ids",
            mocked_get_past_ids,
        )
        mocker.patch(
            "caveclient.chunkedgraph.ChunkedGraphClientV1.is_latest_roots",
            mock_is_latest_roots,
        )
        mocker.patch(
            "caveclient.chunkedgraph.ChunkedGraphClientV1.get_root_timestamps",
            mock_get_root_timestamps,
        )
        df = pd.read_pickle("tests/test_data/live_query_before.pkl")

        context = pa.default_serialization_context()
        serialized = context.serialize(df)
        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"pre_pt_root_id": [100, 103]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )
        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [100, 101, 102]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )
        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [101, 102]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )

        dfq = myclient.materialize.live_query(
            test_info["synapse_table"],
            good_time,
            filter_in_dict={"pre_pt_root_id": [201, 103]},
        )

        dfr = pd.read_pickle("tests/test_data/live_query_after1.pkl")
        assert np.all(dfq.pre_pt_root_id == dfr.pre_pt_root_id)
        assert np.all(dfq.post_pt_root_id == dfr.post_pt_root_id)

        dfq = myclient.materialize.live_query(
            test_info["synapse_table"],
            good_time,
            filter_in_dict={"post_pt_root_id": [201, 203]},
        )

        dfr = pd.read_pickle("tests/test_data/live_query_after2.pkl")
        assert np.all(dfq.pre_pt_root_id == dfr.pre_pt_root_id)
        assert np.all(dfq.post_pt_root_id == dfr.post_pt_root_id)

        dfq = myclient.materialize.live_query(
            test_info["synapse_table"],
            good_time,
            filter_equal_dict={"post_pt_root_id": 203},
        )
        dfr = pd.read_pickle("tests/test_data/live_query_after3.pkl")
        assert np.all(dfq.pre_pt_root_id == dfr.pre_pt_root_id)
        assert np.all(dfq.post_pt_root_id == dfr.post_pt_root_id)

        df_ct = pd.read_pickle("tests/test_data/cell_types.pkl")
        context = pa.default_serialization_context()
        serialized = context.serialize(df_ct)

        endpoint_mapping["table_name"] = "cell_types"
        url = self.endpoints["simple_query"].format_map(endpoint_mapping)
        query_d = {"return_pyarrow": True, "split_positions": True}
        query_string = urlencode(query_d)
        url = url + "?" + query_string

        meta_url = self.endpoints["metadata"].format_map(endpoint_mapping)
        responses.add(responses.GET, url=meta_url, json=self.table_metadata)

        correct_query_data = {}
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )
        dfq = myclient.materialize.live_query(
            "cell_types", good_time, split_positions=True
        )

        correct_ct = pd.read_pickle("tests/test_data/cell_types_live.pkl")
        assert np.all(correct_ct.pt_root_id == dfq.pt_root_id)

        correct_query_data = {"filter_equal_dict": {"cell_types": {"cell_type": "BC"}}}
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={"content-type": "x-application/pyarrow"},
            match=[responses.json_params_matcher(correct_query_data)],
        )
        dfq = myclient.materialize.live_query(
            "cell_types",
            good_time,
            filter_equal_dict={"cell_type": "BC"},
            split_positions=True,
        )

        cdf = correct_ct.query('cell_type=="BC"')
        assert np.all(cdf.pt_root_id == dfq.pt_root_id)
        assert np.all(cdf.cell_type == dfq.cell_type)

        dfq = myclient.materialize.live_query(
            "cell_types",
            good_time,
            filter_equal_dict={"cell_type": "BC"},
            split_positions=False,
        )
        cdf = correct_ct.query('cell_type=="BC"')
        assert np.all(cdf.pt_root_id == dfq.pt_root_id)
        assert np.all(cdf.cell_type == dfq.cell_type)
        x = cdf.iloc[0]
        pos = np.array([x.pt_position_x, x.pt_position_y, x.pt_position_z])
        assert np.all(dfq.pt_position.iloc[0] == pos)

        with pytest.raises(ValueError):
            dfq = myclient.materialize.live_query(
                test_info["synapse_table"],
                good_time,
                filter_in_dict={"pre_pt_root_id": [303]},
            )

        ### testing desired resolution
        orig_df = pd.read_pickle("tests/test_data/synapse_query_split.pkl")
        df = myclient.materialize.query_table(
            test_info["synapse_table"],
            filter_in_dict={"pre_pt_root_id": [500]},
            filter_out_dict={"post_pt_root_id": [501]},
            filter_equal_dict={"size": 100},
            limit=1000,
            offset=0,
            desired_resolution=[1, 1, 1],
        )
        orig_xyz = orig_df[
            ["ctr_pt_position_x", "ctr_pt_position_y", "ctr_pt_position_z"]
        ].values
        new_xyz = np.vstack(df.ctr_pt_position.values)
        assert np.all(new_xyz == orig_xyz * [4, 4, 40])

        myclient._materialize = None
        myclient.desired_resolution = [1, 1, 1]
        df = myclient.materialize.query_table(
            test_info["synapse_table"],
            filter_in_dict={"pre_pt_root_id": [500]},
            filter_out_dict={"post_pt_root_id": [501]},
            filter_equal_dict={"size": 100},
            limit=1000,
            offset=0,
        )
        new_xyz = np.vstack(df.ctr_pt_position.values)
        assert np.all(new_xyz == orig_xyz * [4, 4, 40])
