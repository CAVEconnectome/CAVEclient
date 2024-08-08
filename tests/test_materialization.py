import copy
import datetime
from io import BytesIO
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import responses
from responses.matchers import json_params_matcher, query_param_matcher

from caveclient import materializationengine
from caveclient.endpoints import (
    chunkedgraph_endpoints_common,
    materialization_common,
    materialization_endpoints_v2,
    materialization_endpoints_v3,
    schema_endpoints_v2,
)

from .conftest import TEST_DATASTACK, TEST_GLOBAL_SERVER, TEST_LOCAL_SERVER, test_info


def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert info == test_info


def binary_body_match(body):
    def match(request_body):
        return body == request_body

    return match


class ChunkedgraphTestException(Exception):
    """Error to raise is bad values make it to chunkedgraph"""


def serialize_dataframe(df, compression="zstd"):
    batch = pa.RecordBatch.from_pandas(df)
    sink = pa.BufferOutputStream()
    opt = pa.ipc.IpcWriteOptions(compression=compression)
    with pa.ipc.new_stream(sink, batch.schema, options=opt) as writer:
        writer.write_batch(batch)
    return BytesIO(sink.getvalue().to_pybytes()).getvalue()


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

    multitable_meta = [
        {
            "created": "2023-08-21T21:19:05.917336",
            "valid": True,
            "id": 32080,
            "table_name": "allen_column_mtypes_v2",
            "aligned_volume": "minnie65_phase3",
            "schema": "cell_type_reference",
            "schema_type": "cell_type_reference",
            "user_id": "56",
            "description": "Cluster-based M-types for the minnie column corresponding the updated clustering used for the revision of Schneider-Mizell et al. 2023. Maintained and created by Casey Schneider-Mizell [Note: This table 'allen_column_mtypes_v2' will update the 'target_id' foreign_key when updates are made to the 'nucleus_detection_v0' table] ",
            "notice_text": None,
            "reference_table": "nucleus_detection_v0",
            "flat_segmentation_source": None,
            "write_permission": "PRIVATE",
            "read_permission": "PUBLIC",
            "last_modified": "2023-08-21T22:53:37.378621",
            "voxel_resolution": [4.0, 4.0, 40.0],
        },
        {
            "created": "2020-11-02T18:56:35.530100",
            "valid": True,
            "id": 32131,
            "table_name": "nucleus_detection_v0__minnie3_v1",
            "aligned_volume": "minnie65_phase3",
            "schema": "nucleus_detection",
            "schema_type": "nucleus_detection",
            "user_id": "121",
            "description": "A table of nuclei detections from a nucleus detection model developed by Shang Mu, Leila Elabbady, Gayathri Mahalingam and Forrest Collman. Pt is the centroid of the nucleus detection. id corresponds to the flat_segmentation_source segmentID. Only included nucleus detections of volume>25 um^3, below which detections are false positives, though some false positives above that threshold remain. ",
            "notice_text": None,
            "reference_table": None,
            "flat_segmentation_source": "precomputed://https://bossdb-open-data.s3.amazonaws.com/iarpa_microns/minnie/minnie65/nuclei",
            "write_permission": "PRIVATE",
            "read_permission": "PUBLIC",
            "last_modified": "2022-10-25T19:24:28.559914",
            "segmentation_source": "",
            "pcg_table_name": "minnie3_v1",
            "last_updated": "2024-03-12T20:00:00.168161",
            "annotation_table": "nucleus_detection_v0",
            "voxel_resolution": [4.0, 4.0, 40.0],
        },
    ]

    multischema_meta = {
        "cell_type_reference": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": {
                "CellTypeReference": {
                    "required": ["cell_type", "classification_system", "target_id"],
                    "properties": {
                        "cell_type": {
                            "title": "cell_type",
                            "type": "string",
                            "description": "Cell type name",
                        },
                        "classification_system": {
                            "title": "classification_system",
                            "type": "string",
                            "description": "Classification system followed",
                        },
                        "target_id": {"type": "integer"},
                        "valid": {
                            "title": "valid",
                            "type": ["boolean", "null"],
                            "default": False,
                            "description": "is this annotation valid",
                        },
                    },
                    "type": "object",
                    "additionalProperties": True,
                }
            },
            "$ref": "#/definitions/CellTypeReference",
        },
        "nucleus_detection": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": {
                "SpatialPoint": {
                    "required": ["position"],
                    "properties": {"position": {"type": "array"}},
                    "type": "object",
                    "additionalProperties": False,
                },
                "BoundSpatialPoint": {
                    "required": ["position"],
                    "properties": {
                        "position": {"type": "array"},
                        "root_id": {"type": "integer"},
                        "supervoxel_id": {"type": "integer"},
                    },
                    "type": "object",
                    "additionalProperties": False,
                },
                "NucleusDetection": {
                    "required": ["pt"],
                    "properties": {
                        "bb_end": {
                            "type": "object",
                            "$ref": "#/definitions/SpatialPoint",
                            "description": "high corner of the bounding box",
                        },
                        "bb_start": {
                            "type": "object",
                            "$ref": "#/definitions/SpatialPoint",
                            "description": "low corner of the bounding box",
                        },
                        "pt": {
                            "type": "object",
                            "$ref": "#/definitions/BoundSpatialPoint",
                            "description": "the centroid of the nucleus, to be linked to the segmentation",
                        },
                        "valid": {
                            "title": "valid",
                            "type": ["boolean", "null"],
                            "default": False,
                            "description": "is this annotation valid",
                        },
                        "volume": {
                            "title": "volume",
                            "type": "number",
                            "format": "float",
                            "description": "the volume of the nucleus detected in um^3",
                        },
                    },
                    "type": "object",
                    "additionalProperties": True,
                },
            },
            "$ref": "#/definitions/NucleusDetection",
        },
    }

    views_list = {
        "synapses_pni_2_in_out_degree": {
            "notice_text": None,
            "datastack_name": "minnie65_phase3_v1",
            "id": 1,
            "voxel_resolution_x": 4.0,
            "description": "This calculates the number of input and output synapses from individual segment IDs. ",
            "voxel_resolution_z": 40.0,
            "live_compatible": False,
            "voxel_resolution_y": 4.0,
        },
        "connections_with_nuclei": {
            "notice_text": None,
            "datastack_name": "minnie65_phase3_v1",
            "id": 2,
            "voxel_resolution_x": 4.0,
            "description": "This summarizes connections with number of synapses and summed synapse size, as well as reporting the nucleus id associated with the pt_root_id if there is precisely one in the nucleus_detection_v0 table. \r\n\r\nColumns are pre_pt_root_id, post_pt_root_id, n_syn, sum_size, pre_nuc_id, post_nuc_id.",
            "voxel_resolution_z": 40.0,
            "live_compatible": False,
            "voxel_resolution_y": 4.0,
        },
        "nucleus_detection_lookup_v1": {
            "notice_text": None,
            "datastack_name": "minnie65_phase3_v1",
            "id": 3,
            "voxel_resolution_x": 4.0,
            "description": "A table that merges the nucleus_detection_v0 table with the nucleus_alternative_points table to provide one root_id column which can be used to lookup segments associated with root_ids, while preserving the geometric center of the nucleus. ",
            "voxel_resolution_z": 40.0,
            "live_compatible": True,
            "voxel_resolution_y": 4.0,
        },
        "soma_counts": {
            "notice_text": None,
            "datastack_name": "minnie65_phase3_v1",
            "id": 4,
            "voxel_resolution_x": 4.0,
            "description": "This takes the nucleus_detection_lookup_v1 view and the nucleus_ref_neuron_svm table to measure how many nuclei there are per segment_id and how many of those are neurons. \r\n\r\nThe columns are \r\nid: the ID of the nucleus\r\npt_position: the centroid of the nucleus\r\npt_supervoxel_id: the supervoxel of the nucleus in case you want to update it\r\ncell_type: the cell type of the nucleus from the svm model.\r\nneuron_count: how many neurons are associated with this root id\r\ntotal_count: how many total nucleus detections are associated with this root_id",
            "voxel_resolution_z": 40.0,
            "live_compatible": False,
            "voxel_resolution_y": 4.0,
        },
        "single_neurons": {
            "notice_text": None,
            "datastack_name": "minnie65_phase3_v1",
            "id": 5,
            "voxel_resolution_x": 4.0,
            "description": 'This is a list of all the single neuron objects in the dataset, the id column is the nucleus_id, the root_id is its segment_id (which can change over versions), and then position is the position of the centroid of its nucleus. This is based on the soma_counts view, filtering for cell_type="neuron" and n_neuron=1. ',
            "voxel_resolution_z": 40.0,
            "live_compatible": False,
            "voxel_resolution_y": 4.0,
        },
    }

    views_schema = {
        "synapses_pni_2_in_out_degree": {
            "root_id": {"type": "integer"},
            "n_input": {"type": "integer"},
            "avg_input_size": {"type": "float"},
            "n_output": {"type": "integer"},
            "avg_output_size": {"type": "float"},
        },
        "connections_with_nuclei": {
            "pre_pt_root_id": {"type": "integer"},
            "post_pt_root_id": {"type": "integer"},
            "n_syn": {"type": "integer"},
            "sum_size": {"type": "float"},
            "pre_nuc_id": {"type": "integer"},
            "post_nuc_id": {"type": "integer"},
        },
        "nucleus_detection_lookup_v1": {
            "id": {"type": "integer"},
            "volume": {"type": "float"},
            "pt_position": {"type": "SpatialPoint"},
            "pt_root_id": {"type": "integer"},
            "orig_root_id": {"type": "integer"},
            "pt_supervoxel_id": {"type": "integer"},
            "pt_position_lookup": {"type": "SpatialPoint"},
        },
        "soma_counts": {
            "id": {"type": "integer"},
            "pt_position": {"type": "SpatialPoint"},
            "pt_root_id": {"type": "integer"},
            "pt_supervoxel_id": {"type": "integer"},
            "cell_type": {"type": "string"},
            "neuron_count": {"type": "integer"},
            "total_count": {"type": "integer"},
        },
        "single_neurons": {
            "id": {"type": "integer"},
            "pt_root_id": {"type": "integer"},
            "pt_position": {"type": "SpatialPoint"},
            "pt_supervoxel_id": {"type": "integer"},
        },
    }

    @responses.activate
    def test_matclient_v3_tableinterface(self, myclient, mocker):
        myclient = copy.deepcopy(myclient)
        endpoint_mapping = self.default_mapping
        endpoint_mapping["emas_server_address"] = TEST_GLOBAL_SERVER

        api_versions_url = chunkedgraph_endpoints_common["get_api_versions"].format_map(
            endpoint_mapping
        )
        responses.add(responses.GET, url=api_versions_url, json=[0, 1], status=200)
        responses.add(
            responses.GET,
            url=materialization_common["get_api_versions"].format_map(endpoint_mapping),
            json=[3],
            status=200,
        )

        versionurl = materialization_endpoints_v3["versions"].format_map(
            endpoint_mapping
        )
        responses.add(
            responses.GET,
            url=versionurl,
            json=[1],
            status=200,
            match=[query_param_matcher({"expired": False})],
        )

        all_tables_meta_url = materialization_endpoints_v3[
            "all_tables_metadata"
        ].format_map(endpoint_mapping)
        responses.add(
            responses.GET,
            url=all_tables_meta_url,
            json=self.multitable_meta,
            status=200,
        )

        all_schema_def_url = schema_endpoints_v2["schema_definition_all"].format_map(
            endpoint_mapping
        )
        responses.add(
            responses.GET,
            url=all_schema_def_url,
            json=self.multischema_meta,
            status=200,
        )

        get_views_url = materialization_endpoints_v3["get_views"].format_map(
            endpoint_mapping
        )
        responses.add(
            responses.GET, url=get_views_url, json=self.views_list, status=200
        )

        get_views_schema_url = materialization_endpoints_v3["view_schemas"].format_map(
            endpoint_mapping
        )
        print(get_views_schema_url)
        responses.add(
            responses.GET, url=get_views_schema_url, json=self.views_schema, status=200
        )

        assert len(myclient.materialize.tables) == 2
        qry = myclient.materialize.tables.allen_column_mtypes_v2(
            pt_root_id=[123, 456], target_id=271700
        )
        params = qry.filter_kwargs_live
        assert "allen_column_mtypes_v2" in params.get("filter_equal_dict")
        assert "nucleus_detection_v0" in params.get("filter_in_dict")
        assert "allen_column_mtypes_v2" == qry.joins_kwargs.get("joins")[0][0]

        assert "single_neurons" in myclient.materialize.views
        vqry = myclient.materialize.views.single_neurons(pt_root_id=[123, 456])
        assert 123 in vqry.filter_kwargs_mat.get("filter_in_dict").get("pt_root_id")

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

        query_d = {
            "return_pyarrow": True,
            "split_positions": True,
            "arrow_format": True,
        }
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

        responses.add(
            responses.POST,
            url=url,
            body=serialize_dataframe(df),
            content_type="data.arrow",
            match=[json_params_matcher(correct_query_data)],
        )

        responses.add(
            responses.POST,
            url=url,
            body=serialize_dataframe(df_pos),
            content_type="data.arrow",
            headers={
                "dataframe_resolution": "1, 1, 1",
            },
            match=[json_params_matcher(correct_query_data_with_desired_resolution)],
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
        assert isinstance(df, pd.DataFrame)
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
                raise ChunkedgraphTestException(
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
                raise ChunkedgraphTestException(("should not past_ids on svid =0"))
            id_map = {201: [100], 103: [103], 203: [101, 102]}
            return {
                "future_id_map": {},
                "past_id_map": {k: id_map[k] for k in root_ids},
            }

        def mock_is_latest_roots(self, root_ids, timestamp=None):
            if 0 in root_ids:
                raise ChunkedgraphTestException(
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

        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"pre_pt_root_id": [100, 103]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialize_dataframe(df),
            content_type="data.arrow",
            match=[json_params_matcher(correct_query_data)],
        )
        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [100, 101, 102]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            content_type="data.arrow",
            body=serialize_dataframe(df),
            match=[json_params_matcher(correct_query_data)],
        )
        correct_query_data = {
            "filter_in_dict": {
                test_info["synapse_table"]: {"post_pt_root_id": [101, 102]}
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialize_dataframe(df),
            content_type="data.arrow",
            match=[json_params_matcher(correct_query_data)],
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

        endpoint_mapping["table_name"] = "cell_types"
        url = self.endpoints["simple_query"].format_map(endpoint_mapping)
        query_d = {
            "return_pyarrow": True,
            "split_positions": True,
            "arrow_format": True,
        }
        query_string = urlencode(query_d)
        url = url + "?" + query_string

        meta_url = self.endpoints["metadata"].format_map(endpoint_mapping)
        responses.add(responses.GET, url=meta_url, json=self.table_metadata)

        correct_query_data = {}
        responses.add(
            responses.POST,
            url=url,
            body=serialize_dataframe(df_ct),
            content_type="data.arrow",
            match=[json_params_matcher(correct_query_data)],
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
            body=serialize_dataframe(df_ct),
            content_type="data.arrow",
            match=[json_params_matcher(correct_query_data)],
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
