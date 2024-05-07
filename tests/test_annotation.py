import json

import jsonschema
import numpy as np
import pytest
import responses

from caveclient.endpoints import annotation_endpoints_v2, schema_endpoints_v2

from .conftest import TEST_DATASTACK, TEST_GLOBAL_SERVER, TEST_LOCAL_SERVER, test_info

test_jsonschema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        "BoundSpatialPoint": {
            "type": "object",
            "properties": {
                "position": {"type": "array"},
                "root_id": {"type": "integer"},
                "supervoxel_id": {"type": "integer"},
            },
            "required": ["position"],
            "additionalProperties": False,
        },
        "CellTypeLocal": {
            "type": "object",
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
                "pt": {
                    "type": "object",
                    "$ref": "#/definitions/BoundSpatialPoint",
                    "description": "Location associated with classification",
                },
                "valid": {
                    "title": "valid",
                    "type": ["boolean", "null"],
                    "default": False,
                    "description": "is this annotation valid",
                },
            },
            "required": ["cell_type", "classification_system", "pt"],
            "additionalProperties": True,
        },
    },
    "$ref": "#/definitions/CellTypeLocal",
}


class TestAnnoClinet:
    default_mapping = {
        "ae_server_address": TEST_LOCAL_SERVER,
        "emas_server_address": TEST_GLOBAL_SERVER,
        "datastack_name": TEST_DATASTACK,
        "aligned_volume_name": test_info.get("aligned_volume").get("name"),
        "table_name": "cell_type_test",
    }
    ae_endpoints = annotation_endpoints_v2
    sch_endpoints = schema_endpoints_v2

    table_meta = {
        "flat_segmentation_source": None,
        "description": "Test table",
        "deleted": None,
        "created": "2022-01-12T20:36:00.492313",
        "table_name": default_mapping["table_name"],
        "id": 70,
        "reference_table": None,
        "user_id": "56",
        "valid": True,
        "schema_type": "cell_type_local",
        "voxel_resolution_x": 4.0,
        "voxel_resolution_y": 4.0,
        "voxel_resolution_z": 40.0,
    }

    @responses.activate
    def test_staged_post(self, myclient, mocker):
        endpoint_mapping = self.default_mapping

        metadata_url = self.ae_endpoints.get("table_info").format_map(endpoint_mapping)
        responses.add(responses.GET, url=metadata_url, json=self.table_meta)
        meta = myclient.annotation.get_table_metadata(
            self.default_mapping["table_name"]
        )
        assert meta["schema_type"] == self.table_meta.get("schema_type")

        endpoint_mapping["schema_type"] = self.table_meta.get("schema_type")
        schema_url = self.sch_endpoints.get("schema_definition").format_map(
            endpoint_mapping
        )
        responses.add(responses.GET, url=schema_url, json=test_jsonschema)

        new_stage = myclient.annotation.stage_annotations(
            self.default_mapping.get("table_name")
        )

        assert np.all(np.array(new_stage._table_resolution) == np.array([4, 4, 40]))

        good_annotation = {
            "cell_type": "BC",
            "classification_system": "Exc",
            "pt_position": [1, 2, 3],
        }
        new_stage.add(**good_annotation)

        with pytest.raises(jsonschema.ValidationError):
            bad_annotation = {
                "cell_type": "BC",
                "classification_system": "Exc",
                "pt_position": 32,
            }
            new_stage.add(**bad_annotation)

        assert len(new_stage) == 1

        anno_out = new_stage.annotation_list[0]
        assert np.all(
            np.array(anno_out.get("pt").get("position"))
            == np.array(good_annotation.get("pt_position"))
        )

        format_anno = {
            "cell_type": "BC",
            "classification_system": "Exc",
            "pt": {"position": [1, 2, 3]},
        }
        anno_data = {"annotations": [format_anno]}
        post_url = self.ae_endpoints.get("annotations").format_map(endpoint_mapping)
        responses.add(
            responses.POST,
            post_url,
            body=json.dumps([1]),
            status=200,
            headers={"content-type": "application/json"},
            match=[responses.matchers.json_params_matcher(anno_data)],
        )
        myclient.annotation.upload_staged_annotations(new_stage)

        update_stage = myclient.annotation.stage_annotations(
            self.default_mapping.get("table_name"),
            update=True,
        )
        with pytest.raises(Exception):
            update_stage.add(**good_annotation)

        good_update = {
            "id": 1000,
            "cell_type": "BC",
            "classification_system": "Exc",
            "pt_position": [1, 2, 3],
        }

        update_stage.add(**good_update)

        format_update = {
            "id": 1000,
            "cell_type": "BC",
            "classification_system": "Exc",
            "pt": {"position": [1, 2, 3]},
        }
        update_data = {"annotations": [format_update]}
        post_url = self.ae_endpoints.get("annotations").format_map(endpoint_mapping)
        responses.add(
            responses.PUT,
            post_url,
            body=json.dumps([{"1000": 1001}]),
            status=200,
            headers={"content-type": "application/json"},
            match=[responses.matchers.json_params_matcher(update_data)],
        )
        myclient.annotation.upload_staged_annotations(update_stage)

        schema_stage = myclient.annotation.stage_annotations(
            schema_name="cell_type_local"
        )
        schema_stage.add(**good_annotation)
        with pytest.raises(ValueError):
            myclient.annotation.upload_staged_annotations(schema_stage)
        schema_stage.table_name = self.default_mapping.get("table_name")
        myclient.annotation.upload_staged_annotations(schema_stage)
