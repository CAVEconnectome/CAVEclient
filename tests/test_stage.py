"""Unit tests for StagedAnnotations.

These exercise the staged-upload tracking and batching behavior directly,
without going through the HTTP layer. Anything that depends on the AnnotationClient
or its responses-mocked endpoints lives in `test_annotation.py`.
"""

import pandas as pd
import pytest

from caveclient.tools.stage import StagedAnnotations

test_schema = {
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
                "cell_type": {"type": "string"},
                "classification_system": {"type": "string"},
                "pt": {
                    "type": "object",
                    "$ref": "#/definitions/BoundSpatialPoint",
                },
            },
            "required": ["cell_type", "classification_system", "pt"],
            "additionalProperties": True,
        },
    },
    "$ref": "#/definitions/CellTypeLocal",
}


def _make_stage(update=False):
    return StagedAnnotations(test_schema, update=update, table_name="test_table")


class TestAnnotationBatches:
    def test_batches_skip_uploaded(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)

        batches = stage._annotation_batches(batch_size=10)
        assert len(batches) == 1
        assert batches[0] == [stage._anno_list[1]]

    def test_empty_stage_yields_no_batches(self):
        assert _make_stage()._annotation_batches(batch_size=10) == []

    def test_all_uploaded_yields_no_batches(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)
        assert stage._annotation_batches(batch_size=10) == []


class TestApplyUploadResult:
    def test_post_stamps_ids_in_batch_order(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])

        stage._apply_upload_result(list(stage._anno_list), [10, 11])

        a0, a1 = stage._anno_list
        assert getattr(a0, stage.IS_UPLOADED_FIELD) is True
        assert getattr(a0, stage.UPLOADED_ID_FIELD) == 10
        assert getattr(a1, stage.UPLOADED_ID_FIELD) == 11

    def test_post_asserts_on_length_mismatch(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])

        with pytest.raises(AssertionError):
            stage._apply_upload_result(list(stage._anno_list), [10])

    def test_update_maps_by_id_not_by_response_order(self):
        """The server's {old_id: new_id} dict is keyed-lookup, not zipped, so
        a reordered response must still produce the correct old→new mapping."""
        stage = _make_stage(update=True)
        stage.add(id=1000, cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(id=2000, cell_type="B", classification_system="Y", pt_position=[4, 5, 6])

        shuffled = {"2000": 2001, "1000": 1001}
        stage._apply_upload_result(list(stage._anno_list), shuffled)

        a0, a1 = stage._anno_list
        assert a0.id == 1000 and getattr(a0, stage.UPLOADED_ID_FIELD) == 1001
        assert a1.id == 2000 and getattr(a1, stage.UPLOADED_ID_FIELD) == 2001


class TestAnnotationDataframe:
    def test_empty_stage_does_not_raise(self):
        stage = _make_stage()
        assert stage.annotation_dataframe().empty
        assert stage.annotation_dataframe(include_tracking=True).empty

    def test_default_omits_tracking_and_flattens_positions(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])

        df = stage.annotation_dataframe()
        assert "pt_position" in df.columns
        assert "pt" not in df.columns
        assert df.loc[0, "pt_position"] == [1, 2, 3]
        assert "is_uploaded" not in df.columns
        assert "new_id" not in df.columns

    def test_include_tracking_renames_to_friendly_columns(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)
        setattr(stage._anno_list[0], stage.UPLOADED_ID_FIELD, 42)

        df = stage.annotation_dataframe(include_tracking=True)

        assert list(df["is_uploaded"]) == [True, False]
        assert df["new_id"].iloc[0] == 42
        assert pd.isna(df["new_id"].iloc[1])
        assert df["new_id"].dtype == "Int64"

    def test_update_dataframe_preserves_old_and_new_id(self):
        stage = _make_stage(update=True)
        stage.add(id=1000, cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)
        setattr(stage._anno_list[0], stage.UPLOADED_ID_FIELD, 1001)

        df = stage.annotation_dataframe(include_tracking=True)
        assert df.loc[0, "id"] == 1000
        assert df.loc[0, "new_id"] == 1001

    def test_only_nonuploaded_filters_rows(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)

        df = stage.annotation_dataframe(only_nonuploaded=True)
        assert len(df) == 1
        assert df.loc[0, "cell_type"] == "B"


class TestRepr:
    def test_repr_shows_uploaded_count(self):
        stage = _make_stage()
        stage.add(cell_type="A", classification_system="X", pt_position=[1, 2, 3])
        stage.add(cell_type="B", classification_system="Y", pt_position=[4, 5, 6])
        setattr(stage._anno_list[0], stage.IS_UPLOADED_FIELD, True)

        r = repr(stage)
        assert "2 new annotations" in r
        assert "1 uploaded" in r


def test_no_table_resolution_warns_does_not_raise():
    with pytest.warns(UserWarning):
        StagedAnnotations(test_schema, annotation_resolution=[1, 1, 1])
