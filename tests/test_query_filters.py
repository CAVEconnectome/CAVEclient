import numpy as np
import pytest

from caveclient.query import (
    ColumnHandle,
    Filter,
    FilterKind,
    FilterOp,
    InvalidFilterError,
    filters_to_payload,
    legal_ops,
)


def handle(name, kind, table=None):
    return ColumnHandle(name=name, kind=kind, table=table)


class TestLegalOps:
    def test_numeric_permits_inequalities(self):
        ops = legal_ops(FilterKind.NUMERIC)
        assert FilterOp.GREATER in ops
        assert FilterOp.LESS_EQUAL in ops
        assert FilterOp.REGEX not in ops

    def test_string_permits_regex_not_inequality(self):
        ops = legal_ops(FilterKind.STRING)
        assert FilterOp.REGEX in ops
        assert FilterOp.GREATER not in ops

    def test_id_is_equatable_only(self):
        assert legal_ops(FilterKind.ID) == frozenset(
            {FilterOp.IN, FilterOp.NOT_IN, FilterOp.EQUAL}
        )

    def test_position_is_spatial_only(self):
        assert legal_ops(FilterKind.POSITION) == frozenset({FilterOp.SPATIAL})


class TestFilterValidation:
    def test_illegal_op_for_kind_raises(self):
        with pytest.raises(InvalidFilterError, match="not valid for column"):
            Filter(handle("cell_type", FilterKind.STRING), FilterOp.GREATER, 5)

    def test_inequality_on_root_id_raises(self):
        # root_id is ID kind: equatable, never ordered
        with pytest.raises(InvalidFilterError):
            Filter(handle("pre_pt_root_id", FilterKind.ID), FilterOp.LESS, 10)

    def test_in_requires_sequence(self):
        with pytest.raises(InvalidFilterError, match="requires a sequence"):
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.IN, 5)

    def test_equal_rejects_sequence(self):
        with pytest.raises(InvalidFilterError, match="single value"):
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.EQUAL, [1, 2])

    def test_inequality_requires_number(self):
        with pytest.raises(InvalidFilterError, match="real-number"):
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.GREATER, "big")

    def test_bool_is_not_a_valid_numeric_bound(self):
        with pytest.raises(InvalidFilterError):
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.GREATER, True)

    def test_regex_requires_string(self):
        with pytest.raises(InvalidFilterError, match="string pattern"):
            Filter(handle("tag", FilterKind.STRING), FilterOp.REGEX, 5)

    def test_spatial_requires_2x3(self):
        with pytest.raises(InvalidFilterError, match="bounds"):
            Filter(
                handle("pt_position", FilterKind.POSITION), FilterOp.SPATIAL, [1, 2, 3]
            )

    def test_valid_filters_construct(self):
        Filter(handle("size", FilterKind.NUMERIC), FilterOp.GREATER, 5)
        Filter(handle("size", FilterKind.NUMERIC), FilterOp.IN, [1, 2, 3])
        Filter(handle("tag", FilterKind.STRING), FilterOp.REGEX, "^abc")
        Filter(handle("valid", FilterKind.BOOLEAN), FilterOp.EQUAL, True)
        Filter(
            handle("pt_position", FilterKind.POSITION),
            FilterOp.SPATIAL,
            [[0, 0, 0], [10, 10, 10]],
        )

    def test_numpy_sequence_accepted_for_in(self):
        Filter(
            handle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, np.array([1, 2, 3])
        )

    def test_spatial_bounds_sorted_per_axis(self):
        # corners given in any order are normalized to [[min...], [max...]]
        f = Filter(
            handle("pt_position", FilterKind.POSITION),
            FilterOp.SPATIAL,
            [[258012, 199706, 20263], [244493, 188736, 21510]],
        )
        assert f.value == [[244493, 188736, 20263], [258012, 199706, 21510]]


class TestSerialize:
    def test_each_op_maps_to_its_wire_key(self):
        cases = [
            (FilterOp.IN, [1], "filter_in_dict"),
            (FilterOp.NOT_IN, [1], "filter_notin_dict"),
            (FilterOp.EQUAL, 1, "filter_equal_dict"),
            (FilterOp.GREATER, 1, "filter_greater_dict"),
            (FilterOp.LESS, 1, "filter_less_dict"),
            (FilterOp.GREATER_EQUAL, 1, "filter_greater_equal_dict"),
            (FilterOp.LESS_EQUAL, 1, "filter_less_equal_dict"),
        ]
        for op, value, key in cases:
            f = Filter(handle("size", FilterKind.NUMERIC), op, value)
            payload = filters_to_payload([f], default_table="t")
            assert key in payload
            assert payload[key] == {
                "t": {"size": value if not isinstance(value, list) else [1]}
            }

    def test_out_serializes_as_filter_notin_dict(self):
        f = Filter(handle("size", FilterKind.NUMERIC), FilterOp.NOT_IN, [1, 2])
        payload = filters_to_payload([f], default_table="t")
        assert "filter_notin_dict" in payload
        assert "filter_out_dict" not in payload

    def test_default_table_used_when_handle_table_none(self):
        f = Filter(handle("size", FilterKind.NUMERIC), FilterOp.EQUAL, 3)
        payload = filters_to_payload([f], default_table="my_table")
        assert payload["filter_equal_dict"] == {"my_table": {"size": 3}}

    def test_explicit_table_overrides_default(self):
        f = Filter(handle("size", FilterKind.NUMERIC, table="other"), FilterOp.EQUAL, 3)
        payload = filters_to_payload([f], default_table="my_table")
        assert payload["filter_equal_dict"] == {"other": {"size": 3}}

    def test_multiple_filters_merge_by_table_and_op(self):
        filters = [
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.GREATER, 5),
            Filter(handle("score", FilterKind.NUMERIC), FilterOp.GREATER, 1),
            Filter(handle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, [7, 8]),
        ]
        payload = filters_to_payload(filters, default_table="t")
        assert payload["filter_greater_dict"] == {"t": {"size": 5, "score": 1}}
        assert payload["filter_in_dict"] == {"t": {"pre_pt_root_id": [7, 8]}}

    def test_in_value_normalized_to_list(self):
        f = Filter(
            handle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, np.array([7, 8])
        )
        payload = filters_to_payload([f], default_table="t")
        assert payload["filter_in_dict"]["t"]["pre_pt_root_id"] == [7, 8]
        assert isinstance(payload["filter_in_dict"]["t"]["pre_pt_root_id"], list)

    def test_spatial_normalized_to_list_of_lists(self):
        f = Filter(
            handle("pt_position", FilterKind.POSITION),
            FilterOp.SPATIAL,
            (np.array([0, 0, 0]), np.array([10, 10, 10])),
        )
        payload = filters_to_payload([f], default_table="t")
        bbox = payload["filter_spatial_dict"]["t"]["pt_position"]
        assert bbox == [[0, 0, 0], [10, 10, 10]]

    def test_conflicting_filters_same_column_op_raise(self):
        filters = [
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.EQUAL, 1),
            Filter(handle("size", FilterKind.NUMERIC), FilterOp.EQUAL, 2),
        ]
        with pytest.raises(ValueError, match="conflicting"):
            filters_to_payload(filters, default_table="t")

    def test_empty_filters_produce_empty_payload(self):
        assert filters_to_payload([], default_table="t") == {}
