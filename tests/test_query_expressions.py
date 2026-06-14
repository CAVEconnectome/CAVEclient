import numpy as np
import pytest

from caveclient.query import FilterKind, FilterOp, InvalidFilterError
from caveclient.query.expressions import Column, parse_filter_kwargs

KINDS = {
    "size": FilterKind.NUMERIC,
    "tag": FilterKind.STRING,
    "valid": FilterKind.BOOLEAN,
    "pre_pt_root_id": FilterKind.ID,
    "pt_position": FilterKind.POSITION,
}


class TestColumnHandle:
    def test_ordering_ops(self):
        c = Column("size", FilterKind.NUMERIC)
        assert (c > 5).op is FilterOp.GREATER
        assert (c < 5).op is FilterOp.LESS
        assert (c >= 5).op is FilterOp.GREATER_EQUAL
        assert (c <= 5).op is FilterOp.LESS_EQUAL

    def test_eq_scalar_is_equal_eq_sequence_is_in(self):
        c = Column("size", FilterKind.NUMERIC)
        assert (c == 5).op is FilterOp.EQUAL
        assert (c == [1, 2]).op is FilterOp.IN

    def test_ne_is_not_in(self):
        c = Column("size", FilterKind.NUMERIC)
        f = c != 5
        assert f.op is FilterOp.NOT_IN and f.value == [5]

    def test_methods(self):
        assert Column("r", FilterKind.ID).isin([1, 2]).op is FilterOp.IN
        assert Column("r", FilterKind.ID).notin([1]).op is FilterOp.NOT_IN
        assert Column("tag", FilterKind.STRING).regex("^x").op is FilterOp.REGEX
        assert (
            Column("p", FilterKind.POSITION).within([[0, 0, 0], [1, 1, 1]]).op
            is FilterOp.SPATIAL
        )

    def test_illegal_op_for_kind_raises_at_build(self):
        with pytest.raises(InvalidFilterError):
            Column("tag", FilterKind.STRING) > 5  # ordering on a string
        with pytest.raises(InvalidFilterError):
            Column("size", FilterKind.NUMERIC).regex("x")  # regex on numeric

    def test_handle_carries_table(self):
        f = Column("size", FilterKind.NUMERIC, table="syn") > 5
        assert f.column.table == "syn"


class TestParseFilterKwargs:
    def test_bare_scalar_is_equal_sequence_is_in(self):
        (f,) = parse_filter_kwargs(KINDS, {"size": 5})
        assert f.op is FilterOp.EQUAL
        (g,) = parse_filter_kwargs(KINDS, {"pre_pt_root_id": [1, 2]})
        assert g.op is FilterOp.IN

    def test_suffix_ops(self):
        cases = {
            "size__gt": FilterOp.GREATER,
            "size__lt": FilterOp.LESS,
            "size__gte": FilterOp.GREATER_EQUAL,
            "size__le": FilterOp.LESS_EQUAL,
            "pre_pt_root_id__not_in": FilterOp.NOT_IN,
            "tag__regex": FilterOp.REGEX,
            "pt_position__bbox": FilterOp.SPATIAL,
        }
        for key, op in cases.items():
            val = (
                [[0, 0, 0], [1, 1, 1]]
                if op is FilterOp.SPATIAL
                else (
                    [1]
                    if op in (FilterOp.NOT_IN,)
                    else ("^x" if op is FilterOp.REGEX else 5)
                )
            )
            (f,) = parse_filter_kwargs(KINDS, {key: val})
            assert f.op is op

    def test_unknown_operator_raises(self):
        with pytest.raises(TypeError, match="unknown filter operator"):
            parse_filter_kwargs(KINDS, {"size__bogus": 5})

    def test_unknown_column_raises(self):
        with pytest.raises(KeyError, match="not a filterable column"):
            parse_filter_kwargs(KINDS, {"nope__gt": 5}, table="t")

    def test_illegal_op_for_kind_raises(self):
        with pytest.raises(InvalidFilterError):
            parse_filter_kwargs(KINDS, {"tag__gt": 5})  # ordering on string

    def test_table_stamped_on_handles(self):
        (f,) = parse_filter_kwargs(KINDS, {"size__gt": 5}, table="syn")
        assert f.column.table == "syn"

    def test_numpy_sequence_is_in(self):
        (f,) = parse_filter_kwargs(KINDS, {"pre_pt_root_id": np.array([1, 2])})
        assert f.op is FilterOp.IN

    def test_legacy_wrapper_dicts(self):
        # back-compat with the original interface's {"<": v} style
        for wrapper, op in [
            ({"<": 100}, FilterOp.LESS),
            ({">": 100}, FilterOp.GREATER),
            ({"<=": 100}, FilterOp.LESS_EQUAL),
            ({">=": 100}, FilterOp.GREATER_EQUAL),
        ]:
            (f,) = parse_filter_kwargs(KINDS, {"size": wrapper})
            assert f.op is op and f.value == 100

    def test_legacy_bbox_column_name(self):
        # back-compat with the original interface's pt_position_bbox=[[...],[...]]
        (f,) = parse_filter_kwargs(KINDS, {"pt_position_bbox": [[0, 0, 0], [1, 1, 1]]})
        assert f.op is FilterOp.SPATIAL
        assert f.column.name == "pt_position"

    def test_column_tables_routes_owner(self):
        (f,) = parse_filter_kwargs(
            {"x": FilterKind.NUMERIC},
            {"x": 1},
            table="primary",
            column_tables={"x": "other"},
        )
        assert f.column.table == "other"
