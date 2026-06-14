import datetime

import pytest

from caveclient.query import (
    At,
    ColumnHandle,
    Filter,
    FilterKind,
    FilterOp,
    InvalidQueryError,
    QuerySpec,
    Source,
)

UTC = datetime.timezone.utc


class TestAt:
    def test_version_and_timestamp_mutually_exclusive(self):
        with pytest.raises(InvalidQueryError, match="not both"):
            At(version=3, timestamp=datetime.datetime(2024, 1, 1, tzinfo=UTC))

    def test_is_live_true_for_timestamp(self):
        assert At(timestamp=datetime.datetime(2024, 1, 1, tzinfo=UTC)).is_live

    def test_is_live_false_for_version(self):
        assert not At(version=3).is_live

    def test_is_live_false_for_empty(self):
        assert not At().is_live


class TestSource:
    def test_is_join(self):
        assert Source("t", joins=[["a", "id", "b", "target_id"]]).is_join
        assert not Source("t").is_join


class TestQuerySpecValidation:
    def test_limit_must_be_positive(self):
        with pytest.raises(InvalidQueryError, match="limit must be"):
            QuerySpec(source=Source("t"), limit=0)

    def test_offset_must_be_nonnegative(self):
        with pytest.raises(InvalidQueryError, match="offset must be"):
            QuerySpec(source=Source("t"), offset=-1)

    def test_random_sample_must_be_positive(self):
        with pytest.raises(InvalidQueryError, match="random_sample must be"):
            QuerySpec(source=Source("t"), random_sample=0)

    def test_valid_spec_constructs(self):
        spec = QuerySpec(source=Source("t"), at=At(version=3), limit=100, offset=0)
        assert spec.limit == 100
        assert not spec.is_live


class TestFilterPayload:
    def test_filters_serialize_with_source_as_default_table(self):
        spec = QuerySpec(
            source=Source("synapses"),
            filters=(
                Filter(
                    ColumnHandle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, [1, 2]
                ),
                Filter(ColumnHandle("size", FilterKind.NUMERIC), FilterOp.GREATER, 100),
            ),
        )
        payload = spec.filter_payload()
        assert payload["filter_in_dict"] == {"synapses": {"pre_pt_root_id": [1, 2]}}
        assert payload["filter_greater_dict"] == {"synapses": {"size": 100}}


class TestSchemaValidation:
    def test_unknown_column_reported(self):
        spec = QuerySpec(
            source=Source("t"),
            filters=(
                Filter(ColumnHandle("nope", FilterKind.NUMERIC), FilterOp.EQUAL, 1),
            ),
        )
        problems = spec.validate_against_schema({"size": FilterKind.NUMERIC})
        assert any("not found" in p for p in problems)

    def test_kind_mismatch_reported(self):
        spec = QuerySpec(
            source=Source("t"),
            filters=(Filter(ColumnHandle("size", FilterKind.ID), FilterOp.EQUAL, 1),),
        )
        problems = spec.validate_against_schema({"size": FilterKind.NUMERIC})
        assert any("treats it as" in p for p in problems)

    def test_consistent_spec_has_no_problems(self):
        spec = QuerySpec(
            source=Source("t"),
            filters=(
                Filter(ColumnHandle("size", FilterKind.NUMERIC), FilterOp.GREATER, 1),
            ),
        )
        assert spec.validate_against_schema({"size": FilterKind.NUMERIC}) == []

    def test_joined_table_columns_skipped(self):
        # a filter on another table is not checked against the primary source
        spec = QuerySpec(
            source=Source("t"),
            filters=(
                Filter(
                    ColumnHandle("x", FilterKind.NUMERIC, table="other"),
                    FilterOp.EQUAL,
                    1,
                ),
            ),
        )
        assert spec.validate_against_schema({"size": FilterKind.NUMERIC}) == []


class TestBuildQuerySpecFromEdges:
    def test_star_graph(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        spec, first = build_query_spec_from_edges(
            [
                [Table("A", join_on="column_a"), Table("alpha", join_on="x")],
                [Table("A", join_on="column_b"), Table("beta", join_on="y")],
            ],
            version=1807,
        )
        # one pairwise Join per edge; A is the anchor and joins on different cols
        assert spec.source.name == "A"
        assert [(j.left_table, j.left_column, j.right_table, j.right_column)
                for j in spec.source.joins] == [
            ("A", "column_a", "alpha", "x"),
            ("A", "column_b", "beta", "y"),
        ]
        # suffixes per distinct table, positionally
        assert spec.source.suffixes == {"A": "_x", "alpha": "_y", "beta": "_z"}
        assert set(first) == {"A", "alpha", "beta"}

    def test_filters_on_first_appearance_route_by_table(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        spec, _ = build_query_spec_from_edges(
            [
                [Table("A", join_on="ca", filter_equal={"cell_type": "L2a"}),
                 Table("alpha", join_on="x")],
                [Table("A", join_on="cb"),
                 Table("beta", join_on="y", filter_greater={"size": 5})],
            ],
            version=1807,
        )
        routed = {(f.column.table, f.column.name, f.op) for f in spec.filters}
        assert ("A", "cell_type", FilterOp.EQUAL) in routed
        assert ("beta", "size", FilterOp.GREATER) in routed

    def test_filter_on_repeat_appearance_raises(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        with pytest.raises(InvalidQueryError, match="appears more than once"):
            build_query_spec_from_edges(
                [
                    [Table("A", join_on="ca"), Table("alpha", join_on="x")],
                    [Table("A", join_on="cb", filter_equal={"k": 1}),  # filter on repeat
                     Table("beta", join_on="y")],
                ]
            )

    def test_suffix_on_repeat_appearance_raises(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        with pytest.raises(InvalidQueryError, match="appears more than once"):
            build_query_spec_from_edges(
                [
                    [Table("A", join_on="ca"), Table("alpha", join_on="x")],
                    [Table("A", join_on="cb", suffix="_again"), Table("beta", join_on="y")],
                ]
            )

    def test_missing_join_on_raises(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        with pytest.raises(InvalidQueryError, match="must set join_on"):
            build_query_spec_from_edges([[Table("A"), Table("B", join_on="x")]])

    def test_bad_edge_shape_raises(self):
        from caveclient.query import Table
        from caveclient.query.spec import build_query_spec_from_edges

        with pytest.raises(InvalidQueryError, match="pair of Tables"):
            build_query_spec_from_edges([[Table("A", join_on="a")]])  # not a pair
        with pytest.raises(InvalidQueryError, match="Table objects"):
            build_query_spec_from_edges([["A", "B"]])  # strings, not Tables
