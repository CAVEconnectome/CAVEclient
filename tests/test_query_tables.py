import pytest

from caveclient.query import InvalidQueryError, Table
from caveclient.query.serialize import (
    filters_to_method_kwargs,
    joins_to_quads,
)
from caveclient.query.spec import build_query_spec_from_tables


class TestTableDecomposition:
    def test_two_table_join_gathers_everything(self):
        spec = build_query_spec_from_tables(
            [
                Table(
                    "synapses",
                    "post_pt_root_id",
                    suffix="",
                    filter_greater={"size": 100},
                ),
                Table("nuclei", "pt_root_id", suffix="_nuc", filter_in={"ct": ["pyr"]}),
            ],
            version=943,
        )
        assert spec.source.name == "synapses"
        assert spec.source.is_join
        assert joins_to_quads(spec.source.joins) == [
            ["synapses", "post_pt_root_id", "nuclei", "pt_root_id"],
        ]
        assert spec.source.suffixes == {"synapses": "", "nuclei": "_nuc"}
        nested = filters_to_method_kwargs(spec.filters, spec.source.name, nested=True)
        assert nested["filter_greater_dict"] == {"synapses": {"size": 100}}
        assert nested["filter_in_dict"] == {"nuclei": {"ct": ["pyr"]}}
        assert spec.at.version == 943

    def test_single_table_has_no_join(self):
        spec = build_query_spec_from_tables(
            [Table("synapses", filter_greater={"size": 5})]
        )
        assert not spec.source.is_join
        flat = filters_to_method_kwargs(spec.filters, spec.source.name, nested=False)
        assert flat["filter_greater_dict"] == {"size": 5}

    def test_join_without_suffixes_defaults_to_pandas_style(self):
        # no explicit suffixes -> positional _x/_y (not the server's x/y)
        spec = build_query_spec_from_tables([Table("a", "id"), Table("b", "a_id")])
        assert spec.source.suffixes == {"a": "_x", "b": "_y"}

    def test_explicit_suffixes_win_over_defaults(self):
        spec = build_query_spec_from_tables(
            [Table("a", "id", suffix=""), Table("b", "a_id")]
        )
        # a keeps its explicit "", b falls back to the positional default
        assert spec.source.suffixes == {"a": "", "b": "_y"}

    def test_single_table_has_no_suffix(self):
        spec = build_query_spec_from_tables([Table("a")])
        assert not spec.source.suffixes

    def test_select_columns_gathered_per_table(self):
        spec = build_query_spec_from_tables(
            [
                Table("a", "id", select=["x", "y"]),
                Table("b", "a_id", select=["z"]),
            ]
        )
        assert spec.select_columns == {"a": ["x", "y"], "b": ["z"]}

    def test_multi_table_requires_join_on(self):
        with pytest.raises(InvalidQueryError, match="must set join_on"):
            build_query_spec_from_tables([Table("a", "x"), Table("b")])

    def test_empty_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="at least one"):
            build_query_spec_from_tables([])

    def test_filter_out_maps_to_not_in(self):
        spec = build_query_spec_from_tables(
            [Table("synapses", filter_out={"size": [1, 2]})]
        )
        flat = filters_to_method_kwargs(spec.filters, spec.source.name, nested=False)
        assert flat["filter_out_dict"] == {"size": [1, 2]}

    def test_three_table_chain(self):
        spec = build_query_spec_from_tables(
            [Table("a", "id"), Table("b", "a_id"), Table("c", "b_id")]
        )
        # adjacent pairing: a.id==b.a_id, b.a_id==c.b_id
        pairs = [
            [j.left_table, j.left_column, j.right_table, j.right_column]
            for j in spec.source.joins
        ]
        assert pairs == [
            ["a", "id", "b", "a_id"],
            ["b", "a_id", "c", "b_id"],
        ]
