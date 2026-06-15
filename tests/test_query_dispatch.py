"""Integration tests for the unified MaterializationClient.query() entry point.

These exercise the wiring (kwargs -> QuerySpec -> route -> delegate) by spying on
the delegated methods, rather than re-mocking the HTTP layer that the existing
query_table / live_live_query tests already cover.

Only two server methods are ever delegated to: query_table (single-table frozen)
and live_live_query (everything live or joined). join_query and live_query are
not used.
"""

import datetime

import pandas as pd
import pytest
from packaging.version import Version

from caveclient.query import (
    At,
    Capabilities,
    InvalidQueryError,
    QuerySpec,
    Source,
    Table,
)

from .conftest import myclient  # noqa: F401

UTC = datetime.timezone.utc
NOW = datetime.datetime(2024, 6, 1, tzinfo=UTC)

MODERN = Capabilities(server_version=Version("5.20.0"), has_chunkedgraph=True)


@pytest.fixture(autouse=True)
def _no_reference_by_default(myclient, mocker):  # noqa: F811
    # Default: tables have no reference table, so merge_reference is a no-op.
    # Reference-specific tests override this.
    mocker.patch.object(
        myclient.materialize,
        "_resolve_merge_reference",
        side_effect=lambda mr, table, ds=None, v=None: ([table], None),
    )


def _reference(ref_table, suffix="_ref"):
    """side_effect: report `ref_table` as the reference for any annotation table."""

    def resolve(mr, table, ds=None, v=None):
        if table == ref_table:
            return ([table], None)
        return (
            [[table, "target_id"], [ref_table, "id"]],
            {table: "", ref_table: suffix},
        )

    return resolve


# ---------------------------------------------------------------------------
# Routing to the right delegated method
# ---------------------------------------------------------------------------


def test_frozen_single_table_delegates_to_query_table(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    out = myclient.materialize.query(
        "synapses",
        version=3,
        kind="table",
        filter_in={"pre_pt_root_id": [500]},
        filter_out={"post_pt_root_id": [501]},
        limit=10,
        allow_version_fallback=False,
    )
    assert out == "DF"
    _, kw = spy.call_args
    assert spy.call_args.args[0] == "synapses"
    assert kw["materialization_version"] == 3
    assert kw["limit"] == 10
    # delegated query_table receives the _dict argument names
    assert kw["filter_in_dict"] == {"pre_pt_root_id": [500]}
    assert kw["filter_out_dict"] == {"post_pt_root_id": [501]}
    # query_table merges the reference itself for single-table frozen queries
    assert kw["merge_reference"] is True


def test_string_source_merge_reference_false(myclient, mocker):  # noqa: F811
    # merge_reference is settable on the basic string-source query()
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        "synapses",
        version=3,
        kind="table",
        merge_reference=False,
        allow_version_fallback=False,
    )
    assert spy.call_args.kwargs["merge_reference"] is False


def test_live_table_delegates_to_live_live_query(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    out = myclient.materialize.query(
        "synapses", timestamp=NOW, kind="table", filter_in={"pre_pt_root_id": [500]}
    )
    assert out == "DF"
    _, kw = spy.call_args
    assert kw["timestamp"] == NOW
    assert kw["joins"] is None
    # nested {table: {col: val}} for the live endpoint
    assert kw["filter_in_dict"] == {"synapses": {"pre_pt_root_id": [500]}}


def test_view_delegates_to_query_view(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_view", return_value="DF")
    out = myclient.materialize.query(
        "my_view", version=3, kind="view", allow_version_fallback=False
    )
    assert out == "DF"
    assert spy.call_args.args[0] == "my_view"


def test_auto_kind_resolves_view_via_get_views(myclient, mocker):  # noqa: F811
    mocker.patch.object(myclient.materialize, "get_views", return_value=["my_view"])
    spy = mocker.patch.object(myclient.materialize, "query_view", return_value="DF")
    myclient.materialize.query("my_view", version=3, allow_version_fallback=False)
    spy.assert_called_once()


def test_single_table_spec_auto_resolves_view(myclient, mocker):  # noqa: F811
    # A lone Table with the default kind="table" pointing at a view name must be
    # resolved the same way a bare string / a Table in a join is -- otherwise it
    # would mis-route to query_table. kind="view" is therefore optional here too.
    mocker.patch.object(myclient.materialize, "get_views", return_value=["my_view"])
    spy = mocker.patch.object(myclient.materialize, "query_view", return_value="DF")
    out = myclient.materialize.query(
        Table("my_view"), version=3, allow_version_fallback=False
    )
    assert out == "DF"
    spy.assert_called_once()
    assert spy.call_args.args[0] == "my_view"


def test_single_table_spec_resolved_view_skips_reference_merge(myclient, mocker):  # noqa: F811
    # A view resolved from a Table spec must not be put on the reference-merge
    # path (views have no reference table) -- mirrors the string-source guard.
    mocker.patch.object(myclient.materialize, "get_views", return_value=["my_view"])
    spy = mocker.patch.object(myclient.materialize, "query_view", return_value="DF")
    myclient.materialize.query(
        Table("my_view", merge_reference=True), version=3, allow_version_fallback=False
    )
    spy.assert_called_once()


def test_auto_kind_defaults_to_table_when_views_unavailable(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "get_views", side_effect=RuntimeError("no v3")
    )
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query("synapses", version=3, allow_version_fallback=False)
    spy.assert_called_once()


def test_stale_version_falls_back_to_live(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_versions", return_value=[943, 944])
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)
    live = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    frozen = mocker.patch.object(myclient.materialize, "query_table")
    out = myclient.materialize.query("synapses", version=900, kind="table")
    assert out == "DF"
    live.assert_called_once()
    frozen.assert_not_called()
    assert live.call_args.kwargs["timestamp"] == NOW


def test_passing_a_queryspec_dispatches_it(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    spec = QuerySpec(source=Source("synapses", kind="table"), at=At(version=2))
    out = myclient.materialize.query(spec, allow_version_fallback=False)
    assert out == "DF"
    assert spy.call_args.kwargs["materialization_version"] == 2


def test_queryspec_plus_kwargs_is_rejected(myclient):  # noqa: F811
    spec = QuerySpec(source=Source("synapses", kind="table"), at=At(version=2))
    with pytest.raises(ValueError, match="QuerySpec"):
        myclient.materialize.query(spec, filter_in={"x": [1]})


# ---------------------------------------------------------------------------
# Joins (always via live_live_query; frozen joins run at the version timestamp)
# ---------------------------------------------------------------------------


def test_table_join_runs_live_at_version_timestamp(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    out = myclient.materialize.query(
        [
            Table(
                "synapses", "post_pt_root_id", suffix="", filter_greater={"size": 100}
            ),
            Table("nuclei", "pt_root_id", suffix="_nuc"),
        ],
        version=3,
        allow_version_fallback=False,
    )
    assert out == "DF"
    _, kwargs = spy.call_args
    # frozen join converted to a live query at the version's timestamp
    assert kwargs["timestamp"] == NOW
    assert kwargs["joins"] == [["synapses", "post_pt_root_id", "nuclei", "pt_root_id"]]
    assert kwargs["suffixes"] == {"synapses": "", "nuclei": "_nuc"}
    assert kwargs["filter_greater_dict"] == {"synapses": {"size": 100}}


def test_edge_list_star_join_via_live(myclient, mocker):  # noqa: F811
    # a list of [left, right] edges expresses a graph: table_a joined to alpha on
    # column_a AND to beta on column_b (a star the flat chain can't express)
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    out = myclient.materialize.query(
        [
            [
                Table("table_a", "column_a", filter_equal={"cell_type": "L2a"}),
                Table("alpha", "id"),
            ],
            [Table("table_a", "column_b"), Table("beta", "id")],
        ],
        version=3,
        allow_version_fallback=False,
    )
    assert out == "DF"
    _, kwargs = spy.call_args
    assert kwargs["joins"] == [
        ["table_a", "column_a", "alpha", "id"],
        ["table_a", "column_b", "beta", "id"],
    ]
    # filter stated on table_a's first appearance is routed to table_a
    assert kwargs["filter_equal_dict"] == {"table_a": {"cell_type": "L2a"}}


def test_flat_pair_equals_single_edge(myclient, mocker):  # noqa: F811
    # a flat pair [A, B] is sugar for the single-edge graph [[A, B]]
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query(
        [Table("a", "x"), Table("b", "y")], version=3, allow_version_fallback=False
    )
    flat_joins = spy.call_args.kwargs["joins"]
    spy.reset_mock()
    myclient.materialize.query(
        [[Table("a", "x"), Table("b", "y")]], version=3, allow_version_fallback=False
    )
    edge_joins = spy.call_args.kwargs["joins"]
    assert flat_joins == edge_joins == [["a", "x", "b", "y"]]


def test_short_string_is_single_source_not_a_join(myclient, mocker):  # noqa: F811
    # a str is not a list/tuple, so a (short) string name never gets split into
    # characters and mistaken for a flat pair of tables
    mocker.patch.object(myclient.materialize, "get_views", return_value=[])
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query("xy", version=3, allow_version_fallback=False)
    assert spy.call_args.args[0] == "xy"


def test_non_table_list_is_refused_clearly(myclient):  # noqa: F811
    # a list that's neither a Table pair nor Table edges fails legibly
    for bad in (["a", "b"], [Table("a", "x"), "b"], []):
        with pytest.raises(ValueError, match="flat pair of Table|Table edges"):
            myclient.materialize.query(bad, version=3)


def test_edge_list_of_non_tables_is_refused_clearly(myclient):  # noqa: F811
    # a list-of-lists is an edge list; its contents must be Tables
    with pytest.raises(ValueError, match="Table objects"):
        myclient.materialize.query([["a", "b"]], version=3)


def test_accessor_filter_kwargs_conversion():
    from caveclient.materializationengine import MaterializationClient as MC

    akw, cols = MC._accessor_filter_kwargs(
        {
            "filter_in_dict": {"cell_type": ["A"]},
            "filter_spatial_dict": {"pt_position": [[0, 0, 0], [1, 1, 1]]},
            "filter_greater_dict": {"size": 5},
        },
        "tbl",
    )
    assert akw == {
        "cell_type__in": ["A"],
        "pt_position__bbox": [[0, 0, 0], [1, 1, 1]],
        "size__gt": 5,
    }
    assert cols == {"cell_type", "pt_position", "size"}


def test_accessor_filter_kwargs_unwraps_source_nesting():
    from caveclient.materializationengine import MaterializationClient as MC

    akw, _ = MC._accessor_filter_kwargs({"filter_in_dict": {"tbl": {"x": [1]}}}, "tbl")
    assert akw == {"x__in": [1]}


def test_accessor_filter_kwargs_bails_on_foreign_nesting():
    from caveclient.materializationengine import MaterializationClient as MC

    akw, cols = MC._accessor_filter_kwargs(
        {"filter_in_dict": {"some_other_table": {"x": [1]}}}, "tbl"
    )
    assert akw is None and cols is None


def test_invalid_kind_is_refused(myclient):  # noqa: F811
    # kind is limited to auto/table/view; a stray value fails clearly rather than
    # leaking the not-yet-real dataset/deltalake path
    with pytest.raises(ValueError, match="kind must be 'auto', 'table', or 'view'"):
        myclient.materialize.query("syn", version=3, kind="dataset")


def test_flat_three_tables_is_refused(myclient):  # noqa: F811
    # no implicit chain: a flat list past two tables points at the edge-list form
    with pytest.raises(ValueError, match="single two-table join"):
        myclient.materialize.query(
            [Table("a", "x"), Table("b", "y"), Table("c", "z")], version=3
        )


def test_edge_list_with_view_does_local_merge(myclient, mocker):  # noqa: F811
    # a view in an edge graph is now split off and merged locally (no longer
    # refused): the CAVE side runs as a server query, the view as query_view
    mocker.patch.object(myclient.materialize, "get_views", return_value=["v"])
    mocker.patch.object(
        myclient.materialize,
        "query_table",
        return_value=pd.DataFrame({"k": [1, 2], "xa": [10, 20]}),
    )
    mocker.patch.object(
        myclient.materialize,
        "query_view",
        return_value=pd.DataFrame({"k": [2, 3], "xv": ["p", "q"]}),
    )
    out = myclient.materialize.query(
        [[Table("A", "k"), Table("v", "k")]],
        version=3,
        allow_version_fallback=False,
    )
    # A (query_table) inner-joined to v (query_view) on k -> only k == 2
    assert set(out["k_x"]) == {2}
    assert {"k_x", "k_y", "xa", "xv"} <= set(out.columns)


def test_cyclic_edge_graph_with_view_refused(myclient, mocker):  # noqa: F811
    # A, v1, v2 pairwise joined -> a cycle across engine runs -> refused
    mocker.patch.object(myclient.materialize, "get_views", return_value=["v1", "v2"])
    with pytest.raises(InvalidQueryError, match="cycle"):
        myclient.materialize.query(
            [
                [Table("A", "a"), Table("v1", "k")],
                [Table("A", "b"), Table("v2", "m")],
                [Table("v1", "p"), Table("v2", "q")],
            ],
            version=3,
            allow_version_fallback=False,
        )


def test_single_table_object_delegates_to_query_table(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    out = myclient.materialize.query(
        Table("synapses", filter_greater={"size": 100}),
        version=3,
        allow_version_fallback=False,
    )
    assert out == "DF"
    assert spy.call_args.kwargs["filter_greater_dict"] == {"size": 100}


def test_string_and_table_forms_are_equivalent_for_single_table(myclient, mocker):  # noqa: F811
    calls = []
    mocker.patch.object(
        myclient.materialize,
        "query_table",
        side_effect=lambda *a, **k: calls.append((a, k)) or "DF",
    )
    myclient.materialize.query(
        "synapses",
        version=3,
        filter_greater={"size": 100},
        allow_version_fallback=False,
    )
    myclient.materialize.query(
        Table("synapses", filter_greater={"size": 100}),
        version=3,
        allow_version_fallback=False,
    )
    assert calls[0][1]["filter_greater_dict"] == calls[1][1]["filter_greater_dict"]


def test_table_objects_plus_kwargs_is_rejected(myclient):  # noqa: F811
    with pytest.raises(ValueError, match="Table objects"):
        myclient.materialize.query(
            [Table("a", "x"), Table("b", "y")], filter_in={"c": [1]}
        )


# ---------------------------------------------------------------------------
# Live-only behavior flags
# ---------------------------------------------------------------------------


def test_live_flags_passed_through(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query(
        "synapses",
        timestamp=NOW,
        kind="table",
        allow_missing_lookups=True,
        allow_invalid_root_ids=True,
    )
    _, kwargs = spy.call_args
    assert kwargs["allow_missing_lookups"] is True
    assert kwargs["allow_invalid_root_ids"] is True


def test_live_flags_are_noops_for_frozen_query(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        "synapses",
        version=3,
        kind="table",
        allow_missing_lookups=True,
        allow_invalid_root_ids=True,
        allow_version_fallback=False,
    )
    _, kwargs = spy.call_args
    assert "allow_missing_lookups" not in kwargs
    assert "allow_invalid_root_ids" not in kwargs


# ---------------------------------------------------------------------------
# merge_reference: resolved into deduped reference joins (never via join_query)
# ---------------------------------------------------------------------------


def test_single_table_frozen_reference_uses_query_table(myclient, mocker):  # noqa: F811
    # single table + its reference, frozen -> query_table merges it itself
    # (cheap, no chunkedgraph, no live endpoint)
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        "syn", version=3, kind="table", allow_version_fallback=False
    )
    assert spy.call_args.kwargs["merge_reference"] is True


def test_live_reference_resolves_into_a_live_join(myclient, mocker):  # noqa: F811
    # for a LIVE query, live_live_query can't auto-merge, so the reference is
    # injected as an explicit join
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(
        myclient.materialize,
        "_resolve_merge_reference",
        side_effect=_reference("nuc"),
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query("syn", timestamp=NOW, kind="table")
    _, kwargs = spy.call_args
    assert kwargs["timestamp"] == NOW
    assert kwargs["joins"] == [["syn", "target_id", "nuc", "id"]]
    assert kwargs["suffixes"] == {"nuc": "_ref"}


def test_shared_reference_is_merged_once(myclient, mocker):  # noqa: F811
    # Two reference tables joined together, both referencing the same base table:
    # the shared reference must be merged exactly once.
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)

    def resolve(mr, table, ds=None, v=None):
        if table in ("mtypes_v1", "mtypes_v2"):
            return (
                [[table, "target_id"], ["nucleus_detection_v0", "id"]],
                {table: "", "nucleus_detection_v0": "_ref"},
            )
        return ([table], None)

    mocker.patch.object(
        myclient.materialize, "_resolve_merge_reference", side_effect=resolve
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query(
        [
            Table(
                "mtypes_v2",
                "target_id",
                suffix="_v2",
                filter_equal={"cell_type": "L2a"},
            ),
            Table("mtypes_v1", "target_id", suffix="_v1"),
        ],
        version=943,
        allow_version_fallback=False,
    )
    joins = spy.call_args.kwargs["joins"]
    assert joins == [
        ["mtypes_v2", "target_id", "mtypes_v1", "target_id"],  # explicit join
        ["mtypes_v2", "target_id", "nucleus_detection_v0", "id"],  # one ref, deduped
    ]
    assert sum(1 for j in joins if j[2] == "nucleus_detection_v0") == 1


def test_merge_reference_false_skips_resolution(myclient, mocker):  # noqa: F811
    resolve = mocker.patch.object(
        myclient.materialize,
        "_resolve_merge_reference",
        side_effect=lambda *a, **k: ([a[1]], None),
    )
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        Table("synapses", merge_reference=False),
        version=3,
        allow_version_fallback=False,
    )
    # no reference resolution attempted, stays a single-table query_table call
    resolve.assert_not_called()
    spy.assert_called_once()


# ---------------------------------------------------------------------------
# misc
# ---------------------------------------------------------------------------


def test_available_version_set_is_cached(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(
        myclient.materialize, "get_versions", return_value=[1, 2, 3]
    )
    a = myclient.materialize._available_version_set()
    b = myclient.materialize._available_version_set()
    assert a == b == frozenset({1, 2, 3})
    assert spy.call_count == 1


def test_unknown_filter_kwarg_is_rejected(myclient):  # noqa: F811
    with pytest.raises(TypeError, match="filter_bogus"):
        myclient.materialize.query("synapses", kind="table", filter_bogus={"x": [1]})
