"""Integration tests for the unified MaterializationClient.query() entry point.

These exercise the wiring (kwargs -> QuerySpec -> route -> delegate) by spying on
the delegated methods, rather than re-mocking the HTTP layer that the existing
query_table / live_live_query tests already cover.
"""

import datetime

from packaging.version import Version

from caveclient.query import (
    At,
    Capabilities,
    QuerySpec,
    Source,
    Table,
)

from .conftest import myclient  # noqa: F401

UTC = datetime.timezone.utc
NOW = datetime.datetime(2024, 6, 1, tzinfo=UTC)

MODERN = Capabilities(server_version=Version("5.20.0"), has_chunkedgraph=True)


def test_frozen_table_delegates_to_query_table(myclient, mocker):  # noqa: F811
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
    # delegated query_table still receives the _dict argument names
    assert kw["filter_in_dict"] == {"pre_pt_root_id": [500]}
    assert kw["filter_out_dict"] == {"post_pt_root_id": [501]}


def test_live_table_delegates_to_live_live_query(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(
        myclient.materialize, "_reference_join_for", return_value=(None, None)
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    out = myclient.materialize.query(
        "synapses",
        timestamp=NOW,
        kind="table",
        filter_in={"pre_pt_root_id": [500]},
    )
    assert out == "DF"
    _, kw = spy.call_args
    assert kw["timestamp"] == NOW
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
    myclient.materialize.query(
        "my_view", version=3, allow_version_fallback=False
    )  # kind="auto"
    spy.assert_called_once()


def test_auto_kind_defaults_to_table_when_views_unavailable(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "get_views", side_effect=RuntimeError("no v3")
    )
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        "synapses", version=3, allow_version_fallback=False
    )  # kind="auto"
    spy.assert_called_once()


def test_stale_version_falls_back_to_live(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(myclient.materialize, "get_versions", return_value=[943, 944])
    mocker.patch.object(myclient.materialize, "get_timestamp", return_value=NOW)
    mocker.patch.object(
        myclient.materialize, "_reference_join_for", return_value=(None, None)
    )
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
    try:
        myclient.materialize.query(spec, filter_in={"x": [1]})
    except ValueError as e:
        assert "QuerySpec" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_table_objects_join_delegates_to_join_query(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "join_query", return_value="DF")
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
    args, kwargs = spy.call_args
    assert args[0] == [["synapses", "post_pt_root_id"], ["nuclei", "pt_root_id"]]
    assert kwargs["suffixes"] == {"synapses": "", "nuclei": "_nuc"}
    assert kwargs["filter_greater_dict"] == {"synapses": {"size": 100}}


def test_single_table_object_delegates_to_query_table(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    out = myclient.materialize.query(
        Table("synapses", filter_greater={"size": 100}),
        version=3,
        allow_version_fallback=False,
    )
    assert out == "DF"
    _, kwargs = spy.call_args
    # single table -> flat filters, same as the string form
    assert kwargs["filter_greater_dict"] == {"size": 100}


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
    # both produce the same delegated filter kwargs
    assert calls[0][1]["filter_greater_dict"] == calls[1][1]["filter_greater_dict"]


def test_table_objects_plus_kwargs_is_rejected(myclient):  # noqa: F811
    try:
        myclient.materialize.query(
            [Table("a", "x"), Table("b", "y")], filter_in={"c": [1]}
        )
    except ValueError as e:
        assert "Table objects" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_live_flags_passed_through_to_live_query(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(
        myclient.materialize, "_reference_join_for", return_value=(None, None)
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
    # frozen path delegates to query_table, which has no such kwargs
    _, kwargs = spy.call_args
    assert "allow_missing_lookups" not in kwargs
    assert "allow_invalid_root_ids" not in kwargs


def test_merge_reference_default_true_passed_to_query_table(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        "synapses", version=3, kind="table", allow_version_fallback=False
    )
    assert spy.call_args.kwargs["merge_reference"] is True


def test_table_merge_reference_false_passed_through(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(myclient.materialize, "query_table", return_value="DF")
    myclient.materialize.query(
        Table("synapses", merge_reference=False),
        version=3,
        allow_version_fallback=False,
    )
    assert spy.call_args.kwargs["merge_reference"] is False


def test_live_merge_reference_injects_reference_join(myclient, mocker):  # noqa: F811
    # live_live_query doesn't auto-merge; the backend injects the reference join,
    # reusing the cached _resolve_merge_reference (mocked here to report a ref).
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    mocker.patch.object(
        myclient.materialize,
        "_resolve_merge_reference",
        return_value=(
            [["syn", "target_id"], ["nuc", "id"]],
            {"syn": "", "nuc": "_ref"},
        ),
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query("syn", timestamp=NOW, kind="table")
    _, kwargs = spy.call_args
    # mirrors the server's own reference-join pattern (quad encoding) + _ref suffix
    assert kwargs["joins"] == [["syn", "target_id", "nuc", "id"]]
    assert kwargs["suffixes"] == {"syn": "", "nuc": "_ref"}


def test_live_merge_reference_false_injects_nothing(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    resolve = mocker.patch.object(myclient.materialize, "_resolve_merge_reference")
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query(Table("syn", merge_reference=False), timestamp=NOW)
    assert spy.call_args.kwargs["joins"] is None
    resolve.assert_not_called()


def test_live_no_reference_table_no_join(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    # _resolve_merge_reference returns a single-table list when there's no reference
    mocker.patch.object(
        myclient.materialize, "_resolve_merge_reference", return_value=(["syn"], None)
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    myclient.materialize.query("syn", timestamp=NOW, kind="table")
    assert spy.call_args.kwargs["joins"] is None


def test_available_version_set_is_cached(myclient, mocker):  # noqa: F811
    spy = mocker.patch.object(
        myclient.materialize, "get_versions", return_value=[1, 2, 3]
    )
    a = myclient.materialize._available_version_set()
    b = myclient.materialize._available_version_set()
    assert a == b == frozenset({1, 2, 3})
    # second call served from the short-TTL cache, not a second round-trip
    assert spy.call_count == 1


def test_unknown_filter_kwarg_is_rejected(myclient):  # noqa: F811
    try:
        myclient.materialize.query(
            "synapses", kind="table", filter_bogus_dict={"x": [1]}
        )
    except TypeError as e:
        assert "filter_bogus_dict" in str(e)
    else:
        raise AssertionError("expected TypeError")
