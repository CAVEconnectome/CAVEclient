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
        filter_in_dict={"pre_pt_root_id": [500]},
        filter_out_dict={"post_pt_root_id": [501]},
        limit=10,
        allow_version_fallback=False,
    )
    assert out == "DF"
    _, kw = spy.call_args
    assert spy.call_args.args[0] == "synapses"
    assert kw["materialization_version"] == 3
    assert kw["limit"] == 10
    # flat single-table filters, NOT_IN mapped to the filter_out_dict argument
    assert kw["filter_in_dict"] == {"pre_pt_root_id": [500]}
    assert kw["filter_out_dict"] == {"post_pt_root_id": [501]}


def test_live_table_delegates_to_live_live_query(myclient, mocker):  # noqa: F811
    mocker.patch.object(
        myclient.materialize, "_query_capabilities", return_value=MODERN
    )
    spy = mocker.patch.object(
        myclient.materialize, "live_live_query", return_value="DF"
    )
    out = myclient.materialize.query(
        "synapses",
        timestamp=NOW,
        kind="table",
        filter_in_dict={"pre_pt_root_id": [500]},
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
        myclient.materialize.query(spec, filter_in_dict={"x": [1]})
    except ValueError as e:
        assert "QuerySpec" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_unknown_filter_kwarg_is_rejected(myclient):  # noqa: F811
    try:
        myclient.materialize.query(
            "synapses", kind="table", filter_bogus_dict={"x": [1]}
        )
    except TypeError as e:
        assert "filter_bogus_dict" in str(e)
    else:
        raise AssertionError("expected TypeError")
