import pandas as pd
import pytest

from caveclient.query import Join
from caveclient.query.merge import (
    _resolve_key,
    build_run_tree,
    graph_merge_query,
    plan_runs,
)
from caveclient.query.spec import InvalidQueryError


def df(**cols):
    return pd.DataFrame(cols)


def drive(order, joins, kinds, suffixes, frames, *, offset=None, limit=None):
    """Plan + merge with canned per-run frames, recording each run's pushdown."""
    runs, run_of = plan_runs(order, joins, kinds, suffixes)
    bfs, edges = build_run_tree(runs, run_of, joins, suffixes)
    calls = {}

    def run_executor(run, incoming):
        calls[run.tables[0]] = incoming
        return frames[run.tables[0]]

    out = graph_merge_query(runs, bfs, edges, run_executor, offset=offset, limit=limit)
    return out, calls


class TestPlanRuns:
    def test_views_split_cave_tables_into_separate_runs(self):
        order = ["A", "v", "B"]
        joins = [Join("A", "a", "v", "k"), Join("v", "k", "B", "b")]
        kinds = {"A": "table", "v": "view", "B": "table"}
        sfx = {"A": "_x", "v": "_y", "B": "_z"}
        runs, run_of = plan_runs(order, joins, kinds, sfx)
        assert [r.kind for r in runs] == ["cave", "view", "cave"]
        assert run_of["A"] != run_of["B"]  # the view between them breaks the run
        assert runs[run_of["v"]].kind == "view"

    def test_all_cave_star_is_one_run(self):
        order = ["A", "alpha", "beta"]
        joins = [Join("A", "ca", "alpha", "id"), Join("A", "cb", "beta", "id")]
        kinds = {"A": "table", "alpha": "table", "beta": "table"}
        sfx = {"A": "_x", "alpha": "_y", "beta": "_z"}
        runs, run_of = plan_runs(order, joins, kinds, sfx)
        assert len(runs) == 1 and runs[0].kind == "cave"
        assert set(runs[0].tables) == {"A", "alpha", "beta"}


class TestBuildRunTree:
    def test_disconnected_graph_refused(self):
        # B is joined to nothing -> its run is unreachable
        order = ["A", "v", "B"]
        joins = [Join("A", "a", "v", "k")]
        kinds = {"A": "table", "v": "view", "B": "table"}
        sfx = {"A": "_x", "v": "_y", "B": "_z"}
        runs, run_of = plan_runs(order, joins, kinds, sfx)
        with pytest.raises(InvalidQueryError, match="disconnected"):
            build_run_tree(runs, run_of, joins, sfx)

    def test_cyclic_graph_refused(self):
        # A, v1, v2 each pairwise joined -> 3 runs, 3 cross-edges -> a cycle
        order = ["A", "v1", "v2"]
        joins = [
            Join("A", "a", "v1", "k"),
            Join("A", "b", "v2", "m"),
            Join("v1", "p", "v2", "q"),
        ]
        kinds = {"A": "table", "v1": "view", "v2": "view"}
        sfx = {"A": "_x", "v1": "_y", "v2": "_z"}
        runs, run_of = plan_runs(order, joins, kinds, sfx)
        with pytest.raises(InvalidQueryError, match="cycle"):
            build_run_tree(runs, run_of, joins, sfx)


class TestGraphMergeQuery:
    def test_chain_table_view(self):
        order = ["A", "v"]
        joins = [Join("A", "k", "v", "k")]
        kinds = {"A": "table", "v": "view"}
        sfx = {"A": "_x", "v": "_y"}
        frames = {
            "A": df(k=[1, 2, 3], x=[10, 20, 30]),
            "v": df(k=[2, 3, 4], y=["b2", "b3", "b4"]),
        }
        out, calls = drive(order, joins, kinds, sfx, frames)
        assert calls["A"] is None
        assert calls["v"] == ("v", "k", [1, 2, 3])  # semi-join pushdown
        assert {"k_x", "k_y", "x", "y"} <= set(out.columns)  # `k` collides -> suffixed
        assert set(out["k_x"]) == {2, 3} and len(out) == 2

    def test_star_two_views_merge_into_cave_root(self):
        # A (cave) joined to v1 on a1 and to v2 on a2 -- a star with two views
        order = ["A", "v1", "v2"]
        joins = [Join("A", "a1", "v1", "k1"), Join("A", "a2", "v2", "k2")]
        kinds = {"A": "table", "v1": "view", "v2": "view"}
        sfx = {"A": "_x", "v1": "_y", "v2": "_z"}
        frames = {
            "A": df(a1=[1, 2, 3], a2=[10, 20, 30], xval=["a", "b", "c"]),
            "v1": df(k1=[2, 3, 9], v1val=["p", "q", "r"]),
            "v2": df(k2=[20, 30], v2val=["s", "t"]),
        }
        out, calls = drive(order, joins, kinds, sfx, frames)
        # each view is pushed the anchor's keys for its own edge
        assert calls["v1"] == ("v1", "k1", [1, 2, 3])
        assert calls["v2"] == ("v2", "k2", [10, 20, 30])
        # A.a1 in {2,3} (via v1) and A.a2 in {20,30} (via v2) -> rows a1=2,3
        assert sorted(out["a1"]) == [2, 3]
        assert len(out) == 2

    def test_empty_run_short_circuits(self):
        order = ["A", "v"]
        joins = [Join("A", "k", "v", "k")]
        kinds = {"A": "table", "v": "view"}
        sfx = {"A": "_x", "v": "_y"}
        frames = {"A": df(k=[], x=[]), "v": df(k=[1], y=["z"])}
        out, calls = drive(order, joins, kinds, sfx, frames)
        assert len(out) == 0
        assert "v" not in calls  # view never queried once the cave side is empty

    def test_offset_and_limit_on_merged_result(self):
        order = ["A", "v"]
        joins = [Join("A", "k", "v", "k")]
        kinds = {"A": "table", "v": "view"}
        sfx = {"A": "_x", "v": "_y"}
        frames = {
            "A": df(k=[1, 2, 3, 4], va=[1, 2, 3, 4]),
            "v": df(k=[1, 2, 3, 4], vb=[5, 6, 7, 8]),
        }
        out, _ = drive(order, joins, kinds, sfx, frames, offset=1, limit=2)
        assert out["k_x"].tolist() == [2, 3]


class TestResolveKey:
    def test_bare_suffixed_and_missing(self):
        assert _resolve_key(df(k=[1], k_x=[2]), "k", "_x") == "k"
        assert _resolve_key(df(k_x=[1]), "k", "_x") == "k_x"
        with pytest.raises(KeyError, match="not in the sub-query result"):
            _resolve_key(df(z=[1]), "k", "_x")
