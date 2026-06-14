import pandas as pd
import pytest

from caveclient.query import Table
from caveclient.query.merge import (
    _resolve_key,
    default_suffixes,
    local_merge_query,
    plan_segments,
)


def df(**cols):
    return pd.DataFrame(cols)


def run_with(tables, kinds, frames_by_first, *, offset=None, limit=None):
    """Drive the planner with canned per-segment frames, recording the
    extra_filter_in (semi-join pushdown) each segment was handed."""
    segs = plan_segments(tables, kinds, default_suffixes(tables))
    calls = {}

    def run_segment(seg, extra_filter_in):
        calls[seg.tables[0].name] = extra_filter_in
        return frames_by_first[seg.tables[0].name]

    out = local_merge_query(segs, run_segment, offset=offset, limit=limit)
    return out, calls


class TestDefaultSuffixes:
    def test_positional_and_explicit(self):
        tables = [Table("a"), Table("b", suffix="_nuc"), Table("c")]
        assert default_suffixes(tables) == {"a": "_x", "b": "_nuc", "c": "_z"}


class TestPlanSegments:
    def test_groups_consecutive_cave_and_isolates_views(self):
        tables = [
            Table("a", join_on="ka"),
            Table("b", join_on="kb"),
            Table("v", join_on="kv"),
            Table("c", join_on="kc"),
        ]
        kinds = ["table", "table", "view", "table"]
        segs = plan_segments(tables, kinds, default_suffixes(tables))
        assert [s.kind for s in segs] == ["cave", "view", "cave"]
        assert [tuple(t.name for t in s.tables) for s in segs] == [
            ("a", "b"),
            ("v",),
            ("c",),
        ]
        # boundary keys: first segment has no left, last has no right
        assert segs[0].left_key is None and segs[0].right_key == "kb"
        assert segs[1].left_key == "kv" and segs[1].right_key == "kv"
        assert segs[2].left_key == "kc" and segs[2].right_key is None

    def test_every_view_is_its_own_segment(self):
        tables = [Table("v1", join_on="k"), Table("v2", join_on="k")]
        segs = plan_segments(tables, ["view", "view"], default_suffixes(tables))
        assert [s.kind for s in segs] == ["view", "view"]
        assert [len(s.tables) for s in segs] == [1, 1]


class TestResolveKey:
    def test_bare_suffixed_none_and_missing(self):
        assert _resolve_key(df(k=[1], k_x=[2]), "k", "_x") == "k"  # bare wins
        assert _resolve_key(df(k_x=[1]), "k", "_x") == "k_x"  # server-suffixed
        assert _resolve_key(df(z=[1]), None, "_x") is None
        with pytest.raises(KeyError, match="not in the sub-query result"):
            _resolve_key(df(z=[1]), "k", "_x")


class TestLocalMergeQuery:
    def test_cave_view_inner_join_and_pushdown(self):
        tables = [Table("a", join_on="k"), Table("v", join_on="k")]
        frames = {
            "a": df(k=[1, 2, 3], x=[10, 20, 30]),
            "v": df(k=[2, 3, 4], y=["b2", "b3", "b4"]),
        }
        out, calls = run_with(tables, ["table", "view"], frames)
        assert calls["a"] is None  # driving segment, no pushdown
        assert calls["v"] == {"k": [1, 2, 3]}  # view restricted to the cave keys
        assert set(out["k_x"]) == {2, 3}
        # colliding `k` suffixed per segment; unique columns stay bare
        assert {"k_x", "k_y", "x", "y"} <= set(out.columns)
        assert len(out) == 2

    def test_grouped_cave_run_with_suffixed_boundary_key(self):
        # a 2-table CAVE run is one server join; its boundary key comes back
        # suffixed (kb -> kb_y), which the planner resolves by probing the frame
        tables = [
            Table("a", join_on="ka"),
            Table("b", join_on="kb"),
            Table("v", join_on="kv"),
        ]
        frames = {
            "a": df(kb_y=[1, 2], data=["r1", "r2"]),  # server-joined cave run result
            "v": df(kv=[2, 3], vy=["v2", "v3"]),
        }
        out, calls = run_with(tables, ["table", "table", "view"], frames)
        # one cave sub-query (a+b) and one view sub-query
        assert set(calls) == {"a", "v"}
        # the view is pushed the cave run's (suffixed) boundary key values
        assert calls["v"] == {"kv": [1, 2]}
        assert "kb_y" in out.columns
        assert len(out) == 1  # kb_y in {1,2} ∩ kv in {2,3} -> 2

    def test_three_segment_cave_view_cave(self):
        tables = [
            Table("a", join_on="k1"),
            Table("v", join_on="k2"),
            Table("c", join_on="k3"),
        ]
        frames = {
            "a": df(k1=[1, 2], x=["a1", "a2"]),
            "v": df(k2=[1, 2], y=["v1", "v2"]),
            "c": df(k3=[2, 3], z=["c2", "c3"]),
        }
        out, calls = run_with(tables, ["table", "view", "table"], frames)
        assert calls["v"] == {"k2": [1, 2]}
        assert calls["c"] == {"k3": [1, 2]}
        assert len(out) == 1 and out["z"].tolist() == ["c2"]

    def test_empty_driving_segment_short_circuits(self):
        tables = [Table("a", join_on="k"), Table("v", join_on="k")]
        frames = {"a": df(k=[], x=[]), "v": df(k=[1], y=["x"])}
        out, calls = run_with(tables, ["table", "view"], frames)
        assert len(out) == 0
        assert "v" not in calls  # view never queried once the cave side is empty

    def test_offset_and_limit_apply_to_merged_result(self):
        tables = [Table("a", join_on="k"), Table("v", join_on="k")]
        frames = {
            "a": df(k=[1, 2, 3, 4], va=[1, 2, 3, 4]),
            "v": df(k=[1, 2, 3, 4], vb=[5, 6, 7, 8]),
        }
        out, _ = run_with(tables, ["table", "view"], frames, offset=1, limit=2)
        assert out["k_x"].tolist() == [2, 3]
