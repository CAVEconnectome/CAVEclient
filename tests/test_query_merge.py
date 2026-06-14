import pandas as pd

from caveclient.query import Table
from caveclient.query.merge import default_suffixes, local_merge_query


def df(**cols):
    return pd.DataFrame(cols)


def recording_runner(frames_by_name):
    """A run_source that returns a canned frame per table and records the
    extra_filter_in it was handed (the semi-join pushdown)."""
    calls = {}

    def run_source(t, extra_filter_in):
        calls[t.name] = extra_filter_in
        return frames_by_name[t.name]

    return run_source, calls


class TestDefaultSuffixes:
    def test_positional_and_explicit(self):
        tables = [Table("a"), Table("b", suffix="_nuc"), Table("c")]
        sfx = default_suffixes(tables)
        assert sfx == {"a": "_x", "b": "_nuc", "c": "_z"}


class TestLocalMergeQuery:
    def test_inner_join_and_pushdown(self):
        tables = [Table("a", join_on="k"), Table("b", join_on="k")]
        frames = {
            "a": df(k=[1, 2, 3], val=["a1", "a2", "a3"]),
            "b": df(k=[2, 3, 4], val=["b2", "b3", "b4"]),
        }
        run, calls = recording_runner(frames)
        out = local_merge_query(tables, run)
        # the driving source gets no pushdown; b is restricted to a's keys
        assert calls["a"] is None
        assert calls["b"] == {"k": [1, 2, 3]}
        # inner join keeps only k in {2, 3}
        assert set(out["k_x"]) == {2, 3}
        # colliding columns are suffixed per source (_x / _y)
        assert {"k_x", "val_x", "k_y", "val_y"} <= set(out.columns)
        assert len(out) == 2

    def test_noncolliding_columns_keep_bare_names(self):
        tables = [Table("a", join_on="k"), Table("b", join_on="k")]
        frames = {
            "a": df(k=[1, 2], only_a=[10, 20]),
            "b": df(k=[2, 9], only_b=[30, 40]),
        }
        run, _ = recording_runner(frames)
        out = local_merge_query(tables, run)
        # only `k` collides -> suffixed; the unique columns stay bare
        assert "only_a" in out.columns and "only_b" in out.columns
        assert "k_x" in out.columns and "k_y" in out.columns
        assert out["only_a"].tolist() == [20] and out["only_b"].tolist() == [30]

    def test_empty_driving_source_short_circuits(self):
        tables = [Table("a", join_on="k"), Table("b", join_on="k")]
        frames = {"a": df(k=[], val=[]), "b": df(k=[1], val=["x"])}
        run, calls = recording_runner(frames)
        out = local_merge_query(tables, run)
        assert len(out) == 0
        assert "b" not in calls  # downstream never queried once a is empty

    def test_offset_and_limit_apply_to_merged_result(self):
        tables = [Table("a", join_on="k"), Table("b", join_on="k")]
        frames = {
            "a": df(k=[1, 2, 3, 4], va=[1, 2, 3, 4]),
            "b": df(k=[1, 2, 3, 4], vb=[5, 6, 7, 8]),
        }
        run, _ = recording_runner(frames)
        out = local_merge_query(tables, run, offset=1, limit=2)
        assert len(out) == 2
        assert out["k_x"].tolist() == [2, 3]

    def test_three_source_chain(self):
        # the Table model gives each table one join_on, used for both neighbors:
        # a.k1 == b.k2 links a-b, and b.k2 == c.k3 links b-c.
        tables = [
            Table("a", join_on="k1"),
            Table("b", join_on="k2"),
            Table("c", join_on="k3"),
        ]
        frames = {
            "a": df(k1=[1, 2], x=["a1", "a2"]),
            "b": df(k2=[1, 2], y=["b1", "b2"]),
            "c": df(k3=[2, 3], z=["c2", "c3"]),
        }
        run, calls = recording_runner(frames)
        out = local_merge_query(tables, run)
        # b restricted by a's join key, c by b's join key
        assert calls["b"] == {"k2": [1, 2]}
        assert calls["c"] == {"k3": [1, 2]}
        # a.k1 == b.k2 (1,2), then b.k2 == c.k3 (2) -> one surviving row
        assert len(out) == 1
        assert out["z"].tolist() == ["c2"]

    def test_existing_filter_in_intersects_with_pushdown(self):
        # when run_source folds the pushdown into an existing filter_in, the
        # planner just hands it the keys; intersection is the caller's job, but
        # the planner must pass the exact upstream key set.
        tables = [Table("a", join_on="k"), Table("b", join_on="k")]
        frames = {"a": df(k=[5, 6], v=[1, 2]), "b": df(k=[6, 7], v=[3, 4])}
        run, calls = recording_runner(frames)
        local_merge_query(tables, run)
        assert calls["b"] == {"k": [5, 6]}
