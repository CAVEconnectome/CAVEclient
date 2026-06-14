"""Live tests for the unified MaterializationClient.query() against a real server.

Not part of `poe test` (network + auth required). Run explicitly:

    uv run pytest live_tests/test_query_live.py -x -v

Strategy: for cases the legacy methods also express, assert query() returns the
same data; for the new behaviors (frozen-join-via-live, reference dedup, default
suffixes, stale-version fallback) assert correctness directly.
"""

import datetime

import pandas as pd
import pytest

from caveclient import CAVEclient
from caveclient.query import (
    At,
    ColumnHandle,
    Filter,
    FilterKind,
    FilterOp,
    QuerySpec,
    Source,
    Table,
)

CONFIG = {
    "datastack": "minnie65_phase3_v1",
    "version": 1807,
    "expired_version": 1805,
    "synapse_table": "synapses_pni_2",
    "root_col": "pre_pt_root_id",
    "size_col": "size",
    "position_col": "pre_pt_position",
    # two reference tables that share a base (nucleus_detection_v0)
    "ref_tables": ("allen_column_mtypes_v2", "allen_column_mtypes_v1"),
    "ref_base": "nucleus_detection_v0",
    "ref_join_col": "target_id",
    "ref_string_col": "cell_type",
    "view": "nucleus_detection_lookup_v1",
    "live_compatible_view": "nucleus_detection_lookup_v1",  # live_compatible=True
}
V = CONFIG["version"]


@pytest.fixture(scope="module")
def client():
    return CAVEclient(CONFIG["datastack"])


@pytest.fixture(scope="module")
def mat(client):
    return client.materialize


@pytest.fixture(scope="module")
def sample_root(mat):
    df = mat.query_table(
        CONFIG["synapse_table"],
        limit=5,
        materialization_version=V,
        select_columns=[CONFIG["root_col"]],
    )
    return int(df[CONFIG["root_col"]].iloc[0])


def ids(df):
    return set(df["id"].tolist())


# --------------------------------------------------------------------------
# Tier 1: query() must match the legacy methods (real round-trip of the wiring)
# --------------------------------------------------------------------------


def test_frozen_single_table_matches_query_table(mat, sample_root):
    a = mat.query(
        CONFIG["synapse_table"],
        version=V,
        filter_in={CONFIG["root_col"]: [sample_root]},
    )
    b = mat.query_table(
        CONFIG["synapse_table"],
        materialization_version=V,
        filter_in_dict={CONFIG["root_col"]: [sample_root]},
    )
    assert len(a) > 0
    assert ids(a) == ids(b)


def test_live_single_table_matches_live_live_query(mat, sample_root):
    ts = mat.get_timestamp(V)
    a = mat.query(
        CONFIG["synapse_table"],
        timestamp=ts,
        filter_in={CONFIG["root_col"]: [sample_root]},
    )
    b = mat.live_live_query(
        CONFIG["synapse_table"],
        timestamp=ts,
        filter_in_dict={CONFIG["synapse_table"]: {CONFIG["root_col"]: [sample_root]}},
    )
    assert ids(a) == ids(b)


# --------------------------------------------------------------------------
# Tier 2: the version->timestamp equivalence (the riskiest claim)
# --------------------------------------------------------------------------


def test_frozen_and_live_at_version_timestamp_agree(mat, sample_root):
    # query_table at V  vs  live_live_query at V's timestamp: same rows.
    frozen = mat.query(
        CONFIG["synapse_table"],
        version=V,
        filter_in={CONFIG["root_col"]: [sample_root]},
    )
    live = mat.query(
        CONFIG["synapse_table"],
        timestamp=mat.get_timestamp(V),
        filter_in={CONFIG["root_col"]: [sample_root]},
    )
    assert ids(frozen) == ids(live)


# --------------------------------------------------------------------------
# Tier 2: reference merge, dedup, joins, default suffixes
# --------------------------------------------------------------------------


def test_single_reference_table_frozen_uses_query_table(mat):
    # a reference table, frozen -> query_table merges its base (nucleus) itself
    a = mat.query(CONFIG["ref_tables"][0], version=V, limit=50)
    b = mat.query_table(
        CONFIG["ref_tables"][0],
        materialization_version=V,
        limit=50,
        merge_reference=True,
    )
    assert len(a) > 0
    assert ids(a) == ids(b)
    # the base table's id came along under a suffix
    assert (
        any(c.endswith("_ref") or "id_ref" in c for c in a.columns)
        or len(a.columns) > 3
    )


def test_shared_reference_merged_once(mat):
    v2, v1 = CONFIG["ref_tables"]
    df = mat.query(
        [
            Table(v2, CONFIG["ref_join_col"], suffix="_v2"),
            Table(v1, CONFIG["ref_join_col"], suffix="_v1"),
        ],
        version=V,
        limit=100,
    )
    assert len(df) > 0
    # the shared base (nucleus_detection_v0) is joined once: its id appears under
    # a single _ref suffix, not duplicated.
    ref_id_cols = [c for c in df.columns if c.endswith("_ref")]
    # cell_type from both reference tables, disambiguated by suffix
    assert any("_v2" in c for c in df.columns)
    assert any("_v1" in c for c in df.columns)
    # nucleus base columns appear once (no doubled "_ref" + "_ref_ref" style dup)
    assert len([c for c in ref_id_cols if c.count("_ref") > 1]) == 0


def test_join_default_suffixes_are_pandas_style(mat):
    v2, v1 = CONFIG["ref_tables"]
    df = mat.query(
        [Table(v2, CONFIG["ref_join_col"]), Table(v1, CONFIG["ref_join_col"])],
        version=V,
        limit=20,
    )
    # the shared cell_type column should come back _x / _y, never the server's x/y
    assert any(c.endswith("_x") for c in df.columns)
    assert any(c.endswith("_y") for c in df.columns)
    assert not any(c.endswith("typex") or c.endswith("typey") for c in df.columns)


# --------------------------------------------------------------------------
# Tier 2: views
# --------------------------------------------------------------------------


def test_frozen_view_query(mat):
    # auto-resolves kind=view -> ViewBackend -> query_view
    df = mat.query(CONFIG["view"], version=V, limit=5)
    assert len(df) > 0


def test_live_view_refused_in_phase1(mat):
    # the view advertises live_compatible=True, but Phase 1 doesn't read the flag
    # (caps.source_live_compatible is hardcoded False), so it refuses cleanly.
    from caveclient.query import UnroutableQueryError

    with pytest.raises(UnroutableQueryError):
        mat.query(
            CONFIG["live_compatible_view"], timestamp=mat.get_timestamp(V), limit=5
        )


# --------------------------------------------------------------------------
# Tier 2: stale-version fallback
# --------------------------------------------------------------------------


def test_stale_version_falls_back_to_live(mat, sample_root):
    df = mat.query(
        CONFIG["synapse_table"],
        version=CONFIG["expired_version"],
        filter_in={CONFIG["root_col"]: [sample_root]},
    )
    assert isinstance(df, pd.DataFrame)


# --------------------------------------------------------------------------
# Tier 3: filter ops + output options (smoke + equivalence where cheap)
# --------------------------------------------------------------------------


def test_filter_greater_matches_query_table(mat, sample_root):
    a = mat.query(
        CONFIG["synapse_table"],
        version=V,
        filter_in={CONFIG["root_col"]: [sample_root]},
        filter_greater={CONFIG["size_col"]: 1000},
    )
    b = mat.query_table(
        CONFIG["synapse_table"],
        materialization_version=V,
        filter_in_dict={CONFIG["root_col"]: [sample_root]},
        filter_greater_dict={CONFIG["size_col"]: 1000},
    )
    assert ids(a) == ids(b)


def test_regex_on_reference_string_column(mat):
    df = mat.query(
        CONFIG["ref_tables"][0],
        version=V,
        filter_regex={CONFIG["ref_string_col"]: "^.*"},
        limit=10,
    )
    assert isinstance(df, pd.DataFrame)


def test_output_options(mat, sample_root):
    df = mat.query(
        CONFIG["synapse_table"],
        version=V,
        filter_in={CONFIG["root_col"]: [sample_root]},
        split_positions=True,
        select_columns=[CONFIG["root_col"], CONFIG["position_col"]],
        limit=5,
    )
    assert len(df) <= 5
    # split_positions -> x/y/z columns for the position
    assert any(c.endswith("_x") for c in df.columns)


def test_get_counts(mat, sample_root):
    out = mat.query(
        CONFIG["synapse_table"],
        version=V,
        filter_in={CONFIG["root_col"]: [sample_root]},
        get_counts=True,
    )
    assert out is not None


def test_queryspec_direct(mat, sample_root):
    spec = QuerySpec(
        source=Source(CONFIG["synapse_table"], kind="table"),
        at=At(version=V),
        filters=(
            Filter(
                ColumnHandle(CONFIG["root_col"], FilterKind.ID),
                FilterOp.IN,
                [sample_root],
            ),
        ),
    )
    df = mat.query(spec)
    assert len(df) > 0


# --------------------------------------------------------------------------
# Tier 4: clean refusals / validation
# --------------------------------------------------------------------------


def test_version_and_timestamp_is_rejected(mat):
    with pytest.raises(Exception):
        mat.query(
            CONFIG["synapse_table"],
            version=V,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )


def test_bad_filter_value_rejected_client_side(mat):
    from caveclient.query import InvalidFilterError

    with pytest.raises(InvalidFilterError):
        # regex on a column the spec treats as numeric, or a non-numeric inequality
        Filter(ColumnHandle("size", FilterKind.NUMERIC), FilterOp.REGEX, "x")
