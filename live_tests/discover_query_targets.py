"""Introspect a datastack to find concrete targets for the live query tests.

Read-only: lists versions, tables (and their reference relationships), views, and
samples one annotation table to identify root-id / position / string columns and
a real root id. Prints a config block to paste back.

Usage:
    uv run python live_tests/discover_query_targets.py <datastack_name>
    # optionally: ... <datastack_name> <server_address>
"""

import sys
from collections import defaultdict

from caveclient import CAVEclient


def main(datastack, server_address=None):
    kwargs = {"server_address": server_address} if server_address else {}
    client = CAVEclient(datastack, **kwargs)
    mat = client.materialize

    print(f"\n=== datastack: {datastack} (mat server {mat.server_version}) ===\n")

    # --- versions -----------------------------------------------------------
    valid = sorted(mat.get_versions(expired=False))
    allv = sorted(mat.get_versions(expired=True))
    expired = sorted(set(allv) - set(valid))
    print(f"valid versions   : {valid}")
    print(f"expired versions : {expired}  (for the stale-version fallback test)")
    latest = valid[-1] if valid else None

    # --- tables + reference relationships -----------------------------------
    meta = mat.get_tables_metadata(version=latest)
    if isinstance(meta, dict):
        meta = list(meta.values())
    refs = defaultdict(list)  # base_table -> [reference tables pointing at it]
    plain, schema_of = [], {}
    for m in meta:
        name = m.get("table_name") or m.get("table")
        schema_of[name] = m.get("schema_type") or m.get("schema")
        ref = m.get("reference_table")
        if ref:
            refs[ref].append(name)
        else:
            plain.append(name)

    shared = {base: r for base, r in refs.items() if len(r) >= 2}
    print(f"\nplain (non-reference) tables: {len(plain)} (e.g. {plain[:5]})")
    print("reference relationships (base <- referencing tables):")
    for base, r in list(refs.items())[:10]:
        print(f"    {base} <- {r}")
    print("\n>>> shared-reference groups (>=2 refs on one base; for the dedup test):")
    for base, r in shared.items():
        print(f"    {base} <- {r}")
    if not shared:
        print("    (none found; the dedup test needs two tables sharing a base)")

    # --- views --------------------------------------------------------------
    try:
        views = mat.get_views(version=latest)  # returns {view_name: metadata}
        live_views = [v for v, m in views.items() if m.get("live_compatible")]
        print(f"\nviews: {list(views)[:10]}")
        print(f"live-compatible views: {live_views}  (target for Phase 3 live views)")
    except Exception as e:
        print(f"\nviews: <unavailable: {type(e).__name__}: {e}>")

    # --- sample an annotation table to find columns + a real root id --------
    sample_table = next(
        (t for t in plain if "syn" in t.lower()), plain[0] if plain else None
    )
    print(f"\nsampling `{sample_table}` for column shapes + a real root id ...")
    if sample_table and latest is not None:
        df = mat.query_table(sample_table, limit=3, materialization_version=latest)
        cols = list(df.columns)
        root_cols = [c for c in cols if c.endswith("root_id")]
        pos_cols = [
            c for c in cols if c.endswith(("_x", "_y", "_z")) or "position" in c
        ]
        str_cols = [c for c in cols if str(df[c].dtype) == "object"]
        sample_root = None
        if root_cols and len(df):
            vals = df[root_cols[0]].tolist()
            sample_root = next((v for v in vals if v and v != 0), None)
        print(f"    columns      : {cols}")
        print(f"    root_id cols : {root_cols}")
        print(f"    position cols: {pos_cols}  (for filter_spatial)")
        print(f"    string cols  : {str_cols}  (for filter_regex)")
        print(f"    sample root  : {sample_root}")

    print("\n=== paste the block below back ===")
    print("CONFIG = {")
    print(f"    'datastack': {datastack!r},")
    print(f"    'version': {latest!r},")
    print(f"    'expired_version': {(expired[-1] if expired else None)!r},")
    print(f"    'table': {sample_table!r},")
    print(
        "    'shared_reference': "
        f"{(list(shared.items())[0] if shared else None)!r},  # (base, [ref1, ref2])"
    )
    print("}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(*sys.argv[1:])
