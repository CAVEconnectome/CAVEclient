"""Client-side merge of a heterogeneous (cross-engine) join.

Some joins can never be a single server call -- most importantly a table joined
to a **view** (a view is opaque SQL with no joinable handle), and in the future a
deltalake archive joined to a live server table. The server cannot do these, so
the client splits the query along engine boundaries, runs each piece where it
*can* be served, and merges the results locally.

The unit of decomposition is the **engine run** (:class:`Segment`), not the
individual source: consecutive CAVE tables are grouped into a single server-side
join, while each view stands alone (a view is one query, always). So
``table ⋈ table ⋈ view`` becomes two sub-queries -- one server join over the two
tables, one view query -- merged locally, rather than three.

This module is the *planner*, and it is deliberately pure: it knows nothing about
HTTP, views, or deltalake. It is handed the segment plan and a ``run_segment``
callable that executes one segment, and it

1. runs each segment in chain order, pushing the previous segment's boundary
   join-key values down as a semi-join filter so a downstream query is bounded by
   the keys that can survive the join, and
2. merges the frames left-deep with an inner join, applying each segment's suffix
   to columns that collide *across* segments so names stay unambiguous (the
   server already disambiguated collisions *within* a run).

Because a multi-table run is a real server join, its boundary key may come back
suffixed; the planner resolves the actual key column by probing the returned
frame (``_resolve_key``).

Temporal alignment is not handled here: every sub-query is run at the one
requested version/timestamp by ``run_segment``. Versions and timestamps are a
global clock in CAVE, so every engine resolves the same address to the same data
and there is nothing to reconcile.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from .spec import DEFAULT_JOIN_SUFFIXES


def default_suffixes(tables) -> dict:
    """Per-table suffixes mirroring ``build_query_spec_from_tables``.

    Each table keeps an explicit ``suffix`` if set, otherwise takes the
    positional pandas-style default (``_x``/``_y``/...). Positions are *global*
    across the whole query so a column suffixed inside one run can never collide
    with one suffixed inside another.
    """
    return {
        t.name: (t.suffix if t.suffix is not None else DEFAULT_JOIN_SUFFIXES[i])
        for i, t in enumerate(tables)
    }


@dataclass(frozen=True)
class Segment:
    """One engine run: a slice of the chain served by a single sub-query.

    A run of consecutive CAVE tables (``kind="cave"``, served by one server-side
    join) or a lone view (``kind="view"``). ``left_key`` / ``right_key`` are the
    *bare* join columns connecting this segment to the previous / next segment
    (``None`` at the ends of the chain); ``left_suffix`` / ``right_suffix`` are
    the suffixes those columns would carry if the server suffixed them inside this
    run, used to probe the result frame. ``repr_suffix`` disambiguates this
    segment's columns that collide with another segment's.
    """

    tables: tuple
    kind: str  # "cave" | "view"
    left_key: Optional[str]
    right_key: Optional[str]
    left_suffix: str
    right_suffix: str
    repr_suffix: str


def plan_segments(tables, kinds, suffixes: dict) -> list:
    """Partition a chain of sources into engine runs.

    Consecutive non-view sources are grouped into one CAVE segment (a single
    server-side join); each view is its own segment. ``kinds`` is the resolved
    kind per table (``"view"`` vs anything else); ``suffixes`` maps table name to
    its global suffix.
    """
    n = len(tables)
    runs = []  # (start, end) inclusive index ranges
    i = 0
    while i < n:
        if kinds[i] == "view":
            runs.append((i, i))
            i += 1
        else:
            j = i
            while j + 1 < n and kinds[j + 1] != "view":
                j += 1
            runs.append((i, j))
            i = j + 1

    segments = []
    for a, b in runs:
        segments.append(
            Segment(
                tables=tuple(tables[a : b + 1]),
                kind="view" if kinds[a] == "view" else "cave",
                # the first table's join_on connects to the previous segment;
                # the last table's join_on connects to the next one.
                left_key=tables[a].join_on if a > 0 else None,
                right_key=tables[b].join_on if b < n - 1 else None,
                left_suffix=suffixes[tables[a].name],
                right_suffix=suffixes[tables[b].name],
                repr_suffix=suffixes[tables[a].name],
            )
        )
    return segments


def _resolve_key(df: pd.DataFrame, bare: Optional[str], suffix: str) -> Optional[str]:
    """The actual name of a boundary join column in a (possibly suffixed) frame."""
    if bare is None:
        return None
    if bare in df.columns:
        return bare
    suffixed = f"{bare}{suffix}"
    if suffixed in df.columns:
        return suffixed
    raise KeyError(
        f"join key {bare!r} (or {suffixed!r}) is not in the sub-query result; "
        f"it must be selected so the local merge can join on it"
    )


def local_merge_query(
    segments,
    run_segment: Callable[[Segment, Optional[dict]], pd.DataFrame],
    *,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Run a segmented chain as independent sub-queries merged locally.

    Parameters
    ----------
    segments :
        The engine-run plan from :func:`plan_segments`, in chain order; the first
        is the driving segment.
    run_segment :
        ``run_segment(segment, extra_filter_in) -> DataFrame`` executes one
        segment's sub-query. ``extra_filter_in`` is ``None`` for the driving
        segment, else ``{segment.left_key: [values]}`` -- a semi-join restriction
        on this segment's incoming boundary column the callee must fold into the
        sub-query (on the *bare* column name; the server filters on real columns).
    offset, limit :
        Applied to the final merged frame (offset before limit).

    Returns
    -------
    pandas.DataFrame
        The inner-join of every segment.
    """
    items = []  # (segment, frame, left_name, right_name)
    prev_vals = None  # previous segment's boundary key values, to push down
    for seg in segments:
        extra = (
            {seg.left_key: prev_vals}
            if (prev_vals is not None and seg.left_key)
            else None
        )
        df = run_segment(seg, extra)
        lname = _resolve_key(df, seg.left_key, seg.left_suffix)
        rname = _resolve_key(df, seg.right_key, seg.right_suffix)
        items.append((seg, df, lname, rname))
        if len(df) == 0:
            break  # nothing can survive the join downstream
        prev_vals = pd.unique(df[rname].dropna()).tolist() if rname else None
    return _merge_segments(items, offset, limit)


def _merge_segments(items, offset, limit) -> pd.DataFrame:
    # Columns appearing in more than one segment collide on merge; rename each
    # such column with its segment's suffix. (Collisions *within* a CAVE run were
    # already disambiguated by the server, and those suffixed names are globally
    # unique, so they never reach this set.)
    counts: Counter = Counter()
    for _, df, _, _ in items:
        counts.update(df.columns)
    collide = {col for col, n in counts.items() if n > 1}

    prepared = []  # (frame, left_name, right_name) with cross-segment names final
    for seg, df, lname, rname in items:
        suf = seg.repr_suffix or ""
        rename = {c: f"{c}{suf}" for c in df.columns if c in collide and suf}
        frame = df.rename(columns=rename) if rename else df
        prepared.append(
            (
                frame,
                rename.get(lname, lname) if lname else None,
                rename.get(rname, rname) if rname else None,
            )
        )

    merged, _, prev_right = prepared[0]
    for frame, left, right in prepared[1:]:
        merged = merged.merge(
            frame,
            how="inner",
            left_on=prev_right,
            right_on=left,
            suffixes=("", ""),  # collisions already resolved above
        )
        prev_right = right
    if offset:
        merged = merged.iloc[offset:]
    if limit is not None:
        merged = merged.iloc[:limit]
    return merged.reset_index(drop=True)
