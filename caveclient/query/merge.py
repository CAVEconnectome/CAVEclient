"""Client-side merge of a heterogeneous (cross-engine) join.

Some joins can never be a single server call -- most importantly a table joined
to a **view** (no shared SQL), and in the future a deltalake archive joined to a
live server table. The server cannot do these, so the client queries each source
independently and merges the results locally.

This module is the *planner*, and it is deliberately pure: it knows nothing about
HTTP, views, or deltalake. It is handed a list of sources (chain-joined
:class:`~caveclient.query.spec.Table` objects, each with a ``name`` and a
``join_on`` column) and a ``run_source`` callable that executes one source's
sub-query. It

1. runs each source in chain order, pushing the previous source's join-key
   values down as a semi-join filter so a downstream query is bounded by the
   keys that can actually survive the join (``run_source`` receives them as an
   ``extra_filter_in`` it folds into that source's filters), and
2. merges the frames left-deep with an inner join on the join columns, applying
   each source's suffix to columns that collide across sources so the output
   column names mirror a server-side join (``_x``/``_y``/...).

Temporal alignment is *not* handled here: every sub-query is run at the one
requested version/timestamp by ``run_source``. Because versions and timestamps
are a global clock in CAVE (a deltalake export is a dump *of* a materialization
version), every engine resolves the same address to the same data, so there is
nothing to reconcile -- a source that cannot serve the address simply fails in
its own backend.
"""

from __future__ import annotations

from collections import Counter
from typing import Callable, Optional

import pandas as pd

from .spec import DEFAULT_JOIN_SUFFIXES


def default_suffixes(tables) -> dict:
    """Per-table suffixes mirroring ``build_query_spec_from_tables``.

    Each table keeps an explicit ``suffix`` if set, otherwise takes the
    positional pandas-style default (``_x``/``_y``/...).
    """
    return {
        t.name: (t.suffix if t.suffix is not None else DEFAULT_JOIN_SUFFIXES[i])
        for i, t in enumerate(tables)
    }


def local_merge_query(
    tables,
    run_source: Callable[[object, Optional[dict]], pd.DataFrame],
    *,
    suffixes: Optional[dict] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Run a chain-joined query as independent sub-queries merged locally.

    Parameters
    ----------
    tables :
        Chain-joined sources (``Table`` objects); ``tables[i]`` joins to
        ``tables[i+1]`` on each one's ``join_on`` column. The first is the
        driving source.
    run_source :
        ``run_source(table, extra_filter_in) -> DataFrame`` executes one source's
        sub-query. ``extra_filter_in`` is ``None`` for the driving source, else a
        ``{join_column: [values]}`` semi-join restriction the callee must fold
        into that source's ``filter_in`` (intersecting if it already filters that
        column).
    suffixes :
        ``{table_name: suffix}``; defaults to :func:`default_suffixes`.
    offset, limit :
        Applied to the final merged frame (a join's row count is only known after
        the merge), offset before limit.

    Returns
    -------
    pandas.DataFrame
        The inner-join of every source, columns colliding across sources renamed
        with the owning source's suffix.
    """
    suffixes = suffixes or default_suffixes(tables)
    results = []
    prev_vals = None  # the previous source's join-key values, to push down
    for t in tables:
        extra = {t.join_on: prev_vals} if (prev_vals is not None and t.join_on) else None
        df = run_source(t, extra)
        results.append((t, df))
        if len(df) == 0:
            # nothing can survive the join downstream; stop early
            break
        if t.join_on and t.join_on in df.columns:
            prev_vals = pd.unique(df[t.join_on].dropna()).tolist()
        else:
            prev_vals = None
    return _merge_chain(results, suffixes, offset, limit)


def _merge_chain(results, suffixes, offset, limit) -> pd.DataFrame:
    # Columns appearing in more than one source collide on merge; rename each
    # such column with its source's suffix so the result mirrors a server join.
    counts: Counter = Counter()
    for _, df in results:
        counts.update(df.columns)
    collide = {col for col, n in counts.items() if n > 1}

    frames, key_name = [], []
    for t, df in results:
        suf = suffixes.get(t.name) or ""
        rename = {c: f"{c}{suf}" for c in df.columns if c in collide and suf}
        frames.append(df.rename(columns=rename) if rename else df)
        key_name.append(rename.get(t.join_on, t.join_on) if t.join_on else None)

    merged = frames[0]
    for i in range(1, len(frames)):
        merged = merged.merge(
            frames[i],
            how="inner",
            left_on=key_name[i - 1],
            right_on=key_name[i],
            suffixes=("", ""),  # collisions already resolved by the rename above
        )
    if offset:
        merged = merged.iloc[offset:]
    if limit is not None:
        merged = merged.iloc[:limit]
    return merged.reset_index(drop=True)
