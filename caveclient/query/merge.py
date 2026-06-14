"""Client-side merge of a heterogeneous (cross-engine) join.

Some joins can never be a single server call -- a table joined to a **view** (a
view is opaque SQL with no joinable handle), and in future a deltalake archive
joined to a live server table. The server can't do these, so the client splits
the join *graph* along engine boundaries, runs each piece where it can be served,
and merges the results locally.

The unit of decomposition is the **engine run**: a maximal connected subgraph of
CAVE tables (executed as one server-side join) or a lone view (a view is always
one query). Runs are connected by **cross-run edges** -- the joins whose two
endpoints fall in different runs -- and those edges must form a single connected
**tree** (cycles and disconnected graphs are refused). Each cross-run edge is a
local-merge seam, and its two columns are the semi-join key-pushdown keys.

This module is the *planner* and is deliberately pure: it knows nothing about
HTTP, views, or deltalake. Given the run plan and a ``run_executor`` callback it

1. executes runs parent-before-child over the tree, pushing each parent's
   boundary key values down as a semi-join filter, then
2. merges the frames over the tree with inner joins, suffixing columns that
   collide *across* runs (the server already disambiguated collisions *within* a
   CAVE run).

A linear chain is just the degenerate tree where every run has one parent, so
this subsumes the old chain-only planner rather than duplicating it.

Temporal alignment is not handled here: every sub-query runs at the one requested
version/timestamp. Versions/timestamps are a global clock in CAVE, so every
engine resolves the same address to the same data -- nothing to reconcile.
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Callable, Optional

import pandas as pd

from .spec import DEFAULT_JOIN_SUFFIXES, InvalidQueryError


def default_suffixes(tables) -> dict:
    """Per-table suffixes mirroring ``build_query_spec_from_tables``.

    Each table keeps an explicit ``suffix`` if set, otherwise takes the
    positional pandas-style default (``_x``/``_y``/...). Positions are *global*
    across the query so a column suffixed inside one run can't collide with one
    suffixed inside another.
    """
    return {
        t.name: (t.suffix if t.suffix is not None else DEFAULT_JOIN_SUFFIXES[i])
        for i, t in enumerate(tables)
    }


@dataclass
class Run:
    """One engine run served by a single sub-query.

    ``kind`` is ``"cave"`` (a connected group of CAVE tables, one server-side
    join) or ``"view"`` (a lone view). ``tables`` are the participating
    :class:`~caveclient.query.spec.Table` objects; ``repr_suffix`` disambiguates
    this run's columns that collide with another run's.
    """

    kind: str
    tables: list = field(default_factory=list)
    repr_suffix: str = ""


@dataclass(frozen=True)
class _TreeEdge:
    """How a child run attaches to its parent run in the merge tree."""

    parent: int
    parent_table: str
    parent_column: str
    parent_suffix: str
    child_table: str
    child_column: str
    child_suffix: str


def plan_runs(table_order, joins, kinds, suffixes):
    """Partition the join graph into engine runs.

    Parameters
    ----------
    table_order :
        Distinct table names in first-appearance order (gives runs a stable
        order; the first run is the driving one).
    joins :
        The pairwise :class:`~caveclient.query.spec.Join` edges.
    kinds :
        ``{name: "view" | "table" | ...}``; only ``"view"`` is special.
    suffixes :
        ``{name: suffix}`` for the run representative suffix.

    Returns
    -------
    (runs, run_of) :
        The list of :class:`Run` and a ``{table_name: run_index}`` map. CAVE
        tables joined by CAVE–CAVE edges share a run; every view is its own run.
    """
    # union-find over CAVE–CAVE edges
    parent = {n: n for n in table_order}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for j in joins:
        a, b = j.left_table, j.right_table
        if kinds.get(a) != "view" and kinds.get(b) != "view":
            parent[find(a)] = find(b)

    def run_key(n):
        return ("view", n) if kinds.get(n) == "view" else ("cave", find(n))

    runs, run_of, key_to_idx = [], {}, {}
    for name in table_order:
        key = run_key(name)
        if key not in key_to_idx:
            key_to_idx[key] = len(runs)
            runs.append(Run(kind="view" if key[0] == "view" else "cave"))
        idx = key_to_idx[key]
        runs[idx].tables.append(name)
        run_of[name] = idx
    for run in runs:
        run.repr_suffix = suffixes.get(run.tables[0], "")
    return runs, run_of


def build_run_tree(runs, run_of, joins, suffixes, root=0):
    """Order the runs as a tree rooted at ``root`` from the cross-run edges.

    Refuses anything that isn't a single connected tree: a disconnected graph
    (some run unreachable) or a cyclic one (a redundant cross-run edge).

    Returns ``(order, edges)`` where ``order`` is a parent-before-child run
    ordering and ``edges`` maps each non-root run index to its :class:`_TreeEdge`.
    """
    cross = []  # (run_a, run_b, _TreeEdge-ish info per side)
    adj = {i: [] for i in range(len(runs))}
    for j in joins:
        ra, rb = run_of[j.left_table], run_of[j.right_table]
        if ra == rb:
            continue  # intra-run join; the server handles it
        side = {
            ra: (j.left_table, j.left_column, suffixes.get(j.left_table, "")),
            rb: (j.right_table, j.right_column, suffixes.get(j.right_table, "")),
        }
        cross.append((ra, rb, side))
        adj[ra].append((rb, side))
        adj[rb].append((ra, side))

    order, edges, visited = [root], {}, {root}
    dq = deque([root])
    while dq:
        u = dq.popleft()
        for v, side in adj[u]:
            if v in visited:
                continue
            visited.add(v)
            order.append(v)
            dq.append(v)
            pt, pc, ps = side[u]
            ct, cc, cs = side[v]
            edges[v] = _TreeEdge(u, pt, pc, ps, ct, cc, cs)

    if len(visited) != len(runs):
        raise InvalidQueryError(
            "this join spans engines but its graph is disconnected — every table "
            "must be reachable through the joins (only a single connected tree of "
            "joins is supported across engines)"
        )
    if len(cross) != len(runs) - 1:
        raise InvalidQueryError(
            "this join spans engines but its graph has a cycle — only a single "
            "connected tree of joins is supported across engines"
        )
    return order, edges


def _resolve_key(df: pd.DataFrame, bare: str, suffix: str) -> str:
    """The actual name of a boundary join column in a (possibly suffixed) frame."""
    if bare in df.columns:
        return bare
    suffixed = f"{bare}{suffix}"
    if suffixed in df.columns:
        return suffixed
    raise KeyError(
        f"join key {bare!r} (or {suffixed!r}) is not in the sub-query result; "
        f"it must be selected so the local merge can join on it"
    )


def graph_merge_query(
    runs,
    order,
    edges,
    run_executor: Callable[[Run, Optional[tuple]], pd.DataFrame],
    *,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Execute the run tree and merge the results locally.

    ``run_executor(run, incoming)`` runs one run's sub-query; ``incoming`` is
    ``None`` for the root, else ``(target_table, column, [values])`` -- a
    semi-join restriction the executor folds into ``target_table``'s ``filter_in``
    on the *bare* column. The merge is an inner join over the tree; columns
    colliding across runs are suffixed with the owning run's representative
    suffix.
    """
    frames: dict = {}
    for idx in order:
        if idx == order[0]:
            incoming = None
        else:
            e = edges[idx]
            if e.parent not in frames:
                break  # an ancestor was empty; nothing can survive downstream
            pcol = _resolve_key(frames[e.parent], e.parent_column, e.parent_suffix)
            vals = pd.unique(frames[e.parent][pcol].dropna()).tolist()
            if not vals:
                break  # parent yielded no keys -> the inner join is empty
            incoming = (e.child_table, e.child_column, vals)
        frames[idx] = run_executor(runs[idx], incoming)

    return _merge_tree(runs, order, edges, frames, offset, limit)


def _merge_tree(runs, order, edges, frames, offset, limit) -> pd.DataFrame:
    # if a run was skipped (empty ancestor / no keys), the inner join is empty
    short_circuited = any(idx not in frames for idx in order)

    counts: Counter = Counter()
    for f in frames.values():
        counts.update(f.columns)
    collide = {c for c, n in counts.items() if n > 1}

    prepared, renames = {}, {}
    for idx, f in frames.items():
        suf = runs[idx].repr_suffix or ""
        rename = {c: f"{c}{suf}" for c in f.columns if c in collide and suf}
        prepared[idx] = f.rename(columns=rename) if rename else f
        renames[idx] = rename

    acc = prepared[order[0]]
    for idx in order[1:]:
        if idx not in frames:
            break  # parent-before-child: once one is missing, stop
        e = edges[idx]
        left_raw = _resolve_key(frames[e.parent], e.parent_column, e.parent_suffix)
        right_raw = _resolve_key(frames[idx], e.child_column, e.child_suffix)
        left_on = renames[e.parent].get(left_raw, left_raw)
        right_on = renames[idx].get(right_raw, right_raw)
        acc = acc.merge(
            prepared[idx],
            how="inner",
            left_on=left_on,
            right_on=right_on,
            suffixes=("", ""),  # cross-run collisions already renamed above
        )

    if short_circuited:
        acc = acc.iloc[0:0]
    if offset:
        acc = acc.iloc[offset:]
    if limit is not None:
        acc = acc.iloc[:limit]
    return acc.reset_index(drop=True)
