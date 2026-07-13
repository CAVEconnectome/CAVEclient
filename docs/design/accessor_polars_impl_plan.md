# Implementation plan: Polars-aligned accessor (filters & joins)

Status: **plan / for review.** Companion to `accessor_polars_alignment.md` (the
spec). Scope: make `TableQuery` (`caveclient/query/tables.py`) admit the
Polars-lazy surface for the **Exact** and **Alignable** rows of the spec's §2
mapping — concentrating on filters and joins — with `query()` as the terminal
(`collect`) and `query(lazy=True)` returning a `pl.LazyFrame`. Below the
`query(list[Table], **opts)` seam nothing changes.

## 1. Current state (three gaps to close)

1. **`filter` doesn't exist; `__call__` is keyword-only.** `__call__(self, **kwargs)`
   takes only `col=`/`col__op=` keyword filters; positional column-handle
   expressions are accepted by `query(*exprs, **opts)`, not by the builder call.
   So `syn(syn.size > 100)` is **not** valid today (only `syn.query(syn.size > 100)`
   or `syn(size__gt=100)` are). Adding `.filter()` and routing both call paths
   through it closes this and the latent inconsistency at once.
2. **The accessor emits a flat list, not edges — so chained `.join()` never reaches
   `query()`'s multi-join.** `query()` **fully supports** multi-join and stars via
   the edge list; the limitation is entirely accessor-side. `.join()` builds a flat
   `_parts` tuple and `_build_tables` emits a flat list of `Table`, which `query()`
   accepts only when it's a *pair* (a single join), refusing >2 with a pointer to
   the edge-list form. Worse, `.join()` attaches each new table to `self._primary`,
   so chaining is *star-shaped* — which a flat chain can't represent at all. Fix:
   emit the edge list the accessor's chaining already implies (see §4); no `query()`
   change.
3. **No slice verbs, combinators, aliases, or lazy terminal.** `limit`/`offset`
   only as `query()` kwargs; no `&`/`|`/`~` on filters; no `is_in`/`is_between`;
   `.select()` takes names but not column handles; `query()` always returns pandas.

## 2. Target API (concrete signatures)

```python
tq.filter(*exprs, **kwargs) -> TableQuery        # NEW: positional Filter exprs AND col=/col__op=
tq(*exprs, **kwargs)        -> TableQuery         # __call__ now delegates to .filter()
tq.select(*cols)            -> TableQuery          # accept Column handles or names
tq.join(other, *, on=None, left_on=None, right_on=None, how="inner") -> TableQuery
tq.limit(n) / tq.head(n)    -> TableQuery          # NEW: chainable; == query(limit=n)
tq.slice(offset, length)    -> TableQuery          # NEW: == query(offset=, limit=)
tq.query(*exprs, **opts)                           # terminal (Polars' collect); returns pandas. SOLE terminal — no .collect() alias
# tq.query(lazy=True) -> pl.LazyFrame is FUTURE (deltalake era) — see §9
tq.live_query(timestamp, *exprs, **opts)           # unchanged (query(timestamp=...))

# column handles (expressions.py), Polars spellings:
col.is_in(vals) / col.isin(vals)                   # is_in = alias
col.is_between(lo, hi, closed="both")              # NEW: numeric range -> ge & le
col == v / != / < / <= / > / >= / .regex / .within # existing
expr & expr                                        # NEW: conjunction
expr | expr / ~expr                                # NEW: raise InvalidFilterError (out-of-algebra)
```

## 3. Changes by file

### `caveclient/query/filters.py` — combinators + the in-filter divider
- Add `Filter.__and__`, `Filter.__or__`, `Filter.__invert__`.
  - `__and__(other)` → returns an `AllOf` conjunction (new frozen dataclass holding
    `tuple[Filter, ...]`); `AllOf.__and__` flattens so `a & b & c` is associative.
  - `__or__`, `__invert__` → raise `InvalidFilterError` with a message that names
    the reason and the escape hatch: *"the CAVE server WHERE is a conjunction of
    per-column predicates; OR/NOT aren't expressible — post-filter the returned
    frame, or issue separate queries. (For set-exclusion use `.notin([...])`.)"*
- Add a small `flatten_filters(items) -> tuple[Filter, ...]` helper that expands
  `Filter` / `AllOf` (and rejects bare `Column`, `bool`, etc. with a clear error).
  Used by `.filter()` and `.query()`.

### `caveclient/query/expressions.py` — Polars spellings on `Column`
- `is_in = isin` (alias). Add `is_between(lo, hi, closed="both")` → `AllOf` of
  `>=`/`>` and `<=`/`<` per `closed` ("both"/"left"/"right"/"none"); numeric only
  (raises for non-numeric kinds via the existing `Filter` op-legality check).
- `__or__`/`__invert__` on `Column` should also raise (so `~syn.x` / `a | b` where
  operands are bare handles fail with the same message, not `TypeError`).
- `parse_filter_kwargs` already supports the `__op` suffixes — no change.

### `caveclient/query/tables.py` — the builder surface
- **`.filter(*exprs, **kwargs)`**: `flatten_filters(exprs)` + `parse_filter_kwargs(kwargs)`,
  appended to `_filters`. `.filter` is the **canonical** verb.
- **`__call__` passes through to `.filter`** for compatibility:
  `def __call__(self, *exprs, **kwargs): return self.filter(*exprs, **kwargs)`
  (gains positional support; back-compatible for today's keyword callers). **Plan
  to deprecate:** keep `__call__` working now, add a `DeprecationWarning` pointing
  to `.filter()` in a later minor release once `.filter()` is documented and in
  use, and remove it on the next major. Document `.filter()` as the way from day
  one so new code never adopts `__call__`.
- **`.select(*cols)`**: map `Column -> col.name`, pass strings through; otherwise
  unchanged (per-node select on the primary).
- **`.join(other, *, on/left_on/right_on, how="inner")`**: resolve `(left_col,
  right_col)` from `on` or `left_on`/`right_on` (exactly one form; error if both
  or neither). **`how ∈ {"inner", "left"}` is the complete target set** — no other
  modes. `how` is a per-edge property of the *joined* (right/child) node (matches
  the design doc's "Join type" note). The parameter, the representation, and the
  filter-routing rule (§4.1) are all built now (design with capacity); `how="left"`
  **raises at a capability gate** until the server supports CAVE-run left, then the
  gate flips — no rearchitecture (§6 decision #2). See §4 for the representation
  change that makes chaining work.
- **`.limit(n)` / `.head(n)`**: store on the builder (new `_limit` slot) → folded
  into the `query()` opts. `.slice(offset, length)` sets `_offset` + `_limit`.
  (Keeps `query(limit=)`/`query(offset=)` working; the verbs are sugar.)
- **`.query(*exprs, **opts)`**: unchanged — still delegates to `mat.query()` and
  returns pandas. **The sole terminal — no `.collect()` alias** (mirror, not
  API-compatible; §6 #4). (The `lazy=`/Polars terminal is **future**, not this
  implementation — see §9.)
- **Out-of-algebra stubs** (committed — §6 decision #3 resolved: ship): methods
  named after the Polars verbs we *cannot* push — `with_columns`, `group_by`,
  `agg`, `sort` (others added as users hit them) — defined on `TableQuery` solely
  to **raise an educational error** instead of a bare `AttributeError`. E.g.
  `group_by(...)` → *"group_by isn't a server query operation — call `.query()` and
  aggregate in pandas/Polars, or use a `mat.views.<name>` rollup."* Implement via
  one small helper that takes the verb name and the post-`query()` guidance, so
  each stub is a one-liner. They catch borrowed-API muscle memory at the exact
  point it reaches across the divider (spec §5).

### `caveclient/materializationengine.py`
- **No change in this implementation.** The `query(list[Table], **opts)` seam and
  the pandas return are untouched; everything here is above it. (The `lazy=`
  terminal and Polars wrapping are future — §9.)

### `pyproject.toml`
- **No change.** Polars is not needed for this work; no new dependency or extra.

## 4. The one real refactor: accessor emits edges (query() already does multi-join)

`query()` fully supports multi-join and stars via the edge list — the work here is
purely teaching the accessor to *emit* edges instead of a flat list. To do that
(and to express stars, which `.join()`'s join-to-primary chaining already implies),
`TableQuery` should hold **nodes + edges** instead of a flat `_parts` chain:

- `_nodes: dict[str, _Part]` — per-table identity (suffix, select, kind, reference,
  merge_reference), insertion-ordered; first is the driving source.
- `_edges: tuple[(left_name, left_col, right_name, right_col, how), ...]`.
- `_filters` stays a flat tuple (each `Filter` already carries `column.table`).

`.join(other, ...)` merges `other`'s node/filters into self and appends one edge
connecting the **existing frame** to `other`'s primary — by default the
most-recently-added node (so `a.join(b).join(c)` is the **chain** a→b→c), or, when
`left_on` names a column, that column's owning node. The new node is the nullable
side when `how="left"`. Execution emits to `query()`:
- **0 edges** → a single `Table` (today's single-source path).
- **≥1 edge** → an **edge list** `[[Table(left, join_on=lcol, …), Table(right,
  join_on=rcol, how=…)], …]`, emitting a node's identity (filters/select/suffix)
  only on its **first** appearance and bare (`join_on` only) on repeats — exactly
  the rule `build_query_spec_from_edges` enforces. A single join degenerates to the
  flat pair `query()` already accepts.

This is the meatiest change but it is *additive to* the `query()` edge model
already shipped; the accessor just stops flattening and starts emitting edges.

**Chaining semantics (resolved): chains remain chains.** `a.join(b).join(c)` is a
chain a→b→c — each `.join()` attaches the new table to the **accumulated frame**
(the previous node by default, or the `left_on` column's owning node), exactly as
Polars joins the running result to the next frame. We do *not* collapse everything
onto the primary (a star). Arbitrary stars/trees remain expressible via the
explicit `Table` edge list at `query(...)`. (Note: today's flat `.join()` actually
attaches to `self._primary`; the nodes+edges change also corrects that to true
chain attachment.)

This does **not** contradict the design doc's "no implicit chain" rule — that rule
refuses inferring joins from a bare flat list of 3+ `Table`s passed to `query()`.
The accessor instead emits **explicit edges** (`[a,b], [b,c]`), one per `.join()`
call; the chain is stated, not inferred. So `query()` still never guesses a chain,
and the accessor never produces an ambiguous flat list.

Also fold in **column resolution across nodes**: after a join, `merged.<col>`
currently only sees the *primary's* columns (`__getattr__` reads `_parts[0]`), so a
joined column must be referenced through the other handle (`b.size`). With
nodes+edges, `merged.<col>` should resolve across all nodes (honoring suffixes,
erroring on ambiguity) so `merged.size` finds `b`'s column — Polars-faithful and a
prerequisite for the filter-routing correctness below.

**Incremental option:** ship §3 (filters, slice, aliases, combinators, single
`.join()` with the new kwargs) first on the *current* flat representation — that
already covers `a.join(b)` — and do the nodes+edges refactor as a fast-follow so
chaining/stars work. The single-join cut is genuinely useful and low-risk.
Recommend shipping §3 first, then §4.

### 4.1 Filter routing across merges — the correctness invariant

The worry to nail down: a `.filter()` written *after* a join/merge must be applied
correctly. In our model a filter is tagged by its column's origin table and
**pushed back to that origin** — into the server's nested `{table: {col: val}}`
dict, or the originating sub-query of a local merge — *regardless of chain
position* (`.filter()` accumulates into the flat `_filters` list keyed by
`column.table`; `_build_tables` groups by that table). Polars' `.filter()` is
position-*dependent* (it filters the frame at that point). So we must be explicit
about when "push to origin" preserves Polars semantics.

**The rule (standard predicate pushdown):** pushing a filter through a join to its
source is semantics-preserving **iff** the join is INNER (any side), or the filter
targets the **preserved** side of an outer join. It is *not* valid for the nullable
side of a left/outer join: `LEFT JOIN b WHERE b.col > x` (post-merge) differs from
`LEFT JOIN (b WHERE b.col > x)` (origin) — the former drops the null-`b` rows
(collapsing toward an inner join), the latter keeps them with nulls. Polars
`.filter()` after a left join is the post-merge form.

**Current state (inner-only): always correct.** Every join we emit is inner, so
origin-pushdown matches Polars exactly — filter-`b`-then-inner-join ≡
inner-join-then-filter-`b`. This is *why* the accessor can ignore chain position
today. The plan keeps origin-pushdown and adds a test asserting a post-join filter
on a joined column yields the same rows as filter-then-join.

**Reference merges fall under the same rule and are already handled.** A reference
merge can be left-ish (a base row need not have a matching reference row), so
filtering a reference column must make that merge **inner on the reference** —
which is exactly what the design does: filtering a reference column promotes the
auto-merge into an explicit (inner) `Join` ([[reference-column-transparency]]). So
"filter a merged-in reference column" pushes to the reference origin *and*
inner-izes the join — the correct "only rows whose reference satisfies the
predicate" semantics. An unfiltered reference merge has no predicate to mis-push.

**The line not to cross (when `how="left"` lands):** origin-pushdown must then be
**gated on join type**. A filter on the nullable side of a left join cannot be
pushed to origin; it must run **post-merge** (client-side on the result, or as a
true WHERE) to match Polars. That requires the builder to know a filter's position
relative to the join — filters become *position-aware* for outer joins, or we
refuse filtering the nullable side at origin. This is a concrete second reason
`how="left"` is deferred (§6): it changes the filter-routing contract, not just the
server capability.

## 5. The divider, enforced (spec §5 made concrete)

- **Within filters**: `Column` exposes only server ops; `&` → conjunction, `|`/`~`
  → raise. Column-vs-column (`syn.a > syn.b`) must raise — guard in `Filter`
  construction / the comparison operators when the RHS is a `Column`.
- **At the object**: the builder has only push-able verbs; the out-of-algebra stubs
  (§3) turn a Polars reach into an educational error, not a silent wrong answer.
- **Across backends**: unchanged — the same `Filter` list lowers to SQL today and
  arrow later; the lazy surface does not widen per source.

## 6. Open decisions

1. **Chaining semantics** — RESOLVED: chains remain chains. `a.join(b).join(c)`
   attaches each join to the accumulated frame (a→b→c), not to the primary (star).
   Arbitrary stars/trees via the explicit `Table` edge list (§4).
2. **`how="left"` delivery** — RESOLVED: **wait for server-side enablement, but
   design with the capacity now.** Left stays in the target set and the client is
   built end-to-end ready for it — the `how` parameter exists and accepts
   `"inner"|"left"`, the nodes+edges representation carries per-edge `how`, the
   §4.1 filter-routing rule (pre-merge node filters vs post-merge result filters,
   pushdown gated by preserved/nullable side) is implemented, and the local-merge
   path threads `how` into the pandas merge (design doc: nearly free). The *only*
   thing waiting is the **enablement**: `how="left"` raises at a single capability
   gate until the server supports left within a CAVE run, so the single-run and
   local-merge paths behave identically (no left that works only across an engine
   boundary). When the server ships it, we flip the gate — no rearchitecture,
   exactly the design doc's live-views pattern. We do *not* force left through the
   client-side local-merge as an interim.
3. **Out-of-algebra stubs** — RESOLVED: **ship** the educational
   `sort`/`group_by`/`with_columns`/`agg` raisers (§3). Cheap, and it's where the
   borrowed-API muscle memory lands.
4. **`.collect()` alias** — RESOLVED: **no.** `query()` is the sole terminal. We
   *mirror* Polars (familiar shape), we are not *API-compatible* (drop-in), and a
   distinct terminal name signals that — plus `query()` honestly carries CAVE
   kwargs (`version`/`timestamp`/…) a real `collect()` wouldn't. This "mirror, not
   API-compatible" stance is the general tie-breaker for future naming (§ spec).

## 7. Testing

Extend `tests/test_query_tablequery.py`:
- `.filter()` positional exprs + keyword filters accumulate and AND; `__call__`
  parity (incl. the now-valid `syn(syn.size > 100)`).
- **filter routing across a merge** (the §4.1 invariant): a filter on a joined
  column is grouped to *that column's origin table* in the emitted query — assert
  the nested `{table: {col: val}}` keys land on the joined table, not the primary;
  and assert position-independence (filter-before-join ≡ filter-after-join) for the
  inner case. A reference-column filter inner-izes the reference join and routes to
  the reference's origin.
- combinators: `a & b` flattens; `a | b` and `~a` raise `InvalidFilterError`;
  column-vs-column raises.
- `is_in`/`is_between` (incl. `closed=` variants; non-numeric `is_between` raises).
- `.select()` accepts handles and names.
- `.join()`: `on`, `left_on`/`right_on`, both-forms error, `how="inner"` ok,
  `how="left"` raises; (with §4) chained `a.join(b).join(c)` and a star emit the
  expected edge list.
- slice verbs map to offset/limit.
- out-of-algebra stubs raise with the educational message.
- `.query()` still returns pandas and delegates unchanged.

All existing `responses`-mocked tests stay green: changes are above the
`query(list[Table], **opts)` seam, and `.query()` still delegates there.

## 8. Smallest shippable increment

`.filter()` (+ `__call__` passthrough, deprecation deferred) · `&`/`|`/`~` on
filters · `is_in`/`is_between` · `.select()` handles · `.join(on/left_on/right_on,
how=)` with `how="left"` gated (raises until server-side) · `.limit/.head/.slice` ·
out-of-algebra stubs. No new dependency. That makes the common single-join chain
read like Polars. The nodes+edges refactor (§4) — which also gives true chain
attachment, cross-node column resolution, and the left filter-routing capacity —
follows to unlock multi-join and chains.

## 9. Future (not this implementation): the lazy / Polars terminal

Polars is **not** a current dependency and `query(lazy=True)` is **not** built
here. When the deltalake backend lands (design doc Phase 3), revisit per spec §6:

- Add `lazy: bool = False` to `mat.query()`, wrapping the result as a
  `pl.LazyFrame` (`pl.from_pandas(df).lazy()` eager-backed for SQL; a
  `scan_delta`-rooted plan with real pushdown for deltalake).
- Add a `caveclient[polars]` optional extra and a guarded import.
- Resolve `.attrs` carry-through (CAVE metadata doesn't survive `from_pandas`):
  drop-and-document vs. a side-channel onto the LazyFrame.

The surface built now is forward-compatible: adding `lazy=` later is purely
additive and changes nothing about the builder verbs above the seam.
