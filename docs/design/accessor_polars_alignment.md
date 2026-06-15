# Spec: how Polars-like can the accessor get?

Status: **exploration / for review.** Scope: the `client.materialize.tables.<name>`
/ `.views.<name>` accessor (`TableQuery`, `caveclient/query/tables.py`). Question
from review: can the chained-builder mode look as much like Polars' lazy API as
possible, with `query()` standing in for `collect()`? This specs how close we can
get with the current architecture, and — just as important — where the analogy
*must* stop.

## Motivation: one API to learn, not two

The reason to make this mode look like Polars is **not** performance and **not**
pushdown semantics — it's **cognitive load**. CAVEclient users are data people
who increasingly already know Polars. If the query builder borrows Polars'
surface, there is *nothing new to learn*: a Polars user is productive on day one,
and a CAVEclient user who learns this builder has, in effect, learned a slice of
an API they can use everywhere else. We get to lean on Polars' docs, tutorials,
and muscle memory instead of writing, documenting, and teaching a bespoke query
API of our own. Reusing a large, well-known project's API *is* the feature.

A secondary benefit: it's a gentle **nudge toward Polars over pandas**. The
builder's idioms — lazy, immutable, expression-based filters — are Polars idioms,
and the terminal hands back a frame that drops straight into `pl.from_pandas(...)`,
so the path of least resistance points at Polars for whatever comes next.

**The strategic driver: deltalake dumps are coming.** Over the next several
months, deltalake table exports become a larger part of the CAVE ecosystem
(design doc Phase 3) — and unlike the CAVE SQL server, a deltalake/parquet store
*is* a true predicate-pushdown target, read natively with arrow and Polars. Two
things follow. First, the people working with those dumps will already be in
Polars, so a Polars-shaped accessor is aligning with the engine the data
literally lives in, not just a familiar-API nicety. Second, the **same closed
`Filter` list** the builder produces compiles two ways (design doc §3.3): to the
server's filter dicts today, and to an arrow predicate expression
(`to_arrow_expr`) for deltalake — so on a deltalake source the identical lazy
chain becomes *real* predicate/partition pushdown. The Polars surface is thus
forward-aligned with where the ecosystem is heading: the pushdown subset that is
a fixed-vocabulary convenience against SQL is genuine, value-compounding pushdown
against parquet.

The flip side — and the reason §5 (the divider) carries the weight it does —
is that **borrowing a familiar API raises the cost of where it stops.** A user
with Polars muscle memory will reach for `.with_columns`, `.group_by`, `.sort`;
if those silently do the wrong thing or look available but aren't real, the
familiarity becomes a trap. So the rule is: match the subset exactly *and* make
the boundary loud. An unfamiliar API that's honest beats a familiar one that lies.

**Guiding stance: mirror, not API-compatible.** We deliberately *mirror* Polars'
shape and idioms (so it's familiar and transferable) but do **not** promise
drop-in API compatibility. Where semantics differ, we keep a distinct name rather
than impersonate Polars — the clearest case being the terminal: it stays
`query()` (carrying CAVE's `version=`/`timestamp=`/… kwargs), with **no
`collect()` alias**, precisely so no one mistakes this for a Polars `LazyFrame`.
This is the tie-breaker for future naming: when a Polars name would imply a
compatibility we don't offer, prefer the honest CAVE name.

## 1. The constraint that bounds the borrowing

The analogy is attractive for the reasons above, but it is bounded by one hard
fact: the server is not a compute engine. A Polars `LazyFrame` is a builder over
a **general compute graph**: any
expression, any aggregation, any window, optimized then executed by an in-process
engine. The CAVE accessor is a builder too, but over a **fixed, closed query
algebra** that is *pushed to a server* — and the server is not a compute engine.
The pushable algebra is exactly (confirmed in `query/kinds.py`, `query/spec.py`,
`materializationengine.query`):

- **Filter** — a *conjunction* of per-column predicates: `in`, `not_in`, `equal`,
  `greater`, `less`, `greater_equal`, `less_equal`, `regex`, `spatial` (bbox).
- **Join** — `inner` today (`left` anticipated, per-edge), on named columns.
- **Select** — per-table column projection.
- **Slice** — `limit` / `offset`, plus `random_sample`.

That's the whole algebra. There is no server-side sort, no aggregation, no
computed/derived columns, no `OR` across predicates. So the honest framing is:

> The accessor is a **`LazyFrame` restricted to predicate-pushdown, projection,
> equi-join, and slice.** For that subset we can match Polars *syntax* almost
> 1:1. Everything outside the subset is not "unimplemented" — it's not a query
> operation at all, and should either be refused with a clear message or done by
> the caller on the returned frame.

Two architectural facts already line up nicely with Polars and are worth keeping:

- **Immutability / chaining.** Every `TableQuery` op returns a *new* object
  (`_with`), exactly like `LazyFrame`. Chaining and reuse of a partial query are
  already safe.
- **Lazy until the terminal.** Nothing hits the network until `query()`. That is
  precisely `collect()`'s contract.

### The governing principle: the lazy surface *is* the pushable algebra

The one rule that keeps the analogy honest:

> A method exists on the lazy `TableQuery` **iff** it lowers to the source's
> server algebra. If you can call it on the accessor, it is pushed to the server;
> if you need anything else, you call `query()` and continue on the returned
> frame.

There is deliberately **no mixed-chain optimizer** that quietly splits a Polars
pipeline into "the part we pushed" and "the part we ran locally." That magic is
exactly what would mislead, because **CAVE SQL is not a predicate-pushdown
target.** A parquet/arrow reader accepts arbitrary predicate expressions and
pushes what it can; the CAVE server exposes *fixed filter endpoints* — a closed
vocabulary of `{column op literal}` operations on materialized columns. So the
boundary is not "what our optimizer happens to support today"; it is a hard
property of the backend, and the cleanest way to make it legible is to put it in
the **type**: non-pushable operations are simply *not methods on the lazy
object*. See §5 for the full division.

## 2. Method-by-method mapping

Legend: **Exact** = same call shape achievable; **Alignable** = small rename/kwarg
change gets us there; **Out-of-algebra** = not pushable, must refuse or be a
post-`query()` op.

| Polars (lazy) | Accessor today | Proposed accessor | Status |
|---|---|---|---|
| `pl.scan_*(...)` (source) | `mat.tables.<name>` | same | **Exact** |
| `pl.col("x")` | `tq.x` / `tq["x"]` | same (+ `mat.col`? — see §4) | **Exact** |
| `.filter(pl.col("x") > 1)` | `tq(tq.x > 1)` / `tq(x__gt=1)` | add `.filter(*exprs, **kw)` | **Exact** |
| `.select("a", "b")` | `.select("a","b")` | also accept column handles | **Exact** |
| `.join(o, on=, how=)` | `.join(o, on=)` | add `left_on=/right_on=`, `how=` | **Alignable** |
| `.limit(n)` / `.head(n)` | `query(limit=n)` | add chainable `.limit(n)`/`.head(n)` | **Alignable** |
| `.slice(off, n)` | `query(offset=, limit=)` | add `.slice(off, n)` | **Alignable** |
| `.collect()` | `.query()` | keep `.query()` (the ask) | **Exact** (renamed) |
| `pl.col("x").is_in([...])` | `tq.x.isin([...])` | add `is_in` alias | **Alignable** |
| `pl.col("x").is_between(a,b)` (spatial) | `tq.pt.within([[..],[..]])` | add `is_between`? (bbox only) | **Alignable** |
| `expr_a & expr_b` | pass two filters | support `Filter & Filter` | **Alignable** |
| `expr_a \| expr_b`, `~expr` | — | refuse with message | **Out-of-algebra** (no server `OR`) |
| `.sort(by, descending=)` | — | refuse, or explicit post-op | **Out-of-algebra** (no server sort) |
| `.with_columns(...)` | — | refuse → do on returned frame | **Out-of-algebra** (no compute) |
| `.group_by(...).agg(...)` | — | refuse → point at *views* | **Out-of-algebra** (aggregation = a view) |
| `.unique()`, `.drop_nulls()`, `.fill_null()`, `.rename()` | — | post-`query()` op | **Out-of-algebra** |
| `.tail(n)` | — | refuse (no reverse) | **Out-of-algebra** |

## 3. Proposed surface (the Exact/Alignable column, made concrete)

These are additive — `__call__` and the current `query()` keep working.

```python
syn = mat.tables.synapses_pni_2

# --- filter: the headline alignment -------------------------------------
# .filter() as a primary verb; expressions AND together within and across calls
df = (syn
      .filter(syn.size > 100, syn.post_pt_root_id.is_in([864691135517653777]))
      .filter(syn.pre_pt_position.within([[1e5,1e5,2e4],[1.1e5,1.1e5,2.1e4]]))
      .query(version=1043))

# keyword form still accepted (it's just sugar over .filter)
df = syn.filter(size__gt=100).query()

# --- select: accept handles or names ------------------------------------
df = syn.select(syn.id, syn.size, "post_pt_root_id").query()

# --- join: Polars kwargs ------------------------------------------------
nuc = mat.tables.nucleus_detection_v0
df = (syn.filter(syn.size > 100)
      .join(nuc, left_on="post_pt_root_id", right_on="pt_root_id", how="inner")
      .query())

# --- slice verbs --------------------------------------------------------
df = syn.filter(syn.size > 100).head(1000).query()      # == query(limit=1000)
df = syn.slice(2000, 500).query()                        # offset=2000, limit=500

# --- terminal: query() is collect(); live is collect-at-a-timestamp -----
df = syn.filter(syn.size > 100).query(version=1043)
df = syn.filter(syn.size > 100).live_query(timestamp)
```

Notes:

- **`.filter` AND-semantics already match.** Multiple predicates in one call, and
  chained `.filter().filter()`, both conjoin — same as Polars. We get this free
  from the current accumulate-on-`_with` behavior.
- **`&` is implementable; `|`/`~` are the wall.** `Filter & Filter` can lower to
  "two pushed predicates". `|` has no conjunctive-server equivalent and `~`
  generalizes past `not_in`, so both should raise `InvalidFilterError` with a
  message that says *why* ("the server filters are a conjunction of per-column
  predicates; combine with `&`, or post-filter the returned frame") rather than
  silently doing something lossy.
- **`.query()` is the sole terminal — no `.collect()` alias** (decided). We
  *mirror* Polars, we are not *API-compatible*; a distinct terminal name signals
  that, and `query()` honestly carries CAVE kwargs (`version=`/`timestamp=`/
  `split_positions=`/…) a real `collect()` wouldn't. See the "mirror, not
  API-compatible" principle in §Motivation.

## 4. Nice-to-haves that reinforce the analogy

- **`.explain()`** — Polars has `.explain()`/`.show_graph()`. We can return the
  built `QuerySpec`, the resolved backend/route, and the exact wire payload
  *without* executing. Cheap, and it makes "lazy until collect" legible.
- **`mat.col("x")`** — a free-floating column handle (à la `pl.col`) for building
  predicates before binding a table. Marginal value over `tq.x`; list as optional.
- **A `.str.contains` spelling** mapping to `regex` — only if we want the Polars
  expression-namespace feel; `tq.tag.regex(...)` already reads fine.

## 5. The divider: push vs. post — and why it's a hard line

This is the load-bearing part of the proposal — and it's load-bearing *because*
of the motivation, not despite it. We borrowed Polars precisely so users could
rely on muscle memory; that same muscle memory is what will lead them to reach
for `.with_columns`/`.group_by`/`.sort`. So the division between what runs on the
server and what runs locally must be **explicit, enforced, and visible** — not
the output of a hopeful optimizer. (It is also a hard backend property: CAVE SQL
is not a predicate-pushdown target, §1.) We draw the line in three places, each
catching the boundary at a different altitude so nothing slips through silently.

### 5.1 At the object level — two kinds, not one fluent blur

There are exactly two objects, and the line between them *is* the divider:

| | **`TableQuery`** (lazy, pre-`query()`) | **the returned frame** (post-`query()`) |
|---|---|---|
| What it is | a builder over the server algebra | a real `pandas.DataFrame` |
| Methods | only push-able verbs: `filter`, `select`, `join`, `limit`/`head`/`slice` | everything pandas/Polars: `sort`, `assign`/`with_columns`, `groupby`/`agg`, `merge`, … |
| Cost model | shapes the **server query** (reduces bytes transferred) | operates on **already-fetched** rows |
| Terminal | `query()` / `live_query()` → crosses the line | `pl.from_pandas(df)` → continue in real Polars |

The rule a user can hold in their head: **methods on the accessor change what the
server sends you; methods after `query()` change what you already have.** Because
non-pushable operations are *not on the lazy object at all*, you cannot
accidentally write a query that looks pushed but isn't. (This mirrors how the
local-merge planner already partitions a heterogeneous join into per-engine runs:
same instinct — divide by what each engine can actually do — applied to single-
source ops.)

### 5.2 Within filters — the predicate vocabulary is the divider

A `Column` handle exposes **only** the server's predicate ops (`==`, `!=`, `<`,
`<=`, `>`, `>=`, `isin`, `notin`, `regex`, `within`). It is deliberately *narrower
than `pl.col`*, and the omissions are the boundary, enforced at construction
(`InvalidFilterError`) rather than discovered as a server 500:

- **Column-vs-column** (`pl.col("a") > pl.col("b")`) — not expressible; CAVE
  filters are `{column op literal}`. A `Column` compared to another `Column`
  should raise, not build.
- **Arithmetic / computed predicates** (`pl.col("a") % 2 == 0`,
  `pl.col("a") + pl.col("b") > 1`) — no compute is pushed; not a method.
- **Boolean `OR` / `NOT`** (`a | b`, `~a`) — the server `WHERE` is a *conjunction*
  of per-column predicates. `&` lowers to "two pushed filters"; `|` and general
  `~` have no server form and must raise with a message that names the reason and
  the escape hatch ("post-filter the returned frame, or model it as two queries").
- **`.str` / `.dt` namespaces** — only `regex` exists server-side; the rest is
  post-fetch.

So "true predicate pushdown will not work here" is made concrete: the predicate
*language* is the closed vocabulary, and anything richer is a frame operation, by
construction.

### 5.3 Across backends — the divider is stable, and deltalake makes it pay off

The deltalake/arrow backend executes the **same** closed `Filter` list (via
`to_arrow_expr`, design doc §3.3) — it pushes the same vocabulary to parquet with
partition/predicate pushdown. Crucially, it does **not** widen the lazy surface.
That stability is the whole point: the same chained query must mean the same thing
regardless of which engine serves it, so we never grow the lazy object's method
set just because one backend *could* do more. Richer arrow-only predicates, if
ever wanted, would be an explicit, separately-named opt-in — never a silent
capability that changes what `.filter()` accepts depending on the source.
(Consistent with the design's "no silent capability divergence" stance and
`can_handle` gating.)

What deltalake changes is not the *surface* but the *payoff*: against SQL the
pushdown subset is a fixed-endpoint convenience; against a parquet dump the
identical `filter`/`select`/`slice` chain is real predicate/partition pushdown
that skips row-groups on disk. So holding the surface stable means a query a user
already wrote gets *faster and cheaper for free* as data migrates to deltalake —
the strongest argument for committing to this surface now rather than after the
dumps land.

**Forward opportunity:** because a deltalake source is itself lazy,
`query(lazy=True)` on a deltalake-backed query hands back a Polars `LazyFrame`
instead of a materialized frame, turning the divider into a clean lazy handoff
into real Polars. That raises the question of what the accessor object *is* for a
deltalake table — addressed in §6.

### 5.4 The operations that live on the far side of the line

For completeness, the common Polars verbs that are **post-`query()`**, with where
they actually belong:

- **Aggregation / `group_by` / window** — not a query operation at all. In CAVE an
  aggregate *is a view* (a server-defined, possibly materialized rollup), so the
  Polars-shaped answer is "there's a `mat.views.<name>` for that"; for ad-hoc
  rollups, `pl.from_pandas(df).group_by(...)`.
- **Sort** — the server does not order, so `limit` is "first N returned", *not*
  "top-N by a column". A lazy `.sort()` would be a correctness trap (it could only
  sort the already-truncated page). Omit it; sort the returned frame. (If a
  convenience is ever wanted, it must be a clearly-named post-collect helper that
  warns it fetched-then-sorted.)
- **Computed columns (`with_columns`)** — no expression compute is pushable;
  returned-frame territory.

## 6. So what *is* `mat.tables.my_deltalake_table`?

This is the question the deltalake direction forces, and it deserves a deliberate
answer because the two tempting ones pull in opposite directions.

- **Tempting answer A — it's literally a `pl.LazyFrame`.** A deltalake table is a
  native Polars/arrow source, so why wrap it? Hand back `pl.scan_delta(...)` and
  the user has the entire real Polars API, lazily, with true pushdown.
- **Tempting answer B — it's our closed-algebra `TableQuery`, like every other
  source.** Uniform surface, the divider holds, one API to learn.

A alone breaks exactly what §5.3 and the motivation argue for: if a deltalake
table is a raw `LazyFrame` while a SQL table is our builder, the surface
**diverges by source** — same-looking code does different things, exposes
different methods, and (worst) carries a different auth/credential-lifecycle
model. B alone throws capability away: it crawls a backend that genuinely *can* do
full lazy Polars through a fetch boundary it doesn't need.

**The resolution: uniform builder, one terminal (`query`) with a `lazy=` flag,
auth minted at the edge.**

1. `mat.tables.<name>` is **always our auth-aware `TableQuery`**, SQL or deltalake
   alike. The closed-algebra builder (`filter`/`select`/`join`/`slice`) is the one
   surface everyone learns, source-agnostic by construction.
2. `query()` is **always the escape hatch** — the single terminal, no second verb
   to learn. Its `lazy=` flag chooses the *form* of the result, not a different
   door:
   - `query()` (default, `lazy=False`) → a **materialized frame** (pandas today,
     for back-compat), uniform across sources, return type stable.
   - `query(lazy=True)` → **always a `pl.LazyFrame`, regardless of source.** On a
     deltalake source it is a real, credentialed `scan_delta`-rooted plan with true
     predicate/partition pushdown to disk. On a CAVE SQL source it is a LazyFrame
     rooted in the already-fetched in-memory result (`pl.from_pandas(df).lazy()`):
     the server-side filtering already happened in the builder, so the laziness
     here is **not additive** — it buys no pushdown — but the return type stays
     uniform and the user is in real Polars either way.

Keeping it on `query()` (rather than a separate `.lazy()`/`.scan()` method) means
there is exactly one way out of the builder, and `lazy=` reads as "give me the
same query, but as a lazy Polars plan" — the laziness is an attribute of the
collection, not a parallel API.

So the honest answer to "is it a LazyFrame?" is: **it's the auth-aware thing that
can hand you a LazyFrame** — via `query(lazy=True)` — not literally one. That
indirection is not bureaucracy; it's the two requirements you named:

- **Auth.** Deltalake dumps sit behind credentialed cloudpaths (design doc §3.4).
  A raw `LazyFrame` handed out eagerly bakes in a token that expires, with no way
  to refresh or re-credential mid-session. The credentialed `scan_delta` must be
  **minted by us at `query(...)` time** (with the `DeltalakeQueryClient`
  cached/refreshed), which means the entry point has to be our object, not Polars'.
- **Uniformity.** Keeping `mat.tables.X` the same type across sources is what makes
  "learn one API" true, and `query(lazy=True)` returning a `pl.LazyFrame`
  regardless of source extends that uniformity to the terminal. Polars is now the
  result form everywhere on `lazy=True`; the source only decides whether the
  laziness is *load-bearing* (deltalake: real pushdown to disk) or *cosmetic*
  (SQL: a lazy view over an already-fetched frame).

Net: "Polars-like objects as a major component" lands in its strongest form. The
user ends up holding actual Polars objects whenever they ask for them, and the one
surface they learned behaves identically right up to — and through — the moment
they pass `lazy=True`.

**Decision (settled): `query(lazy=True)` always returns a `pl.LazyFrame`; we do
not refuse on SQL.** The flag's contract is purely the result *form* — `lazy=True`
⇒ a Polars LazyFrame, source-agnostic — and the source only determines whether
that frame is rooted in a disk scan (deltalake, real I/O pushdown) or an in-memory
result (SQL, already fetched). This is coherent rather than dishonest: the builder
is where server pushdown happens, the returned LazyFrame is purely the
downstream-ops handoff, and a LazyFrame over an in-memory frame is an ordinary,
honest Polars construct (`pl.DataFrame.lazy()`). The one thing it does not do on
SQL is add pushdown — which is fine; non-additive laziness is still a real,
chainable Polars object, and the coherence of "`lazy=True` always means a
LazyFrame" is worth more than refusing on the source that can't accelerate it.

### Cross-engine results stay lazy too

A heterogeneous merge — an in-memory CAVE SQL result joined to a remote deltalake
table — also returns a single `LazyFrame`; it does **not** need to be a different
type. Polars composes a plan across heterogeneous roots (an in-memory root and a
`scan_delta` root) natively:

```python
keys     = sql_df["root_id"].unique()                       # planner harvests join keys
delta_lf = pl.scan_delta(url, ...).filter(pl.col("root_id").is_in(keys))
merged   = pl.from_pandas(sql_df).lazy().join(delta_lf, on="root_id")  # one LazyFrame
```

The key design point: our local-merge planner keeps owning the **semi-join
key-set pushdown** (design doc §3 Phase 3) rather than hoping Polars synthesizes
it — Polars does *static* predicate/projection pushdown automatically but won't
invent a *dynamic* "push the left keys into the right scan" filter. By injecting
the harvested keys as an explicit `is_in`, we turn that into a static predicate
Polars *does* push into the parquet read (row-group skipping). Planner and Polars
laziness compose: the planner supplies the keys, the LazyFrame carries the
filtered scan, only matching row-groups are read.

The only inherently eager seam is fetching the SQL keys — the CAVE server isn't a
lazy engine, so the keys must be known before the deltalake scan can be bounded.
That's an execution boundary, not a type problem: the SQL fetch is eager (it's the
premise), and everything on top of it stays a `LazyFrame`. (Aside, for later:
CAVE result metadata rides on pandas `.attrs`, which Polars objects don't carry —
threading that through the lazy path is a separate design point.)

### Graduation is free: SQL → deltalake with no code change

This is the property that makes the whole design pay off. Because the source kind
is resolved behind `mat.tables.<name>` and the terminal form is fixed by `lazy=`
(not by the backend), a table **graduating from CAVE SQL to a deltalake dump
requires zero user code change.** The same chain —

```python
mat.tables.my_table.filter(mat.tables.my_table.size > 100).query(lazy=True)
```

— keeps working across the migration. Before: a `LazyFrame` over an in-memory
result (cosmetic laziness, server-side filtering). After: a `scan_delta`-rooted
`LazyFrame` with real predicate/partition pushdown. Same code, same return type,
strictly better execution — the user rides the upgrade without noticing, and a
cross-engine join against that table (previous subsection) starts pushing keys to
disk the day the dump lands. The borrowed Polars surface is what makes the
migration invisible: there was never a CAVE-specific API to outgrow.

## 7. One namespace, not kinds: collapse `tables`/`views`

This is a **principle, not an optimization**: which storage backend holds a source
— a CAVE SQL table, a server view, a deltalake dump — is an obscure implementation
detail the user must never need to know to *find or query* that source. A shared
namespace is the accessor-level expression of the same backend-transparency north
star that `query()` already embodies. It would be correct even if nothing ever
migrated; graduation (§6) just makes the cost of getting it wrong concrete.

So the two accessors, `mat.tables` and `mat.views`, are the last place the API
still forces users to know a source's kind. If `query()` already dispatches on
kind, and tables/views/deltalakes are "one surface" (examples doc), then splitting
the *namespace* by kind leaks exactly the storage detail the design works to hide
— and it would force a name to *move between accessors* the day it changes backend.
So the bucket of queryable sources — tables, views, future deltalakes — should be
**one undistinguished namespace**, with kind handled by the selective dispatch we
already built into `query()`.

**Decisions (settled):**

- **`tables` broadens into the universal bucket.** `mat.tables.<name>` resolves any
  queryable source regardless of kind. Defensible naming: in a query context a view
  *is* a thing-you-query (SQL/Polars users already think of views as
  tables-you-select-from). `mat.views` is kept as a **filtered subset / deprecated
  alias** (the views-only slice) for back-compat, not as a separate world.
- **Name collisions: view-precedence + `kind=` escape.** A name present as both
  resolves to the view, matching the existing rule in `_resolve_source_kind`
  (`"view" if name in views else "table"`); `query(name, kind="table")` is the
  explicit override. Documented, not silent.

**Architecture (cheap, as predicted):** `TableManager` and `ViewManager` are already
thin `_Accessor` subclasses differing only in `_kind` and `build()`. Unify them:
one `build()` ingests both metadata sets (table metadata+schemas *and* view
metadata+schemas) and records each name with its **own** kind, column kinds, and
(table-only) reference. `_Accessor.__getitem__` stamps the *per-name* kind on the
`_Part` instead of a fixed `self._kind`; everything downstream (`TableQuery`,
column classification, reference handling) already branches on kind internally.
Per-kind classification stays where it is — `classify_table_schema` for point
structure, `classify_view_schema` for the flat schema — chosen per name at build
time. Future deltalake sources simply enter the same bucket as more names; the
accessor never grows a third sibling.

**Payoff:** graduation (§6) becomes invisible at the namespace too — a name that
moves from CAVE SQL to a view to a deltalake dump never changes where the user
finds it (`mat.tables.<name>`), never changes the chain they write, only changes
how it's served. One bucket, one surface, selective dispatch underneath.

## 8. Verdict

For the **builder surface of the pushable algebra**, we can be ~1:1 with Polars:
`scan → filter → select → join → slice → collect` reads the same, the laziness and
immutability semantics already match, and the only authoring differences are
CAVE-specific terminal kwargs (`version`/`timestamp`) and the spatial `within`
predicate that has no Polars scalar analog.

The gap is **categorical, not cosmetic**: we are a pushdown query builder over a
closed server vocabulary, not a compute engine, so `with_columns` / `group_by` /
`sort` aren't "missing methods" to add later — they're outside the algebra by
construction. The deliverable is therefore not just the matched verbs but the
**divider itself** (§5): (a) match the pushable subset exactly; (b) keep the lazy
object's surface *equal to* that subset so the push/post line is visible in the
type, not hidden in an optimizer; (c) make the predicate vocabulary the in-filter
boundary, failing richer expressions at construction; and (d) return a frame that
drops cleanly into `pl.from_pandas` so the user finishes the pipeline in real
Polars. The line is the feature — it's what stops the Polars look from promising
pushdown CAVE SQL can't do.

If we want, the smallest high-value increment is just: add `.filter()`,
`left_on/right_on/how` on `.join()`, `.limit()/.head()/.slice()`, and `&` on
filters. That alone makes the common chain indistinguishable from Polars, with no
change below the `query(list[Table], **opts)` seam.

**On timing:** the deltalake migration is the reason to commit to this surface
*now* rather than after the dumps land. A query a user writes against the closed
algebra today gets faster and cheaper for free when its source moves to a
parquet dump (§5.3), and `query(lazy=True)` (§6) turns our familiar on-ramp into a
doorway to native, fully-lazy Polars exactly as that backend becomes a major part
of the ecosystem. Because `lazy=True` already returns a `pl.LazyFrame` on every
source today (cosmetically on SQL, then load-bearing on deltalake), the same code
a user writes now upgrades from in-memory-lazy to true-pushdown-lazy with **no
rewrite** as the source migrates. Building the borrowed surface before the
migration means users learn one API once and ride the capability upgrade for free.
