# Design: Unified Query Interface for CAVEclient

Status: **draft / for review** — design and scope only, no implementation.
Companion doc (server side, Phase 2): `MaterializationEngine/docs/design/live_views.md`.

## 1. Motivation

`MaterializationClient` currently exposes five public query methods that do
fundamentally similar work against the same server:

| Method | Temporal address | Filter shape | Joins |
|---|---|---|---|
| `query_table` | version (delegates to `live_query` if given a `timestamp`) | flat `{col: val}` | via `merge_reference` |
| `join_query` | version | nested `{table: {col: val}}` | explicit |
| `live_query` | timestamp | flat | via `merge_reference` |
| `live_live_query` | timestamp | nested | explicit |
| `query_view` | version | flat | none |

Each method has grown its own copy of the same pipeline: resolve defaults
(`datastack_name`, `desired_resolution`, `version`), package filters, POST,
deserialize Arrow, convert resolution, assemble `.attrs` metadata, and
split/concatenate position columns. That duplication is where bugs accumulate —
the recent robustness pass found a missing `return` in `join_query`, a
`stream=~return_df` bitwise-not bug, and an `e.message` access on `HTTPError`,
each living in one copy of the pipeline but not the others.

Meanwhile `tools/table_manager.py` already *is* a switchboard — its
`query()` picks between `query_table`/`join_query`/`live_live_query` based on
whether there is a reference table and a timestamp — but that routing lives
inside a closure (`make_kwargs_mixin`) and cannot be reused, tested in
isolation, or extended.

Two upcoming capabilities make a real switchboard worthwhile:

- **Live views** (server Phase 2): views become queryable at a timestamp.
- **Deltalake archival access**: the server advertises exported Delta tables in
  cloud storage, and the client reads them directly through an authenticated
  cloudpath.

We want a single, modular entry point that routes a query to the right backend,
where adding a backend is additive rather than another copy of the pipeline.

## 2. Key insight: the schema is not very diverse

The apparent obstacle is CAVE's schema flexibility. In practice the column
*types* reduce to a small closed set, already enumerated in
`tools/table_manager.py`:

```python
ALLOW_COLUMN_TYPES   = ["integer", "boolean", "string", "float"]
NUMERIC_COLUMN_TYPES = ["integer", "float"]
SPATIAL_POINT_TYPES  = ["SpatialPoint"]   # plus BoundSpatialPoint
```

So there are **five column kinds**, and the legal operations follow from the
kind rather than being freeform:

| Kind | Expands to | Legal ops |
|---|---|---|
| Numeric (`integer`/`float`) | itself | `eq, in, gt, lt, ge, le` |
| String | itself | `eq, in, regex` |
| Boolean | itself | `eq, in` |
| BoundSpatialPoint | `{pt}_position`, `{pt}_supervoxel_id`, `{pt}_root_id` | position → `bbox`; root_id → `eq, in`; supervoxel_id → `eq, in` |
| SpatialPoint (unbound) | `{pt}_position` | `bbox` |

Only column *names* are dynamic; the query algebra is fixed and small. This lets
us model queries with concrete typed intermediates rather than choosing between
the two extremes in the codebase today — freeform dicts (the
`materializationengine.py` methods) or full per-table dynamism (the `attrs`
classes in `table_manager.py`).

A second, load-bearing consequence: **the column-kind taxonomy is also the
table-liveability predicate.** A source can be live-queried iff every row still
carries supervoxels, which is true iff it has at least one `BoundSpatialPoint`.
The same taxonomy that validates filters tells us whether a table is live-able.
(For views, liveability additionally depends on the server's `live_compatible`
flag — see §6 — because aggregation can destroy supervoxel granularity in a way
the client cannot see.)

## 3. Architecture

Three layers, with a new switchboard seam between the existing user-facing layer
and the execution layer:

```
TableManager / ViewManager            (user-facing, dynamic column completion)
        |  builds typed filters from kwargs
        v
client.materialize.query(spec)        <- the switchboard (new)
        |  routes by (source kind, temporal address, capability)
        v
QueryBackend implementations          (one module each)
   MaterializedBackend     version-addressed table / join query
   LiveBackend             timestamp-addressed; tables now, views after Phase 2
   ViewBackend             version-addressed (frozen) view query
   LiveEmulationBackend    legacy fallback (today's client-side live_query)
   DeltalakeBackend        version-addressed; local cloudpath client (Phase 3)
```

### 3.1 Column-kind taxonomy (single source of truth)

Lift the kinds out of `table_manager`'s constants and the `emannotationschemas`
field types into a new `caveclient/query/` module, and have `table_manager`
import from it. One definition drives filter legality *and* liveability, so the
dynamic per-table classes and the serializers agree by construction.

### 3.2 Typed intermediates: `Filter` and `QuerySpec`

```python
# caveclient/query/spec.py  (sketch, not final)

@dataclass(frozen=True)
class ColumnHandle:
    name: str                 # dataframe/model column name
    kind: ColumnKind
    table: Optional[str] = None   # None -> the primary source

@dataclass(frozen=True)
class Filter:
    column: ColumnHandle
    op: str                   # in / not_in / eq / gt / lt / ge / le / regex / bbox
    value: Any
    def __post_init__(self):
        # stage 1: structural validation, schema-free
        #   op in column.kind.legal_ops; value shape matches op
        #   (bbox -> 2x3; inequality -> scalar numeric; regex -> str)
        ...

@dataclass(frozen=True)
class Source:
    name: str
    kind: Literal["table", "view", "dataset", "auto"] = "auto"
    joins: Optional[list[Join]] = None
    suffixes: Optional[dict[str, str]] = None

@dataclass(frozen=True)
class At:                     # temporal address; at most one set
    version: Optional[int] = None
    timestamp: Optional[datetime] = None

@dataclass(frozen=True)
class QuerySpec:
    source: Source
    at: At
    filters: tuple[Filter, ...] = ()
    select_columns: Optional[dict[str, list[str]]] = None
    offset: Optional[int] = None
    limit: Optional[int] = None
    random_sample: Optional[int] = None
    get_counts: bool = False
    output: OutputOptions = OutputOptions()   # split_positions, desired_resolution, metadata
    def validate_against_schema(self, metadata) -> list[str]:
        # stage 2: schema-aware validation against cached table/view metadata
        #   columns exist; inequality ops target numeric columns; etc.
        ...
```

**Two-stage validation** resolves the freeform-vs-dynamic tension:

- *Structural* validation at construction (`__post_init__`) — schema-free, so a
  malformed bbox or an inequality on a non-numeric op fails in the user's stack
  frame, not as a server 500.
- *Schema* validation against cached metadata just before dispatch — the same
  check `table_manager` does dynamically today (`InvalidInequalityException`,
  `table_manager.py:587`), relocated so every entry path benefits, not just the
  `tables` interface. Errors are collected and raised together.

### 3.3 Serializers

Because the kinds are a closed set, each serializer is a total function:

- `to_server_payload(spec)` → the nine `{table: {col: val}}` dicts, with the
  per-endpoint quirks handled in exactly one place: `filter_out` → the server's
  `filter_notin_dict`; `suffixes`-list vs `suffix_map`-dict; `select_columns`
  vs `select_column_map`. These quirks (confirmed in the server's
  `schemas.py:41-107`) currently leak into every client method separately.
- `to_arrow_expr(spec)` → a pyarrow dataset filter expression for the deltalake
  backend (Phase 3).

There is no "unsupported filter" escape hatch — that is the property freeform
dicts lose. Where a *target* genuinely cannot express something (e.g. arrow
cannot do root-id-at-timestamp remapping), it surfaces as a backend capability
gate, not a serialization failure.

### 3.4 Switchboard and backend protocol

```python
class QueryBackend(Protocol):
    name: str
    def can_handle(self, spec: QuerySpec) -> Union[bool, str]:
        """True, or a string explaining why not (used to build the error)."""
    def execute(self, spec: QuerySpec) -> pd.DataFrame: ...
```

Routing is an ordered registry: the first backend whose `can_handle` returns
`True` wins. If none can, the user gets one error assembled from every backend's
reason ("LiveBackend: server 4.30 < 5.13 required; ViewBackend: views do not
support timestamp queries on this server"). That assembled message is itself a
robustness win over today's behavior, where the wrong combination surfaces as a
server 500 or an `AttributeError` on `cg_client`.

### 3.5 Shared execution pipeline

All HTTP backends share `_post_and_deserialize(url, data, query_args, spec)` and
`_postprocess(df, response_headers, spec)` — resolution conversion, `.attrs`
metadata assembly, and split/concatenate position columns. The deltalake backend
skips the first but **reuses `_postprocess` unchanged**. That shared postprocess
is the test that the seam is in the right place: position columns,
`desired_resolution`, and `.attrs` behave identically regardless of where the
bytes came from.

### 3.6 `attrs` → `dataclasses`

The new design removes the only `attrs` feature without a stdlib equivalent.
Current usage (verified):

| `attrs` feature | stdlib equivalent |
|---|---|
| `make_class(name, fields, bases=)` | `dataclasses.make_dataclass(...)` |
| `field(metadata=, default=, type=)` | `dataclasses.field(metadata=, default=)` |
| `fields(cls)` → `.name`, `.metadata` | `dataclasses.fields(cls)` |
| `asdict(self, filter=…, value_serializer=…)` | **none** |

`asdict(filter=…)` is load-bearing only for the old extraction approach —
`make_kwargs_mixin` calls it nine times (`table_manager.py:460-572`), each
scanning all fields and partitioning by metadata flags to reconstruct which
filter dict a field belongs to. With a typed `Filter` list there is nothing to
reconstruct; serialization is `groupby(op)`. The one use in `stage.py:296`
(`asdict(anno, filter=lambda a, v: v is not None)`) is a None-drop, replaceable
by a one-line comprehension over `fields()`. So we **drop the `attrs`
dependency** and port both `table_manager` and `stage` to `dataclasses`.

Caveats, all minor: on Python 3.9 the `slots=`/`kw_only=` kwargs to
`make_dataclass` are unavailable (cosmetic — skip them); field-default ordering
needs care in dynamic creation (all generated filter fields default to `None`,
so order is fine); validation moves to `__post_init__` (no validators are used
today).

## 4. Backward compatibility

The five existing methods become thin shims that build a `QuerySpec` and call
the switchboard. Signatures and return types are preserved; no deprecation in
Phase 1. `TableManager`/`ViewManager` route through the switchboard too, which
collapses the `filter_kwargs_mat`/`filter_kwargs_live` duality and the nine
`asdict` blocks in `make_kwargs_mixin`. The dynamic per-table classes survive,
demoted to ergonomics: their job shrinks to minting `ColumnHandle`s from live
metadata so column-name tab-completion still works.

## 5. Phasing

### Phase 1 — front end, current server functionality

Build the whole client architecture against what the server does **today**.

- `caveclient/query/`: column-kind taxonomy, `Filter`/`QuerySpec`, serializers.
- Backends: `MaterializedBackend`, `LiveBackend` (tables, via the live endpoint),
  `ViewBackend` (frozen views), `LiveEmulationBackend` (legacy fallback = today's
  client-side `live_query`).
- `client.materialize.query(spec | kwargs)` switchboard with capability gates.
- The five existing methods delegate; `TableManager`/`ViewManager` route through
  the switchboard.
- Remove `attrs`; port `table_manager` and `stage` to `dataclasses`.
- Views remain **mat-only**: `ViewBackend` accepts version, and a
  `(view, timestamp)` query is refused with a clear message
  ("this view does not support timestamp queries on this server").
- Deltalake: not yet (or a stub backend that always refuses with a reason).

No server changes. Existing `responses`-mocked integration tests keep passing
through the shims; new tests target the serializers (pure functions — unit-test
the nine-dict mapping including the `filter_notin_dict`/`suffix_map`/
`select_column_map` quirks) and routing (which backend, and refusal messages).

### Phase 2 — server: improve views

Implemented in MaterializationEngine (see companion doc). Partial-liveness live
views: query the closest materialized version *that contains the view*, resolve
root IDs to the timestamp, post-filter down to the queried set. Gated on the
`live_compatible` flag, which the server makes authoritative. The client is not
touched in this phase.

### Phase 3 — front end for new views + deltalake

Once the server ships Phase 2:

- Flip `LiveBackend.can_handle` to accept view sources when `live_compatible` is
  true. Routing already sends `(view, timestamp)` to `LiveBackend`; this is the
  gate flipping from "always refuse" to "refuse unless the flag is set." No new
  backend, no new public method.
- Relay the server's partial-liveness warning (which materialized version served
  the rows) into `df.attrs` so the semantics are explicit (see companion doc §7).
- Add `DeltalakeBackend`: the server advertises `{table → authenticated
  cloudpath}`; the client lazily builds and caches a `DeltalakeQueryClient`
  bound to that cloudpath, compiles the same `Filter` list via `to_arrow_expr()`,
  reads with predicate/partition pushdown, and runs the **same `_postprocess`**.
  Deltalake is version-addressed (exports come from frozen DBs, no time travel),
  so a `timestamp=` query is simply not `can_handle`-able there — correct, not a
  missing feature. Auth split (bearer vs cloudpath credentials) is itself a
  `can_handle` check, so "lake advertised but no credentials" fails in the user's
  frame with a clear message.
- Optionally retire `LiveEmulationBackend` once the minimum supported server
  version has the live endpoint.

### Phase 3 — cross-engine local merge (heterogeneous joins)

Some joins can never be a single server call: a table joined to a view (no
shared SQL), or — the case that actually motivates this — a deltalake archive
joined to a live server table. The decomposition unit is therefore not "table
vs view" but the **execution engine**. The switchboard graduates from "route the
whole spec to one backend" to "**partition** the spec into maximal single-engine
sub-specs, execute each natively, and **merge locally**." The merge planner never
learns whether a frame arrived over HTTP or from a parquet read; table+view is
just the trivial in-process instance of the deltalake-vs-server mechanism.

This rests on a small **backend contract** — three capabilities, after which the
planner is engine-agnostic:

1. **Advertise** — `can_handle(sub_spec, At) -> True | reason` (the existing
   gate, evaluated per sub-spec instead of per whole spec).
2. **Execute** — `(sub_spec, At) -> DataFrame`.
3. **Key-set pushdown** — accept `join_key ∈ {…}` as a filter. This is the only
   new requirement and it is what makes the merge tractable: run the selective
   side, harvest its join-key values, push them into the other engine's sub-spec,
   then merge. Without it both sides are fetched whole — does not scale.

Given those, the **planner is purely structural**: partition by engine → order by
selectivity → semi-join key pushdown → pandas merge, the client owning the
`_x`/`_y`/`_ref` suffixing so the output frame is identical to a server-side join.
It always prefers a single-backend route when one covers the whole spec (the
switchboard already does); local merge is strictly the **fallback**, never an
optimizer, because a local join forfeits the server's set operations.

**Temporal alignment is not a planner concern.** Versions and timestamps are a
*global* clock in CAVE: a materialization version denotes one fixed instant, and
a deltalake export is a dump *of* such a version, so every engine resolves the
same `At` to the same data by construction. The planner stamps every sub-spec
with the one requested `At` and hands it down; each backend resolves it natively
(temporal resolution and view partial-liveness both live *inside* the backend,
invisible to the planner). The only residual is **coverage, not time**: if no
engine can serve some source at the requested `At` (version not dumped to the
lake, view did not yet exist), the join is unroutable — the same
`UnroutableQueryError`, assembled from per-source reasons. Nothing to reconcile.

**Status: implemented for table↔view (frozen), `caveclient/query/merge.py`.** A
join is detected as heterogeneous when a participant resolves to a view. The
planner partitions the chain into **engine runs** (`plan_segments`): consecutive
CAVE tables are grouped into a single server-side join (so a reference table —
itself an annotation+reference merge — joined to a view is *one* `query_table`
plus *one* `query_view`), while each view stands alone (a view is one query,
always). Each run is an ordinary `query()` call (so the switchboard routes it on
its own), and the pure `local_merge_query` planner does the semi-join pushdown
across run boundaries and the inner merge. A run's boundary key may come back
suffixed by the server's intra-run `_x`/`_y` disambiguation, so the planner
resolves it by probing the returned frame (`_resolve_key`); cross-*run* collisions
are suffixed by the planner. Because the
planner passes the requested `At` straight through, **live table↔view joins come
for free the moment the server makes views live-queryable** (Phase 2 flips the
view backend's timestamp gate) — no planner change; today a live table+view join
refuses cleanly at the view sub-query. This is *morally equivalent* to a
server-side version anyway: a view is fully-formed SQL the join planner can't see
into, so even server-side the join can't be pushed into the view — the server
would also evaluate each side and stitch them (the partial-liveness recipe). The
client doing it gives up no join the server could actually have done better.
Caveat: the driving (first) source should be bounded by its own filters — `limit`
applies to the merged result, so an unfiltered driving source is fetched whole.

### The join model: chain (flat list) vs graph (edge list)

The atomic join is **pairwise** — `Join(left_table, left_col, right_table,
right_col)` — and a query holds a tuple of them on `Source.joins`. The user-facing
rule is a single one: **a join is an edge `[left, right]`, given as a list of
edges.**

- **A single join** is one edge, and a flat pair is sugar for it:
  `query([Table(A, join_on=a), Table(B, join_on=b)])` == `query([[Table(A, ...),
  Table(B, ...)]])`. (Implementation: a flat list of two `Table`s is just wrapped
  one level into a single-edge list.)
- **More than one join** is an explicit edge list — `query([[Table(A, join_on=a),
  Table(alpha, join_on=x)], [Table(A, join_on=b), Table(beta, join_on=y)]])` — a
  **general join graph**. A table may appear in several edges with a *different*
  `join_on` per edge, which is what expresses a star (one anchor joined to two
  tables on different columns) or any tree topology.

There is deliberately **no implicit chain**: a flat list of three-or-more `Table`s
is refused with a message pointing at the edge-list form. The old "adjacent tables
are joined, each with one `join_on` hinge" shorthand was a constrained subset
(it couldn't express a star) that cost more to explain than it saved, so the only
multi-join form is the explicit edge list. One rule, no caveats: you state every
join, but you don't build lists for a single source.

Per-edge vs per-table is the crux: **`join_on` is per-edge** (which is why
repeating a table with a different `join_on` is correct), while **filters /
`select` / `suffix` are per-table identity** — a table is one node in the result
regardless of how many edges touch it. So the rule is: a table's
filters/select/suffix go on its **first** appearance; a later appearance may set
only `join_on`, and setting filters/select/suffix on a repeat is a hard error
(`build_query_spec_from_edges`). Suffixes are assigned per *distinct* table,
positionally. (A self-join — the *same* table as two distinct result nodes with
different suffixes — is therefore not expressible yet; it needs aliasing, and the
repeat-must-be-bare rule refuses it cleanly.)

### Join type (inner / left) — anticipated

Today every join is **inner**. The one other mode anticipated is **left** (keep
all rows of the left/driving side, null the joined side). It is a **per-edge**
property — each join independently inner or left — modeled cleanest as a
`how="inner"|"left"` field on the *joined* (right) `Table`: in a tree, each node
is the right side of exactly one edge (its parent edge), so "how is this table
attached to its parent" is unambiguous. Default `inner`. It propagates down the
tree (a child left-joined keeps its parent-subtree rows and nulls its own
columns). Touch points when it lands:

- **Cross-engine (local merge):** nearly free — `_merge_tree` threads the edge's
  `how` into the pandas `merge(how=...)` (currently hardcoded `"inner"`).
- **Server-side (within a CAVE run):** the gating dependency — a left join
  between two CAVE tables in one `live_live_query` needs the **server's** join
  endpoint to support left/outer. Until then a left join *within* a CAVE run
  isn't expressible; a left join *across* runs (the local-merge seam) would be.
- **Two correctness notes.** (1) The semi-join pushdown stays valid for left:
  restricting the child query to the parent's keys still only fetches child rows
  that could match, and non-matching parent rows survive as nulls — no change
  needed. (2) The empty/short-circuit *does* change: today "parent yields no keys
  → whole result empty" is an inner-only shortcut; under a left join no child keys
  means *all parent rows with nulls*, so that `break` (`graph_merge_query`) must be
  conditioned on `how`. That is the single spot where left isn't a drop-in.

Deliberately not half-built: a left join that worked across runs but not within a
CAVE run would be exactly the "fails for an obscure reason" inconsistency this
design avoids, so it lands once the server's CAVE-side left is settled and both
paths behave identically.

**Why this matters for the cross-engine future:** the edge list is the planner's
native input. Cross-engine joins are graphs, not chains (a deltalake table joined
to two server tables on different columns), and an edge that *crosses* an engine
boundary is exactly a local-merge seam whose two columns are the semi-join
pushdown keys. So the edge list hands the planner its cut points directly.

The local-merge planner (`caveclient/query/merge.py`) is **graph-aware**:
`plan_runs` partitions the graph into engine runs (CAVE tables connected by
CAVE–CAVE edges become one server-side join via union-find; each view is its own
run), `build_run_tree` requires the cross-run edges to form a single connected
**tree** (cycles and disconnected graphs are refused with a clear reason), and
`graph_merge_query` executes runs parent-before-child with the semi-join pushdown
then inner-merges over the tree. A view anywhere in a join now works (it becomes
its own run); the all-CAVE connected case is a single run and goes straight to
`live_live_query`. The remaining cross-engine piece is purely the **deltalake
backend** — once it exists it participates for free, since the planner is
engine-agnostic.

## 6. Liveability as a capability gate (not a hardcoded rule)

Liveability is a property of **supervoxel availability at row granularity**, not
of "table vs view". The routing must refuse-with-reason on a capability, never
hardcode "views are mat-only":

- **Table with a BoundSpatialPoint** → has supervoxels → live-able (derivable
  client-side from the taxonomy).
- **Table with no bound point** (metadata-only) → not live-able, same reason as
  an aggregating view.
- **Projection/filter/join view** → supervoxels survive → live-able; the server
  sets `live_compatible=true`.
- **GROUP BY / aggregating view** → supervoxels collapsed → not live-able;
  `live_compatible=false`.

The client cannot see a view's SQL, so for views `live_compatible` is
authoritative; for tables the same property is derived from the taxonomy. Both
encode the identical underlying fact, so `can_handle` reads as one coherent
check. When the server flips a view's flag to true (Phase 2), the client unlocks
it with no code change beyond the gate already described.

## 7. Graceful degradation: stale version fallback

Materialization versions churn — a pinned `version=N` may name a version whose
`{datastack}__mat{N}` database has since been deleted, even though its
`AnalysisVersion` metadata row (and therefore its timestamp) survives.
`client.materialize.get_timestamp(version=N)` resolves that timestamp even for
expired versions, because it reads `get_version_metadata` (the persistent
metadata row), not the deleted database.

Because a live query at a version's *exact* timestamp reconstructs equivalent
data — the server short-circuits a timestamp that matches a version's frozen
time, and otherwise rolls root IDs forward from the nearest surviving version —
a stale version query can silently degrade instead of failing.

The switchboard does this as a dispatch-time normalization, implemented as the
pure `resolve_version_fallback(spec, available_versions, timestamp_lookup)`:

- If the spec pins a version that is **not** in the currently-available set but
  `get_timestamp` resolves it, rewrite `At(version=N)` → `At(timestamp=ts)` (so
  the query routes to the live backend) and emit a warning.
- If the version is wholly unknown (no metadata row, `get_timestamp` yields
  nothing), leave the spec untouched so the natural "version not found" error
  surfaces.

This is a behavior toggle on `query()` (default on). It is silent in the sense
of not raising, but it logs a warning and records the fallback in `df.attrs` so
the substitution is observable. The available-version set is cached
(`_available_version_set`, short TTL) so the fallback check does not add a
`get_versions` round-trip to every pinned query.

### Only two server methods; joins and references via the live path

The client delegates to exactly two server methods: **`query_table`** for
single-table frozen queries (the fast `simple_query` path) and
**`live_live_query`** for everything live or joined. `join_query` is never used
(its frozen endpoint only joins two tables — `common.py:485` — and is otherwise
subsumed) and `live_query` is never used (its client-side emulation updates root
IDs but not the row set, so it silently diverges when tables are edited).

`query_table`'s only join capability is the automatic single reference-table
merge, which it performs *frozen* (no chunkedgraph client). So a **single table
plus its reference**, queried by version, stays on the fast `query_table` path.
**Explicit joins** (and anything live) go to `live_live_query`: because it at a
version's exact timestamp reproduces that frozen version, a versioned explicit
join is converted to a query at `get_timestamp(version)` before dispatch. Joins
are typed `Join(left_table, left_column, right_table, right_column)` serialized
to the live endpoint's `[[t1, c1, t2, c2], ...]` form, which handles N joins.
(Consequence: an *explicit* join requires a chunkedgraph client and the live
endpoint; a single table — with or without a reference merge — keeps the fast
frozen `query_table` path.)

### Reference merges as deduped joins

`merge_reference` (per-`Table`, default True) has two resolutions depending on
the query shape:

- **Single table, frozen** → `query_table` does the merge itself (frozen, no
  chunkedgraph), the cheap common case.
- **Explicit join, or live** → `query()` resolves references into explicit
  reference `Join`s (`table.target_id == reference_table.id`, reference columns
  suffixed `_ref`), reusing the cached `_resolve_merge_reference`, and routes to
  `live_live_query`. References are **deduped**: if several tables in a join
  reference the same base table, it is merged exactly once (and never if it is
  already an explicit table in the query).

## 8. Open decisions

1. **Taxonomy home** — proposed: `caveclient/query/` is the single definition,
   `table_manager` imports it. (Leaning yes.)
2. **`Filter`/`QuerySpec` public?** — leaning public-but-thin: power users can
   build a spec directly (useful for the deltalake-direct case and for testing),
   and the closed-kind validation makes it safe to accept. Commits us to an API
   surface.
3. **Deltalake dependency** — direct dep on `deltalake`/`pyarrow` as a
   `caveclient[delta]` extra (true client-direct reads) vs waiting for a
   server-side read/redirect endpoint. The advertise + cloudpath model is usable
   today but bundles lake-reading and its credential story into the client.

## 9. Testing strategy

- Serializers are pure → exhaustive unit tests over `(kind, op)` pairs and the
  payload-key quirks; arrow-expression tests for deltalake.
- Routing tests: assert which backend handles each `(source kind, temporal
  address, server version)` combination, and assert the assembled refusal
  message when none can.
- Backward-compat: existing `responses`-mocked integration tests must pass
  unchanged through the shims.
- Continuity with the robustness pass: keep all responses funneled through
  `handle_response`; a lint-style guard against raw `.json()` calls.
