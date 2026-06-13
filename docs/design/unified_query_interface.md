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
the substitution is observable.

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
