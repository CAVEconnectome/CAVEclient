# Query interface: example patterns

A sequential tour of `client.materialize.query()` and the table/view accessors,
from the simplest single read to a multi-table, filtered, time-addressed join.
Each step builds on the previous one, and **runs a table and a view in parallel**
so you can see exactly where the two behave identically and where they diverge in
the current implementation.

Two entry points run the same machinery:

- **The accessor** — `client.materialize.tables.<name>` / `.views.<name>` — the
  recommended path. It knows the source's columns (tab-completion, validation)
  and builds the query for you.
- **`query(...)` with `Table` specs** — the lower-level path. Write the `Table`
  objects yourself; useful for scripting or building a query programmatically.

Throughout, `client = CAVEclient("my_datastack")` and `mat = client.materialize`.
The running examples use a table `synapses_pni_2` and a view
`synapse_target_predictions`.

## Tables and views are one surface

The whole point of `query()` is that you address a **source by name** and the
client figures out the rest. You do **not** have to tell it whether a name is a
table or a view — `kind="auto"` (the default) resolves it from metadata, and the
accessors (`mat.tables` / `mat.views`) carry the kind implicitly. So
`kind="view"` is **purely optional** (it skips one metadata probe, or
disambiguates a name that somehow exists as both).

Filtering, selection, paging, sampling, output shaping, and joins are written the
**same way** for both. The differences are not syntax — they are *capabilities*,
and they surface as clear refusals rather than silent wrong answers:

| Aspect | Table | View (today) |
|---|---|---|
| Address by `version=` (frozen) | ✅ | ✅ |
| Address by `timestamp=` (live) | ✅ *if it has a bound spatial point* | ❌ refused — not live-queryable yet (Phase 3 unlocks projection/filter views via `live_compatible`; aggregating views never) |
| Stale-version fallback (`allow_version_fallback`, default on) | ✅ degrades a vanished `version=` to a live query at its timestamp — *if* the table is live-able | ❌ falls back onto the live backend, which views can't use yet — so a vanished version stays an error |
| `merge_reference` | ✅ table concept | n/a — views have no reference table (no-op) |
| Participates in joins | ✅ | ✅ — but as its own engine run, merged client-side (same result, different path) |
| `kind=` needed? | no (auto) | no (auto) — fully optional everywhere: string, accessor, single `Table` spec, and joins all resolve it from metadata |
| Column-kind inference | from the annotation schema's *point structure* — bound spatial points expand to `_position`/`_root_id`/`_supervoxel_id` with exact op legality | from the view's *flat* schema — declared types pin numeric/string/bool precisely; spatial/ID kinds inferred by name convention (no point structure), with an "any op" fallback for types the schema doesn't pin |

Each example below marks **Same** where tables and views are interchangeable and
**Diverges** where they aren't.

---

## 1. A single source, by name

The simplest query: a name as a string, at the datastack's default version.

```python
df = mat.query("synapses_pni_2")                # table
df = mat.query("synapse_target_predictions")    # view — identical call
```

**Same.** You never write `kind=`; it's auto-resolved. (`kind="view"` is allowed
and only saves the resolution probe.)

Pin a version — still identical:

```python
df = mat.query("synapses_pni_2", version=1043)
df = mat.query("synapse_target_predictions", version=1043)
```

**Diverges — live address.** A timestamp routes to the live backend:

```python
df = mat.query("synapses_pni_2", timestamp=datetime(2026, 6, 1))         # ✅ table (has bound points)
df = mat.query("synapse_target_predictions", timestamp=datetime(2026, 6, 1))  # ❌ refused, clearly
```

The view refusal is a *capability gate*, not a hardcoded "views are frozen" rule:
a table with no bound spatial point is refused for the same reason, and a view
unlocks the moment the server marks it `live_compatible` (Phase 3) — no code
change here.

---

## 2. A single source with filters

Filters are `{column: value}` keyword dicts. A list value means "in this set"; a
scalar means "equal".

```python
df = mat.query(
    "synapses_pni_2",
    filter_in={"post_pt_root_id": [864691135517653777, 864691136577830164]},
    filter_greater={"size": 100},
)

df = mat.query(
    "synapse_target_predictions",
    filter_in={"target_id": [864691135517653777, 864691136577830164]},
    filter_greater={"score": 0.9},
)
```

**Same.** The full set: `filter_in`, `filter_out`, `filter_equal`,
`filter_greater`, `filter_less`, `filter_greater_equal`, `filter_less_equal`,
`filter_spatial`, `filter_regex`. A spatial (bounding-box) filter is
`[[min_x, min_y, min_z], [max_x, max_y, max_z]]`.

*Subtle point, not a syntax difference:* both tables and views have a schema, but
of different shapes. A table's annotation schema declares *point structure* — a
`BoundSpatialPoint` field expands into `_position`/`_root_id`/`_supervoxel_id`
with exact op legality — so an illegal op (a regex on a numeric column) is caught
up front. A view's schema is *flat* (`field → type`): its declared types still
pin numeric/string/boolean kinds precisely, but there is no point structure to
read, so spatial/ID columns are recognized by name convention (`*position`,
`*root_id`, `*supervoxel_id`) and a column whose type the schema doesn't pin
falls back to "any op" (value *shape* still checked). The call you write is the
same either way.

---

## 3. The same query through the accessor (a Polars-like builder)

The accessor is a **lazy, immutable, chainable builder** shaped after Polars: each
verb returns a new builder, nothing hits the server until the terminal. The
terminal is `query()` (Polars' `collect()`); we *mirror* Polars, we are not a
drop-in for it, so the terminal keeps its CAVE name and carries CAVE kwargs
(`version=`/`timestamp=`/…). The accessor knows the source's columns, so you get
tab-completion and validation. Bind it to a variable first so completion works;
the `tables` and `views` accessors are the same object type.

```python
syn = mat.tables.synapses_pni_2
stp = mat.views.synapse_target_predictions

# .filter() is the canonical verb; keyword filters (col=, col__op=)
df = syn.filter(post_pt_root_id=[864691135517653777], size__gt=100).query()
df = stp.filter(target_id=[864691135517653777], score__gt=0.9).query()

# ...or column-handle expressions, combined with `&` (Polars/SQLAlchemy style)
df = syn.filter((syn.size > 100) & syn.post_pt_root_id.is_in([864691135517653777])).query()
df = stp.filter(stp.score > 0.9).query()
```

`syn(...)` still works as a shorthand for `syn.filter(...)`, but it is a
**deprecated alias** — prefer `.filter()`.

**Same.** Keyword operator suffixes: `__in`, `__not_in`, `__eq`, `__gt`, `__lt`,
`__ge`/`__gte`, `__le`/`__lte`, `__regex`, `__bbox`/`__within`. A bare `col=[...]`
is `in`; a bare `col=value` is `equal`. Column-handle methods: `> < >= <=`,
`==`/`!=` (scalar → equal, list → in), `.is_in()`/`.isin()`, `.notin()`,
`.is_between(lo, hi, closed="both")`, `.regex()`, and the spatial `.within()`
below. Multiple `.filter(...)` calls (and predicates within one) **AND** together.

**Spatial filtering — a CAVE-native op Polars lacks.** A position column takes a
bounding box; there is no scalar comparison on it (and no Polars equivalent — you'd
otherwise split into x/y/z and hand-write six range predicates):

```python
bbox = [[100_000, 100_000, 20_000], [110_000, 110_000, 21_000]]
df = syn.filter(syn.pre_pt_position.within(bbox)).query()       # handle form
df = syn.filter(pre_pt_position__bbox=bbox).query()             # keyword form
```

Corners may be given in any order (normalized to min-first), and `syn.pre_pt_position > 5`
is rejected up front (a position only takes a box).

**Select and slice** chain like Polars; `.select()` accepts names or handles:

```python
df = syn.select(syn.id, "size", "post_pt_root_id").filter(size__gt=100).query()
df = syn.filter(size__gt=100).head(1000).query()                # first N
df = syn.filter(size__gt=100).slice(2000, 500).query()          # offset, length
```

**The boundary is loud (out-of-algebra).** Verbs the server can't do raise with
guidance instead of silently misleading — do them after `query()`:

```python
syn.filter(size__gt=100).sort("size")        # raises: not a server op — sort the returned frame
syn.group_by("post_pt_root_id")              # raises: aggregation is a view, or group post-query
(syn.size > 100) | (syn.size < 10)           # raises: OR isn't expressible (server filters are conjunctive)
syn.size > syn.id                            # raises: can't compare a column to another column
```

**Diverges — `.live_query()`.** It exists on both (it's just `query(timestamp=)`),
but on a view it hits the same refusal as step 1:

```python
df = syn.filter(size__gt=100).live_query(datetime(2026, 6, 1))   # ✅ table
df = stp.filter(score__gt=0.9).live_query(datetime(2026, 6, 1))  # ❌ view refused
```

---

## 4. A single `Table` spec

The lower-level equivalent of steps 2–3: one `Table` object gathers a source's
name, filters, column selection, and suffix into one value.

```python
from caveclient.query import Table

df = mat.query(
    Table("synapses_pni_2",
          filter_greater={"size": 100},
          select=["id", "pre_pt_root_id", "post_pt_root_id", "size"]),
    version=1043,
)
```

**Same.** A lone view spec needs no `kind=` — like the string form and joins, a
single `Table` resolves its kind from metadata:

```python
df = mat.query(
    Table("synapse_target_predictions",
          filter_greater={"score": 0.9},
          select=["id", "target_id", "score"]),
    version=1043,
)
```

You *may* pass `Table(..., kind="view")` to skip the resolution probe (or to force
the kind), but it's optional. The `Table.kind` field defaults to `"table"`;
that default is treated as "not stated" and re-resolved, while an explicit
`kind="view"`/`"dataset"` is honored as-is.

---

## 5. A two-table join (flat pair)

A join is an **edge**: `[left, right]`. The single-join case is a flat pair of
`Table` objects, each carrying its own `join_on` column. Filters, `select`, and
`suffix` live on the `Table` they belong to.

```python
df = mat.query([
    Table("synapses_pni_2", join_on="post_pt_root_id", suffix=""),
    Table("nucleus_detection_v0", join_on="pt_root_id", suffix="_nuc"),
])
```

The accessor spelling mirrors Polars' `join`: give `on` (a shared column name or a
`(my_col, other_col)` pair) or separate `left_on`/`right_on`, plus `how`:

```python
syn = mat.tables.synapses_pni_2
nuc = mat.tables.nucleus_detection_v0

df = syn.join(nuc, on=("post_pt_root_id", "pt_root_id")).query()
df = syn.join(nuc, left_on="post_pt_root_id", right_on="pt_root_id").query()  # same thing
```

`how="inner"` is the default. `how="left"` is accepted by the API but **gated** —
it raises until the server supports left within a join (the client is built ready
to enable it without an API change). The accessor handles a **single** join today;
for more than one join use the `Table` edge list (step 7).

**Same syntax, diverging execution when a view is involved.** Swap a view into
either side and the call looks identical — the view's kind is auto-resolved, no
`kind=` needed:

```python
df = mat.query([
    Table("synapses_pni_2", join_on="post_pt_root_id"),
    Table("synapse_target_predictions", join_on="target_id", suffix="_stp"),
])
# accessor form, equally seamless:
df = syn.join(mat.views.synapse_target_predictions,
              on=("post_pt_root_id", "target_id")).query()
```

A view has no joinable SQL, so the client can't push this to a single server
join. It runs each side as its own query and **merges locally** (semi-join key
pushdown, then a pandas merge) — the *result frame is identical* to an
all-table server join. Two consequences worth knowing: the driving side should
be filtered (so it isn't fetched whole), and a *live* table↔view join refuses
cleanly at the view sub-query today.

---

## 6. Filters on a join

Each side filters independently — put the filter on the `Table` (or accessor
handle) it applies to.

```python
# table↔table
df = mat.query([
    Table("synapses_pni_2", join_on="post_pt_root_id",
          filter_greater={"size": 100}),
    Table("nucleus_detection_v0", join_on="pt_root_id", suffix="_nuc",
          filter_greater={"volume": 25}),
])

# table↔view — same shape; filters on the view side work the same
df = mat.query([
    Table("synapses_pni_2", join_on="post_pt_root_id",
          filter_greater={"size": 100}),
    Table("synapse_target_predictions", join_on="target_id", suffix="_stp",
          filter_greater={"score": 0.9}),
])
```

**Same authoring experience.** Accessor form carries each side's filters along —
filter each handle before joining:

```python
df = (syn.filter(size__gt=100)
      .join(mat.views.synapse_target_predictions.filter(score__gt=0.9),
            on=("post_pt_root_id", "target_id"))
      .query(version=1043))
```

Filtering the view side here is what keeps the local merge tractable — it bounds
the view sub-query instead of reading it whole. (Filters added *after* the join
route back to their column's origin table just the same; for inner joins that is
equivalent — the accessor never has to guess which side a filter belongs to,
because each column carries its origin.)

---

## 7. More than one join: an edge list (join graph)

Beyond a single join you give an explicit **list of edges**, which expresses any
tree — including a star, where one anchor joins two others on *different*
columns. A flat list of three-or-more tables is deliberately refused; state every
edge.

```python
# star: synapses joined to a nucleus table on the post side
#       AND to a target-prediction view on the post side
df = mat.query([
    [Table("synapses_pni_2", join_on="post_pt_root_id"),
     Table("nucleus_detection_v0", join_on="pt_root_id", suffix="_nuc")],
    [Table("synapses_pni_2", join_on="post_pt_root_id"),
     Table("synapse_target_predictions", join_on="target_id", suffix="_stp")],
])
```

**Same rule for tables and views.** `join_on` is **per-edge** (a source reappears
in each edge it joins, with that edge's column), while filters / `select` /
`suffix` are **per-table identity** and go on the source's *first* appearance; a
later appearance sets only `join_on`. The planner groups connected CAVE tables
into one server-side run and peels each view off as its own run — automatically,
from the auto-resolved kinds. You don't annotate any of that.

---

## 8. Reference merges and how `merge_reference` is handled (table-only)

**Diverges — tables only.** A table with a reference table merges it by default
(`merge_reference=True`), so the reference's columns appear in the result *and*
are filterable by the same name, off the base table, without your knowing they
live on the reference. (A reference column that collides with a base column
surfaces suffixed `_ref`, and you filter it under that name — see the
reference-transparency invariant.)

```python
prox = mat.tables.proofreading_status_public_release
df = prox.filter(valid=True).query(version=1043)   # `valid` lives on the reference table
```

### How it resolves

`merge_reference` is a **per-table** flag (set it on each `Table`, or as the
`merge_reference=` argument of the single-source string form). How it's satisfied
depends on the query shape — and you never choose; `query()` picks the cheaper
path:

- **Single table, frozen** (`version=`, no explicit join) → `query_table` merges
  the reference *itself*, server-side, with no chunkedgraph call. The cheap common
  case (the `prox` example above).
- **Any explicit join, or any live query** → `live_live_query` can't auto-merge,
  so the reference is rewritten into an explicit reference **`Join`**
  (`base.target_id == reference.id`, reference columns suffixed `_ref`) and routed
  to the live endpoint. A *versioned* explicit join runs live at the version's
  timestamp, so its references resolve as joins there too.

Either way the resulting columns are identical — the path is an internal
optimization, not something you address.

### Multiple tables sharing a reference: merged exactly once

The interesting case: a join where several participating tables reference the
**same** base table. The reference is merged **once**, not once per referrer —
and never at all if that base table is already an explicit table in the query.

```python
# two cell-type tables, both of which reference nucleus_detection_v0
df = mat.query([
    Table("mtypes_v2", join_on="target_id", suffix="_v2",
          filter_equal={"cell_type": "L2a"}),
    Table("mtypes_v1", join_on="target_id", suffix="_v1"),
], version=943)
```

Both `mtypes_v2` and `mtypes_v1` would, on their own, merge
`nucleus_detection_v0`. In the join, `query()` resolves the explicit
`mtypes_v2 ↔ mtypes_v1` join and then injects the shared reference **a single
time**:

```text
joins = [
    ["mtypes_v2", "target_id", "mtypes_v1", "target_id"],          # the explicit join
    ["mtypes_v2", "target_id", "nucleus_detection_v0", "id"],      # the reference — deduped, added once
]
```

The dedup rule (`_inject_reference_joins`): walk the tables that asked to merge,
and add each one's reference join only if that reference table isn't already
**present** — where "present" means an explicit query table *or* a reference
already injected by an earlier table. So a base table that two tables reference is
merged once; a base table you already joined explicitly is not merged again.
(Covered by `test_shared_reference_is_merged_once`.)

### Turning it off

Set `merge_reference=False` on a `Table` (or on the string-source call) to skip
resolution entirely — the reference's columns won't appear and won't be
filterable:

```python
df = mat.query(Table("proofreading_status_public_release", merge_reference=False),
               version=1043)
```

### Views

Views have no reference table, so `merge_reference` is simply a **no-op** for
them — not an error, just nothing to merge. This is the one capability that has
no view counterpart at all (rather than a deferred one).

---

## 9. Stale-version fallback (you usually get this for free)

Materialization versions churn: a pinned `version=N` can name a version whose
database has since been deleted, even though its metadata row — and therefore its
*timestamp* — survives. A query at a version's exact timestamp reconstructs
equivalent data (the server rolls root IDs forward from the nearest surviving
version), so rather than failing on a vanished version, `query()` silently
degrades to a live query at that version's timestamp.

This is on by default — every example above that passed `version=` already had it.
You only ever *see* it as a logged warning when it fires:

```python
# version 900's database is gone, but its metadata/timestamp remain
df = mat.query("synapses_pni_2", version=900)
# -> logs: "Materialization version is no longer available; falling back to a
#           live query at its timestamp (...)" and returns equivalent data
```

Turn it off to get the hard error instead (e.g. when a missing version must fail
loudly rather than degrade):

```python
df = mat.query("synapses_pni_2", version=900, allow_version_fallback=False)  # raises
```

The mechanics: if the pinned version isn't in the currently-available set but its
timestamp still resolves, the spec's address is rewritten `version=N` →
`timestamp=ts` and routed to the live backend; a *wholly* unknown version (no
metadata row) is left alone so the natural "version not found" error surfaces.
The available-version set is cached behind a short TTL, so this costs no extra
round-trip on the common (version-still-here) path.

**Diverges — it lands on the live backend, so it inherits live's limits.** The
fallback only helps sources that can be queried live:

- A **table with bound spatial points** falls back cleanly. ✅
- A **metadata-only table** (no bound point) or a **view** can't be live-queried
  today, so the fallback rewrites to a timestamp and then hits live's refusal —
  it converts "version gone" into "can't be queried live," not into data. For
  those, a vanished version is simply an error (with or without the flag).

So the fallback is, in practice, the same capability as live queries (step 1):
a clean win for live-able tables, a no-op for views until the server makes them
live-compatible.

---

## 10. A complex, multifaceted query (tables + a view together)

Everything at once: a star join that mixes tables **and** a view, each side with
its own filters and column selection, a spatial bound on the driving table,
capped and sampled, positions split into x/y/z, at a target resolution. Note
there is no `kind=` anywhere — every participant's kind is auto-resolved, and the
single view is merged in locally without you asking.

```python
from caveclient.query import Table

df = mat.query(
    [
        # driving table: large synapses within a bounding box
        [Table("synapses_pni_2",
               join_on="post_pt_root_id",
               filter_greater={"size": 150},
               filter_spatial={"post_pt_position": [[100_000, 100_000, 20_000],
                                                    [120_000, 120_000, 22_000]]},
               select=["id", "pre_pt_root_id", "post_pt_root_id",
                       "size", "post_pt_position"]),
         # ...onto cells with a confirmed nucleus  (table)
         Table("nucleus_detection_v0",
               join_on="pt_root_id", suffix="_nuc",
               filter_greater={"volume": 25},
               select=["id", "pt_root_id", "volume"])],

        # ...and with a high-confidence target prediction  (view -> local merge)
        [Table("synapses_pni_2", join_on="post_pt_root_id"),
         Table("synapse_target_predictions",
               join_on="target_id", suffix="_stp",
               filter_greater={"score": 0.9},
               select=["id", "target_id", "score"])],
    ],
    version=1043,                       # frozen: a view can't be live yet (step 1)
    limit=5000,
    random_sample=20000,                # sample the driving table before the join
    split_positions=True,               # post_pt_position -> _x/_y/_z columns
    desired_resolution=[4, 4, 40],      # report positions at this voxel resolution
)
```

Why `version=` and not `timestamp=` here: the moment a view joins in, a live
address would refuse at the view sub-query (step 1's divergence). A pure
table↔table version of this query *can* run live by passing `timestamp=` instead.

What the switchboard does with this, invisibly and identically for both kinds:

- **Routes by time** — `version=` is frozen; `timestamp=` would be live (and
  would refuse on the view).
- **Partitions the join** — connected CAVE tables become one server-side join;
  the view becomes its own run, executed and **merged client-side** with a
  semi-join key pushdown. Same result frame either way.
- **Stamps one clock** — the requested address flows to every sub-query, so every
  source resolves to the same instant by construction.
- **Shapes the output uniformly** — `split_positions`, `desired_resolution`, and
  `.attrs` metadata are applied the same regardless of which backend (or local
  merge) produced the bytes.

A query this shape — a **star** with two joins — needs the explicit `Table` edge
list: the accessor handles single joins today (its multi-join/chaining refactor is
still ahead), and the edge list is the way to author arbitrary join graphs
regardless. For the common single-join, filtered, sliced case, the accessor
(step 3) reads more like Polars; the two surfaces produce the same query and treat
tables and views identically throughout.
