# Query interface: example patterns

A sequential tour of `client.materialize.query()` and the table/view accessors,
from the simplest single-table read to a multi-table, filtered, time-addressed
join. Each step builds on the previous one.

Two entry points run the same machinery:

- **The accessor** — `client.materialize.tables.<name>` / `.views.<name>` — the
  recommended path. It knows the table's columns (tab-completion, validation) and
  builds the query for you.
- **`query(...)` with `Table` specs** — the lower-level path. Write the
  `Table` objects yourself; useful for scripting, when you don't want metadata
  fetched for completion, or when building a query programmatically.

Throughout, `client = CAVEclient("my_datastack")` and `mat = client.materialize`.

---

## 1. A single table, by name

The simplest query: a table name as a string, at the datastack's default
materialization version.

```python
df = mat.query("synapses_pni_2")
```

Pin a version, or query live at a timestamp:

```python
df = mat.query("synapses_pni_2", version=1043)
df = mat.query("synapses_pni_2", timestamp=datetime(2026, 6, 1))
```

`version` and `timestamp` are mutually exclusive — the temporal address is what
selects the backend (frozen vs. live). Neither set means "default version".

---

## 2. A single table with filters

Filters are `{column: value}` keyword dicts. A list value means "in this set";
a scalar means "equal".

```python
df = mat.query(
    "synapses_pni_2",
    filter_in={"post_pt_root_id": [864691135517653777, 864691136577830164]},
    filter_greater={"size": 100},
)
```

The full set of filter kwargs: `filter_in`, `filter_out`, `filter_equal`,
`filter_greater`, `filter_less`, `filter_greater_equal`, `filter_less_equal`,
`filter_spatial`, `filter_regex`. A spatial (bounding-box) filter is
`[[min_x, min_y, min_z], [max_x, max_y, max_z]]`:

```python
df = mat.query(
    "synapses_pni_2",
    filter_spatial={"pre_pt_position": [[100_000, 100_000, 20_000],
                                        [110_000, 110_000, 21_000]]},
)
```

---

## 3. The same query through the accessor

The accessor knows the columns, so you get tab-completion and immediate
validation (a regex on a numeric column fails right here, not as a server error).
Bind it to a variable first so completion works:

```python
syn = mat.tables.synapses_pni_2

# keyword filters: col=, col__op=
df = syn(post_pt_root_id=[864691135517653777, 864691136577830164],
         size__gt=100).query()

# or column-handle expressions, SQLAlchemy-style
df = syn(syn.size > 100,
         syn.post_pt_root_id.isin([864691135517653777])).query()
```

Operator suffixes for the keyword form: `__in`, `__not_in`, `__eq`, `__gt`,
`__lt`, `__ge`/`__gte`, `__le`/`__lte`, `__regex`, `__bbox`/`__within`. A bare
`col=[...]` is `in`; a bare `col=value` is `equal`.

Temporal address and options go on `.query()`:

```python
df = syn(size__gt=100).query(version=1043, limit=1000)
df = syn(size__gt=100).live_query(datetime(2026, 6, 1))   # alias for query(timestamp=...)
```

---

## 4. A single `Table` spec

The lower-level equivalent of step 2/3: one `Table` object gathers a table's
name, filters, column selection, and suffix into one value. `select` limits the
returned columns.

```python
from caveclient.query import Table

df = mat.query(
    Table("synapses_pni_2",
          filter_greater={"size": 100},
          select=["id", "pre_pt_root_id", "post_pt_root_id", "size"]),
    version=1043,
)
```

A lone `Table` (or string) auto-merges its reference table if it has one — see
step 9. Pass `merge_reference=False` on the `Table` to suppress that.

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

`suffix` disambiguates columns that exist in both tables; omit it to take the
positional defaults (`_x`, `_y`, ...).

The accessor spelling of the same join — `on` is `(my_col, other_col)`, or a
single string when both sides share the column name:

```python
syn = mat.tables.synapses_pni_2
nuc = mat.tables.nucleus_detection_v0

df = syn.join(nuc, on=("post_pt_root_id", "pt_root_id")).query()
```

---

## 6. Filters on a join

Each side filters independently — put the filter on the `Table` (or accessor
handle) it applies to. Here: large synapses onto neurons whose nucleus is big.

```python
df = mat.query([
    Table("synapses_pni_2", join_on="post_pt_root_id",
          filter_greater={"size": 100}),
    Table("nucleus_detection_v0", join_on="pt_root_id", suffix="_nuc",
          filter_greater={"volume": 25}),
])
```

Accessor form — `.join()` carries each side's filters along:

```python
df = (syn(size__gt=100)
      .join(nuc(volume__gt=25), on=("post_pt_root_id", "pt_root_id"))
      .query(version=1043))
```

---

## 7. A view

Views query exactly like tables — by name through `mat.query(...)`, or via the
`views` accessor. Views are version-addressed (frozen) on today's server; a
`timestamp=` query against a view is refused with a clear message.

```python
df = mat.query("synapse_target_predictions", kind="view")   # kind avoids a metadata probe
# or
df = mat.views.synapse_target_predictions(score__gt=0.9).query(version=1043)
```

---

## 8. More than one join: an edge list (join graph)

Beyond a single join you give an explicit **list of edges**, which can express
any tree — including a star, where one anchor table joins two others on
*different* columns. A flat list of three-or-more tables is deliberately refused;
state every edge.

```python
# star: synapses joined to nucleus on the post side AND to cell-types on the pre side
df = mat.query([
    [Table("synapses_pni_2", join_on="post_pt_root_id"),
     Table("nucleus_detection_v0", join_on="pt_root_id", suffix="_nuc")],
    [Table("synapses_pni_2", join_on="pre_pt_root_id"),
     Table("cell_types_v1", join_on="pt_root_id", suffix="_ct")],
])
```

Per-edge vs. per-table is the rule to remember: **`join_on` is per-edge** (a
table reappears in each edge it participates in, with the right `join_on` for
that edge), while **filters / `select` / `suffix` are per-table** and go on the
table's *first* appearance. A later appearance sets only `join_on`.

---

## 9. Reference tables are transparent

A table with a reference table merges it by default, so the reference's columns
appear in the result — and are filterable by the same name, off the annotation
table, without your knowing it lives on the reference. (If a reference column
collides with one of the base table's, it surfaces suffixed `_ref`, and you
filter it under that same name.)

```python
prox = mat.tables.proofreading_status_public_release

# `valid` lives on the reference table; you filter it as if it were local
df = prox(valid=True).query(version=1043)
```

Filtering a reference column promotes the merge to an explicit join under the
hood; an unfiltered reference still merges (frozen, cheap) so its columns show up
in the frame. No flag to set — it just mirrors what comes back.

---

## 10. A complex, multifaceted query

Everything together: a live (timestamp-addressed) star join across three tables,
each with its own filters and column selection, a spatial bound on the driving
table, capped and sampled, with positions split into x/y/z columns and a target
resolution.

```python
from caveclient.query import Table

ts = datetime(2026, 6, 1)

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
         # ...onto cells with a confirmed nucleus
         Table("nucleus_detection_v0",
               join_on="pt_root_id", suffix="_nuc",
               filter_greater={"volume": 25},
               select=["id", "pt_root_id", "volume"])],

        # ...and whose presynaptic partner is an excitatory cell
        [Table("synapses_pni_2", join_on="pre_pt_root_id"),
         Table("aibs_metamodel_celltypes_v661",
               join_on="pt_root_id", suffix="_ct",
               filter_equal={"classification_system": "excitatory_neuron"},
               select=["id", "pt_root_id", "cell_type"])],
    ],
    timestamp=ts,                       # live: resolves root IDs to this instant
    limit=5000,
    random_sample=20000,                # sample the driving table before the join
    split_positions=True,               # post_pt_position -> _x/_y/_z columns
    desired_resolution=[4, 4, 40],      # report positions at this voxel resolution
    allow_missing_lookups=True,         # tolerate not-yet-resolved supervoxels
)
```

What the switchboard does with this, invisibly:

- **Routes by time** — `timestamp=` sends it to the live backend; root IDs are
  resolved to that instant. (The same spec with `version=` would route frozen.)
- **Partitions the join** — consecutive CAVE tables become a single server-side
  join; if any participant were a *view*, it would be executed separately and
  merged locally with a semi-join key pushdown, all behind the same call.
- **Stamps one clock** — the requested `timestamp` flows to every sub-query, so
  every table resolves to the same instant by construction.
- **Shapes the output uniformly** — `split_positions`, `desired_resolution`, and
  `.attrs` metadata are applied the same way regardless of which backend served
  the bytes.

The accessor spelling of a query this shape is possible too (chained `.join()`
calls), but at this complexity the explicit `Table` edge list is usually clearer
to read and to build programmatically.
