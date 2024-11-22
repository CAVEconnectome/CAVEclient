---
title: Materialization
---

The Materialization client allows one to interact with the materialized
annotation tables, that were posted to the annotation service
([the annotations tutorial](./annotation.md)).

To see the entire class visit the [API doc]({{ api_paths.materialize }}).

The service regularly looks up all annotations and the segids underneath
all the boundspatialpoints. You can then query these tables to find out
the IDs that underlie the annotations, or the annotations that now
intersect with certain IDs.

For example, one common pattern is that you have identified a cell based
on the location of its cell body, and you have an annotation there.

You want to know what are the inputs onto the cell, so you first query
the annotation table with your soma annotation, asking for the current
ID underneath that soma. Then you query a synapse table for all synapse
annotations that have a post-synaptic ID equal to the ID from your soma
annotation.

In this way your code stays the same, as the proofreading changes and
you can track the connectivity of your cell over time.

## Initializing the client

By default when you initialize the overall client, it will choose the
most recent materialization version available. This may or may not be
desirable depending on your use case. If your code involves using
specific IDs then you should be using a specific version that is tied to
a timepoint where those IDs are valid.

To see what versions are available, use the
[client.materialize.get_versions()]({{ api_paths.materialize }}.get_versions) function.

```python
client.materialize.get_versions()
```

Each version has a timestamp it was run on as well as a date when it
will expire. You can query all this metadata for a specific version
using
[client.materialize.get_version_metadata()]({{ api_paths.materialize }}.get_version_metadata) or
all versions using
[client.materialize.get_versions_metadata()]({{ api_paths.materialize }}.get_versions_metadata).

To change the default version, alter the `.version` property of the
client. This will change the version for all subsequent calls which expect one, unless
you specify a different version in the method call. Note that this also sets the
`timestamp` property of the client to the timestamp of the version for methods which
expect a timestamp.

```python
client.version = 9
```

You can also specify the version when you initialize the client, e.g.:

```python
client = CAVEclient('minnie65_public', version=661)
```

Or, you can specify the version when making a particular method call.

## Browsing versions

To see what tables are available in a version you can use
[client.materialize.get_tables()]({{ api_paths.materialize }}.get_tables).

If you want to read about the description of what that table is, use the
annotationengine client
[client.materialize.get_table_metadata()]({{ api_paths.materialize }}.get_table_metadata).

If you want to read more about the schema for the annotation table use
the schema service [client.schema.schema_definition()]({{ api_paths.schema }}.schema_definition).

Note, the materialization service has a human readable webpage that
links to the other services that might be more convenient for you to
browse, to get a link there in ipython display
`client.materialize.homepage`

for some important tables, the info service has a pointer to which table
you should use in the metadata for the datastack.
`client.info.get_datastack_info()['synapse_table']` and
`client.info.get_datastack_info()['soma_table']`.

To see how many annotations are in a particular table use

```python
nannotations=client.materialize.get_annotation_count('my_table')
```

## Querying tables

To query a small table, you can just download the whole thing using
[client.materialize.query_table()]({{ api_paths.materialize }}.query_table) which will return a
dataframe of the table.

Note however, some tables, such as the synapse table might be very large
200-300 million rows and the service will only return the first 200,000
results, and not in a deterministic manner. **NOTE! This API is not
designed to enable enmass downloading of the entire synapse table there
are more efficient ways of doing this. Contact your dataset administrator
for more information if this is what you are looking to do.**

To just get a preview, use the limit argument (but note again that this
won't be a reproducible set)

```python
df=client.materialize.query_table('my_table', limit=10)
```

For most applications, you will want to filter the query in some way.

We offer seven kinds of filters you can apply: `filter_in_dict`
`filter_out_dict`, `filter_equal_dict`, `filter_greater_dict`, `filter_less_dict`,
`filter_greater_equal_dict`, and `filter_less_equal_dict`. For query_table each is
specified as a dictionary where the keys are column names, and the values are a
list of values (or single value in the case of filter_equal).

So for example to query a synapse table for all synapses onto a neuron
in flywire you would use

```python
synapse_table = client.info.get_datastack_info()['synapse_table']
df=client.materialize.query_table(synapse_table,
                                  filter_equal_dict = {'post_pt_root_id': MYID})
```

The speed of querying is affected by a number of factors, including the
size of the data. To improve the performance of results, you can reduce
the number of columns returned using `select_columns`.

So for example, if you are only interested in the root_ids and locations
of pre_synaptic terminals you might limit the query with select_columns.
Also, it is convenient to return the with positions as a column of
`np.array([x,y,z])` coordinates for many purposes. However, sometimes
you might prefer to have them split out as separate x, y, z
columns. To enable this option use `split_columns=True`.
split_columns=True is faster, as combining them is an extra step.

```python
synapse_table = client.info.get_datastack_info()['synapse_table']
df=client.materialize.query_table(synapse_table,
                                  filter_equal_dict = {'post_pt_root_id': MYID},
                                  select_columns=['id','pre_pt_root_id', 'pre_pt_position'],
                                  split_columns=True)
```

## Desired Resolution

Often you want to have position information in different units. For
example, to consider synapse locations or soma locations, you might want
to have positions in nanometers or microns.

To create neuroglancer views, you might want positions in integer voxels
of a size that aligns with the resolution you are used to using
Neuroglancer at.

Annotation tables can be created and uploaded in varying resolutions
according to whatever the user of the table felt was natural. This
information is available in the metadata for that table. In addition,
you may pass _desired_resolution_ as a keyword argument which will
automatically convert all spatial positions into voxels of that size in
nanometers.

So if you want positions in nanometers, you would pass
`desired_resolution=[1,1,1]`. If you want positions in microns you would
pass `desired_resolution=[1000,1000,1000]`. If you want positions in
4,4,40nm voxel coordinates to use with cloud-volume or neuroglancer you
would pass `desired_resolution=[4,4,40]`.

## Spatial Filters

You can also filter columns that are associated with spatial locations
based upon being within a 3d bounding box.

This is done by adding a filter_spatial_dict argument to query_table.
The units of the bounding box should be in the units of the
voxel_resolution of the table (which can be obtained from
[client.materialize.get_table_metadata()]({{ api_paths.materialize }}.get_table_metadata)).

```python
bounding_box = [[min_x, min_y, min_z], [max_x, max_y, max_z]]
synapse_table = client.info.get_datastack_info('synapse_table')
df=client.materialize.query_table(synapse_table,
                                  filter_equal_dict = {'post_pt_root_id': MYID},
                                  filter_spatial_dict = {'post_pt_position': bounding_box})
```

## Synapse Query

For synapses in particular, we have a simplified method for querying
them with a reduced syntax.
[client.materialize.synapse_query()]({{ api_paths.materialize }}.synapse_query) lets you specify pre
and post synaptic partners as keyword arguments and bounding boxes. The defaults make reasonable assumptions
about what you want to query, namely that the synapse_table is the table
that the info service advertises, and that if you specify a bounding
box, that you want the post_pt_position. These can be overridden of
course, but the above bounding box query is simplified to.

**NOTE! This API is not designed to enable enmass downloading of the
entire synapse table there are more efficient ways of doing this. Contact
your dataset administrator for more information if this is what you are
looking to do.**

```python
bounding_box = [[min_x, min_y, min_z], [max_x, max_y, max_z]]
df=client.materialize.query_table(post_ids = MYID,
                                  bounding_box=bounding_box)
```

## Live Query

In order to query the materialized tables above you can only use IDs
that were present at the timestamp of the materialization. If you query
the tables with an ID that is not valid during the time of the
materialization you will get empty results.

To check if root_ids are valid at your materialization's timestamp, you
can use
[client.chunkedgraph.is_latest_roots()]({{ api_paths.chunkedgraph }}.is_latest_roots)

```python
import numpy as np
mat_time = client.materialize.get_timestamp()
is_latest = client.chunkedgraph.is_latest_roots([MYID], timestamp=mat_time)
assert(np.all(is_latest))
```

If you need to lookup what happened to that ID, you can use the
chunkedgraph lineage tree, to look into the future or the past,
depending on your application you can use
[client.chunkedgraph.get_lineage_graph()]({{ api_paths.chunkedgraph }}.get_lineage_graph).

Again, the ideal situation is that you have an annotation in the
database which refers to your objects of interest, and querying that
table by the id column will return the object in the most recent
materialization.

However, sometimes you might be browsing and proofreadding the data and
get an ID that is more recent that the most recent version available.
For convenience, you can use
[client.materialize.live_query()]({{ api_paths.chunkedgraph }}.live_query).

to automatically update the results of your query to a time in the
future, such as now. For example, to pass now, use
`datetime.datetime.now(datetime.timezone.utc)`. Note all
timestamps are in UTC throughout the codebase.

```python
import datetime
synapse_table = client.info.get_datastack_info()['synapse_table']
df=client.materialize.live_query(synapse_table,
                                  datetime.datetime.now(datetime.timezone.utc),
                                  filter_equal_dict = {'post_pt_root_id': MYID})
```

This will raise an ValueError exception if the IDs passed in your
filters are not valid at the timestamp given

You can also pass a timestamp directly to query_table and it will call
live_query automatically.

```python
import datetime
synapse_table = client.info.get_datastack_info()['synapse_table']
df=client.materialize.query_table(synapse_table,
                                  timestamp=datetime.datetime.now(datetime.timezone.utc),
                                  filter_equal_dict = {'post_pt_root_id': MYID})
```

Also, keep in mind if you run multiple queries and at each time pass
`datetime.datetime.now(datetime.timezone.utc)`, there is no guarantee
that the IDs will be consistent from query to query, as proofreading
might be happening at any time. For larger scale analysis constraining
oneself to a materialized version will ensure consistent results.

Versions have varying expiration times in order to support the tradeoff
between recency and consistency, so before undertaking an analysis
project consider what version you want to query and what your plan will
be to update your analysis to future versions.

## Content-aware Interface (Experimental)

As of version 5.8.0, we have introduced a new interface to query tables
and views. This interface might have small but breaking changes in the
near future.
:::

In order to make the querying interface more consistent across tables,
we have introduced an additional alternative interface to filtering and
querying data via the `client.materialize.tables` object. When you
instantiate this object, this object finds all of the existing tables
and the list of their columns and lets you filter the tables as
arguments in the function with suggestions. Moreover, the filtering
arguments and the querying arguments are separated into two.

Let's see how this works with a simplest example --- downloading a
table called `nucleus_detection_v0`. First, we reference the table as a
function and then we run the query --- this is exactly the same as
`client.materialize.query_table('nucleus_detection_v0')`.

```python
client = CAVEclient('minnie65_public')
nuc_df = client.materialize.tables.nucleus_detection_v0().query()
```

Where things differ is when we add filters. If we want to query based on
a set of values for the field "id", for example, we add that as an
argument:

```python
my_ids = [373879, 111162]
nuc_df = client.materialize.tables.nucleus_detection_v0(id=my_ids).query()
```

Where in this example the `id=` queries the column `id` based on the
schema. These values can be either individual elements (i.e. an integer
or a string) or a list/array of elements, and any field can be used. The
tooling will automatically sort out how to format the filtering
appropriately when running the query. Importantly, the filtering is
identical between querying all types of tables and queries. To see the
complete list of fields that can be queried, you can tab-autocomplete or
in Jupyter or IPython glance at the docstring with
`client.materialize.tables.nucleus_detection_v0?`.

In addition to filtering by one or many values, you can also do spatial queries.
To find only annotations within a particular bounding box, you need to specify the
spatial point (e.g. `pt_position`). For each such spatial point, there will be an
argument `{spatial_point_position}_bbox` (e.g. `pt_position_bbox`) that accepts a 2x3 list
of coordinates specifying the upper and lower bounds for the query. For example, to
find all nucleus centroids within a particular bounding box, it would be:

```python
bounding_box = [[min_x, min_y, min_z], [max_x, max_y, max_z]]
nuc_df = client.materialize.tables.nucleus_detection_v0(
    pt_position_bbox=bounding_box
).query()
```

If you are not using any filters, you can omit the parenthesis and use the `get_all`
or `get_all_live` functions directly, which act similarly to the `query` and `live_query` functions
respectively.
The first example could be rewritten as:

```python
nuc_df = client.materialize.tables.nucleus_detection_v0.get_all()
```

If you want to list all available fields, you can use the `.fields` attribute.
Similarly, you can get all numeric fields with the `.numeric_fields` attribute
and all spatial fields (allowing bounding box queries) with `.spatial_fields`.

```python
nuc_df = client.materialize.tables.nucleus_detection_v0.spatial_fields
```

If you need to specify the table programmatically, you can also use a
dictionary-style approach to getting the table filtering function. For
example, an equivalent version of the above line would be:

```python
my_ids = [373879, 111162]
my_table = 'nucleus_detection_v0'
nuc_df = client.materialize.tables[my_table](id=my_ids).query()
```

The `query` function can also take arguments relating to timestamps or
formatting where they act just like in the other query method. In
particular, the arguments that apply to `query` are: `select_columns`,
`offset`, `limit`, `split_positions`, `materialization_version`,
`timestamp`, `metadata`, `desired_resolution`, and `get_counts`. For
example, to add a desired resolution and split positions in the above
query, it would look like:

```python
my_ids = [373879, 111162]
nuc_df = client.materialize.tables.nucleus_detection_v0(
    id=my_ids
).query(
    split_positions=True,
    desired_resolution=[1,1,1],
)
```

Inequalities can also be used in filtering numeric columns.
Here, you can pass a dictionary instead of a list of values, with the keys being
inequality operators (">", ">=", "<", and "<=") and the values being the comparison.
For example, to query for all nuclei with a volume greater than 1000:

```python
client.materialize.tables.nucleus_detection_v0(
    volume={">": 1000}
).query()
```

You can also use multiple inequalities in the same dictionary to filter within a range.
For example, to query for all nuclei with a volume between 500 and 750:

```python
client.materialize.tables.nucleus_detection_v0(
    volume={">": 500, "<": 750}
).query()
```

If you want to do a live query instead of a materialized query, the
filtering remains identical but we use the `live_query` function
instead. The one required argument for `live_query` is the timestamp.

```python
my_ids = [373879, 111162]
nuc_df = client.materialize.tables.nucleus_detection_v0(
    id=my_ids
).live_query(
    timestamp=datetime.datetime.now(datetime.timezone.utc),
)
```

The live query functions have similar but slightly different arguments:
`timestamp` (required), `offset`, `limit`, `split_positions`,
`metadata`, `desired_resolution`, and `allow_missing_lookups`.

Note that way that IPython handles docstrings means that while you can
use `?` to get the docstring of the filtering part of the function, you
can't simply do something like
`client.materialize.tables.nucleus_detection_v0().query?`. It will tell
you the function can't be found, because technically the `query`
function does not yet exist until the table filtering function is
called.

Instead, if you want to glimpse the docstring of the query or live_query
functions, you need to split it into two lines:

```python
qry_func = client.materialize.tables.nucleus_detection_v0().query
qry_func?
```

Finally, if the project you are working with has views, a similar
interface is available to them via `client.materialize.views`. Currently
views are not compatible with live query, and so only the `.query`
function is available.
