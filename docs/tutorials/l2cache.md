---
title: Level 2 Cache
---

To understand the level 2 cache, you must understand the structure of
the chunkedgraph so see [the chunkedgraph tutorial](chunkedgraph.md).

Nodes on the second level or layer of the graph, corresponds to all the
supervoxels that are locally connected to one another within a single
level 2 spatial "chunk" of the data. The Level 2 Cache, is a service
whose job it is to track and update relevant statistics about every
level 2 node within the a chunkedgraph. The source code of this service
can be found [here](https://github.com/CAVEconnectome/PCGL2Cache).

## Finding Level 2 Nodes

The chunkedgraph can be used to find the level2 nodes of a rootID using
a `stop_layer=2` keyword argument on the
[client.chunkedgraph.get_leaves()]({{ api_paths.chunkedgraph }}.get_leaves).
Conversely the level 2 node of a supervoxel can be found using the same keyword argument of
[client.chunkedgraph.get_roots()]({{ api_paths.chunkedgraph }}.get_roots).
Note if you don't specify a timestamp it will give you the level2 node that is
presently associated with the object.

## Statistics

The statistics that are available are:

- **area_nm2:** The surface area of the object in square nanometers. Does not include border touching voxels
- **size_nm3:** The volume of the object in cubic nanometers,
  based on counting voxels in the object.
- **max_dt_nm:** The maximum edge distance transform of that
  object in nanometers. Meant to capture the
  maximum "thickness" of the voxels in the
  node.
- **mean_dt_nm:** The average edge distance transform of that
  object in nanometers. Meant to capture the
  average "thickness" of voxels in that node.
- **rep_coord_nm:** A list of x,y,z coordinates in nanometers that
  represent a point within the object that is
  designed to be close to the "center" of the
  object. This is the location of the max_dt_nm
  value.
- **chunk_intersect_count:** A 2 x 3 matrix representing the 6 sides of the
  chunk, and whose values represent how many
  voxels border that side of the chunk. Meant to
  help understand significant the borders with
  other chunks are. Ordering is the [[x_bottom,
y_bottom, z_bottom],[x_top, y_top, z_top]]
  where {xyz}\_bottom refers to the face which
  has the smallest values for that dimension, and
  {xyz}\_top refers to the face which has the
  largest.
- **pca** A 3x3 matrix representing the principal
  components of the xyz point cloud of voxels for
  this object. Ordering is NxD where N is the
  components and D are the xyz dimensions. Meant
  to help desribe the orientation of the level 2
  chunk. Note that this is not calculated for
  very small objects and so might not be present
  for all level 2 nodes. You will see that its
  availability correlates strongly with size_nm3.
- **pca_val** The 3 principal component values for the PCA
  components.

## Retrieving Level 2 Statistics

Level 2 stats about nodes can be retreived using the
[client.l2cache.get_l2data()]({{ api_paths.l2cache }}.get_l2data) method. It simply takes a list of level 2 nodes you want to
retrieve. Optionally you can specify only the attributes that you are
interested in retrieving which will speed up the request.

## Missing Data

The service is constantly watching for changes made to objects and
recalculating stats on new level2 nodes that are created, in order to
keep its database of statistics current. This however takes some time,
and is subject to sporadic rare failures. If you request stats on a
level 2 node which are not in the database, you will receive an empty
dictionary for that node. This will immediately trigger the system to
recalculate the statistics of that missing data, and so it should be
available shortly (on the order of seconds) if systems are operational.
Please note that PCA is not calculated for very small objects because it
is not meaningful. So if you are interested in differentiating whether
PCA is not available because it hasn't been calculated, vs when its not
available because it is not possible to calculate, you should ask for at
least one other non PCA statistic as well. You will see that its
availability correlates strongly with `size_nm3`.

## Use Cases

### Calculate Total Area and Volume of Cells

Say you want to calculate the total surface area and volume of a object
in the dataset. The areas and volume of each component can simply be
added together to do this.

```python
import pandas as pd
root_id = 648518346349541252
lvl2nodes = client.chunkedgraph.get_leaves(root_id,stop_layer=2)
l2stats = client.l2cache.get_l2data(lvl2nodes, attributes=['size_nm3','area_nm2'])
l2df = pd.DataFrame(l2stats).T
total_area_um2=l2df.area_nm2.sum()/(1000*1000)
total_volume_um3 = l2df.size_nm3.sum()/(1000*1000*1000)
```

By utilizing the bounds argument of get_leaves, you can also do simple
spatially restricted analysis of objects. In fact, because you have data
on each level2 node individually, you can segregate the neuron using any
labelling of its topology.

### Skeletonization

Level 2 nodes have "cross chunk" edges within the chunkedgraph which
represent what level 2 nodes that object is locally connected to. This
forms a graph between the level 2 nodes of the object that can be
retrieved using the chunkedgraph function
[client.chunkedgraph]({{ api_paths.chunkedgraph }}.level2_chunk_graph). This graph represents a topological representation of the
neuron at the resolution of individual chunks, and is guaranteed to be
fully connected, unlike a voxel or mesh representation of the neuron
which can have gaps where there are defects in the segmentation volume
or incorrectly inferred edges at self contact locations.

The level 2 graph can be turned into a skeleton representation of a
neuron using a graph based TEASAR like algorithm as described for
skeletonizing meshes in this [MeshParty
Documentation](https://meshparty.readthedocs.io/en/latest/guide/skeletons.html).
There is an implementation of this approach that utilizes the
chunkedgraph and the L2cache if available
[here](https://github.com/AllenInstitute/pcg_skel) and on pypi as
`pcg-skel`. In this implementation the l2cache is used to more
accurately place the level 2 nodes in space using the `rep_coord_nm`
value.

Note that there is detailed documentation on the Skeleton Client interface at [the skeletonization tutorial]({{ tutorial_paths.skeleton }}).

### Trajectory Distributions

If one is interested in the bulk direction of processes in a region of
the brain, one can start with supervoxels in a region, find level 2
nodes that correspond to them, filter out components based on size, (or
other criteria such as whether they are part of objects that have
components in some other brain area) and look at the distribution of PCA
components to understand the directions that those processes are moving
within that region of space.
