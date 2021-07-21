Materialization
================

The Materialization client allows one to interact with the materialized
annotation tables, that were posted to the annotation service (see 
:doc:`annotation`). 

To see the entire class visit the API doc :class:`~caveclient.materializationengine.MaterializatonClientV2`

The service regularly looks up all annotations and the segids underneath
all the boundspatialpoints. You can then query these tables to find out
the IDs that underlie the annotations, or the annotations that now intersect
with certain IDs.

For example, one common pattern is that you have idenfied a cell based on
the location of its cell body, and you have an annotation there.

You want to know what are the inputs onto the cell, so you first query the 
annotation table with your soma annotation, asking for the current ID underneath
that soma.  Then you query a synapse table for all synapse annotations that 
have a post-synaptic ID equal to the ID from your soma annotation. 

In this way your code stays the same, as the proofreading changes and you can
track the connectivity of your cell over time.

Initializing the client
^^^^^^^^^^^^^^^^^^^^^^^
By default when you initialize the overall client, it will choose the most recent
materialization version available.  This may or may not be desirable depending on your
use case.  If your code involves using specific IDs then you should be using a 
specific version that is tied to a timepoint where those IDs are valid.

To see what versions are available, use the :func:`~caveclient.materializationengine.MaterializatonClientV2.get_versions`

.. code:: python

    client.materialize.get_versions()

Each version has a timestamp it was run on as well as a date when it will expire.
You can query all this metadata for a specific version  using 
:func:`~caveclient.materializationengine.MaterializatonClientV2.get_version_metadata`
or all versions using
:func:`~caveclient.materializationengine.MaterializatonClientV2.get_versions_metadata`


To change the default version, alter the .version property of the materialization client.

.. code:: python

    client.materialize.version = 9

or specify the version when making a particular call.

Browsing versions
^^^^^^^^^^^^^^^^^
To see what tables are available in a version you can use 
:func:`~caveclient.materializationengine.MaterializatonClientV2.get_tables`

If you want to read about the description of what that table is, use the annotationengine client
:func:`~caveclient.annotationengine.AnnotationClientV2.get_table_metadata`

If you want to read more about the schema for the annotation table use the schema service
:func:`~caveclient.emannotationschemas.SchemaClientLegacy.schema_definition`

Note, the materialization service has a human readable webpage that links to the other services
that might be more convienent for you to browse,
to get a link there in ipython display ``client.materialize.homepage``

for some important tables, the info service has a pointer to which table you should use in 
the metadata for the datastack.  ```client.info.get_datastack_info()['synapse_table']```
and ```client.info.get_datastack_info()['soma_table']```.

To see how many annotations are in a particular table use

.. code:: python

    nannotations=client.materialize.get_annotation_count('my_table')

Querying tables
^^^^^^^^^^^^^^^
To query a small table, you can just download the whole thing using  
:func:`~caveclient.materializationengine.MaterializatonClientV2.query_table`
which will return a dataframe of the table.

Note however, some tables, such as the synapse table might be very large 200-300 million rows
and the service will only return the first 200,000 results.

To just get a preview, use the limit argument

.. code:: python

    df=client.materialize.query_table('my_table', limit=10)

For many applications, you will want to filter the query in some way.

We offer three kinds of filters you can apply:  filter_equal, filter_in and filter_not_in. 
For query_table each is specified as a dictionary where the keys are column names, 
and the values are a list of values (or single value in the case of filter_equal). 

So for example to query a synapse table for all synapses onto a neuron in flywire you would use

.. code:: python

    synapse_table = client.info.get_datastack_info('synapse_table')
    df=client.materialize.query_table(synapse_table,
                                      filter_equal_dict = {'post_pt_root_id': MYID})


The speed of querying is affected by a number of factors, including the size of the data. 
To improve the performance of results, you can reduce the number of columns returned using
select_colums. 

So for example, if you are only interested in the root_ids and locations of pre_synaptic terminals
you might limit the query with select_columns.  Also, it is convient to return the 
with positions as a column of np.array([x,y,z]) coordinates for many purposes.
However, sometimes you might prefer to have them split out as seperate _x, _y, _z columns.
To enable this option use split_columns=True. split_columns=True is faster, as combining them is an extra step.  
You can recombine split-out position columns using :func:`~caveclient.materializationengine.concatenate_position_columns`

.. code:: python

    synapse_table = client.info.get_datastack_info('synapse_table')
    df=client.materialize.query_table(synapse_table,
                                      filter_equal_dict = {'post_pt_root_id': MYID},
                                      select_columns=['id','pre_pt_root_id', 'pre_pt_position'],
                                      split_columns=True)

Spatial Filters
^^^^^^^^^^^^^^^
You can also filter columns that are associated with spatial locations based upon being within a 3d bounding box.

This is done by adding a filter_spatial_dict argument to query_table.
The units of the bounding box should be in the units of the voxel_resolution of the table 
(which can be obtained from :func:`~caveclient.materializationengine.MaterializatonClientV2.get_table_metadata`).


.. code:: python

    bounding_box = [[min_x, min_y, min_z], [max_x, max_y, max_z]]
    synapse_table = client.info.get_datastack_info('synapse_table')
    df=client.materialize.query_table(synapse_table,
                                      filter_equal_dict = {'post_pt_root_id': MYID},
                                      filter_spatial_dict = {'post_pt_position': bounding_box})


Synapse Query
^^^^^^^^^^^^^
For synapses in particular, we have a simplified method for querying them with a reduced syntax.
:func:`~caveclient.materializationengine.MaterializatonClientV2.synapse_query` 
lets you specify pre and post synaptic partners as keyword arguments and bounding boxes.
The defaults make reasonable assumptions about what you want to query, namely that the synapse_table is
the table that the info service advertises, and that if you specify a bounding box, that you want the post_pt_position. 
These can be overridden of course, but the above bounding box query is simplified to.

.. code:: python

    bounding_box = [[min_x, min_y, min_z], [max_x, max_y, max_z]]
    df=client.materialize.query_table(post_ids = MYID,
                                      bounding_box=bounding_box)


Live Query
^^^^^^^^^^
In order to query the materialized tables above you can only use IDs that were present at the 
timestamp of the materialization.  If you query the tables with an ID that is not valid during the 
time of the materialization you will get empty results. 

To check if root_ids are valid at your materialization's timestamp, you can use 
:func:`~caveclient.chunkedgraph.ChunkedGraphClientV1.is_latest_roots`

.. code:: python

    import numpy as np
    mat_time = client.materialize.get_timestamp()
    is_latest = client.chunkedgraph.is_latest_roots([MYID], timestamp=mat_time)
    assert(np.all(is_latest))


If you need to lookup what happened to that ID, you can use the chunkedgraph lineage tree,
to look into the future or the past, depending on your application you can use
:func:`~caveclient.chunkedgraph.ChunkedGraphClientV1.get_lineage_graph`

Again, the ideal situation is that you have an annotation in the database which refers 
to your objects of interest, and querying that table by the id column will return the 
object in the most recent materialization.

However, sometimes you might be browsing and proofreadding the data and get an ID
that is more recent that the most recent version available.  For convience, you can use 
:func:`~caveclient.materializationengine.MaterializatonClientV2.live_query`.


to automatically update the results of your query to a time in the future, such as now.
For example, to pass now, use ```datetime.datetime.utcnow```.  Note all timestamps are in UTC
throughout the codebase. 

.. code:: python

    import datetime
    synapse_table = client.info.get_datastack_info('synapse_table')
    df=client.materialize.live_query(synapse_table,
                                      datetime.datetime.utcnow(),
                                      filter_equal_dict = {'post_pt_root_id': MYID})

This will raise an ValueError exception if the IDs passed in your filters are not valid at the timestamp given

You can also pass a timestamp directly to query_table and it will call live_query automatically. 

.. code:: python

    import datetime
    synapse_table = client.info.get_datastack_info('synapse_table')
    df=client.materialize.query_table(synapse_table,
                                      timestamp=datetime.datetime.utcnow(),
                                      filter_equal_dict = {'post_pt_root_id': MYID})


Also, keep in mind if you run multiple queries and at each time pass ``datetime.datetime.utcnow()``,
there is no gauruntee that the IDs will be consistent from query to query, as proofreading might be happening
at any time.  For larger scale analysis constraining oneself to a materialized version will ensure consistent results.

Versions have varying expiration times in order to support the tradeoff between recency and consistency, 
so before undertakin an analysis project consider what version you want to query and what your plan will be to 
update your analysis to future versions. 



