ChunkedGraph
============

The ChunkedGraph client allows one to interact with the ChunkedGraph,
which stores and updates the supervoxel agglomeration graph. This is
most often useful for looking up an object root id of a supervoxel or
looking up supervoxels belonging to a root id. The ChunkedGraph client
is at ``client.chunkedgraph``.

Look up a supervoxel
^^^^^^^^^^^^^^^^^^^^

Usually in Neuroglancer, one never notices supervoxel ids, but they are
important for programmatic work. In order to look up the root id for a
location in space, one needs to use the supervoxel segmentation to get
the associated supervoxel id. The ChunkedGraph client makes this easy
using the ``get_root_ids`` method.

.. code:: ipython3

    sv_id = 104200755619042523
    client.chunkedgraph.get_root_id(supervoxel_id=sv_id)

However, as proofreading occurs, the root id that a supervoxel belongs
to can change. By default, this function returns the current state,
however one can also provide a UTC timestamp to get the root id at a
particular moment in history. This can be useful for reproducible
analysis. Note below that the root id for the same supervoxel is
different than it is now.

.. code:: ipython3

    import datetime
    
    # I looked up the UTC POSIX timestamp from a day in early 2019. 
    timestamp = datetime.datetime.utcfromtimestamp(1546595253)
    
    sv_id = 104200755619042523
    client.chunkedgraph.get_root_id(supervoxel_id=sv_id, timestamp=timestamp)

Getting supervoxels for a root id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A root id is associated with a particular agglomeration of supervoxels,
which can be found with the ``get_leaves`` method. A new root id is
generated for every new change in the chunkedgraph, so time stamps do
not apply.

.. code:: ipython3

    root_id = 648518346349541252
    client.chunkedgraph.get_leaves(root_id)
