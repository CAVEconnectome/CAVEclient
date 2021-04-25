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
using the :func:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClientV1.get_root_id` method.

.. code:: python

    sv_id = 104200755619042523
    client.chunkedgraph.get_root_id(supervoxel_id=sv_id)

However, as proofreading occurs, the root id that a supervoxel belongs
to can change. By default, this function returns the current state,
however one can also provide a UTC timestamp to get the root id at a
particular moment in history. This can be useful for reproducible
analysis. Note below that the root id for the same supervoxel is
different than it is now.

.. code:: python

    import datetime
    
    # I looked up the UTC POSIX timestamp from a day in early 2019. 
    timestamp = datetime.datetime.utcfromtimestamp(1546595253)
    
    sv_id = 104200755619042523
    client.chunkedgraph.get_root_id(supervoxel_id=sv_id, timestamp=timestamp)

If you are doing this across lots of supervoxels (or any nodes)
then you can do it more efficently in one request with
:func:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClientV1.get_roots`

.. code:: python

    node_ids = [104200755619042523, 104200755619042524,104200755619042525]
    root_ids = client.chunkedgraph.get_roots(node_ids)

Getting supervoxels for a root id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A root id is associated with a particular agglomeration of supervoxels,
which can be found with the :func:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClientV1.get_leaves` method. A new root id is
generated for every new change in the chunkedgraph, so time stamps do
not apply.

.. code:: python

    root_id = 648518346349541252
    client.chunkedgraph.get_leaves(root_id)

You can also query the chunkedgraph not all the way to the bottom, using the stop_layer
option

.. code:: python

    root_id = 648518346349541252
    client.chunkedgraph.get_leaves(root_id,stop_layer=2)

This will get all the level 2 IDs for this root, which correspond to the lowest chunk of the heirachy.
An analogous option exists for :func:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClientV1.get_roots`.

Other functions
^^^^^^^^^^^^^^^

There are a variety of other interesting functions to explore in the :class:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClientV1`
