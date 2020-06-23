LookupClient
============

The LookupClient simplifies the process of looking up supervoxel ids and root ids for a list of points in space.
While this is less efficient than using the materialized database, it can be useful for small-scale scenarios like checking a few hundred annotations before uploading them to the AnnotationEngine or prototyping an analysis.

Initializing a LookupClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The LookupClient combines multiple services and can't be created from a server address and datastack name alone, unlike the single service clients.
Thus instead of being part of a FrameworkClient object, we instead use a ``client`` to initialize a LookupClient. To generate a default client, you don't need any arguments.

.. code:: python

    lookup = client.make_lookup_client()

If you need more complex features, like setting a timestamp for ``root_id`` queries, setting a segmentation mip level, or changing the default voxel resolution, these are all possible.
For example,

.. code:: python

    import datetime
    timestamp = datetime.datetime.utcfromtimestamp(1546595253)
    lookup = client.make_lookup_client(timestamp=timestamp,
                                       segmentation_mip=1,
                                       voxel_resolution=[8,8,40])

Will use the mip-1 segmentation level to map points to supervoxels, will expect points given in a resolution of 8x8x40 nm, and will query root ids at the specified timestamp.

Looking up points
^^^^^^^^^^^^^^^^^

The LookupClient links two actions: Finding the supervoxels associated with a point in space, and looking up the root ids for those supervoxels.
Supervoxel lookup uses only Cloudvolume and root id lookup uses only the ChunkedGraph. However, for simplicity we can call each here.

.. code:: python

    import numpy as np
    pts = [[1,2,3], [3,4,5]]
    supervoxel_ids = lookup.lookup_supervoxels(pts)
    root_ids = lookup.lookup_root_ids(supervoxel_ids)

However, because these two actions are often called as part of one pipeline, we can simplify the process in a single command:

.. code:: python

    root_ids, supervoxel_ids = lookup.lookup_points(pts)

Note that one can override the default mip, voxel resolution, and timestamp options for each call. See the method documentation for details.

Looking up DataFrames
^^^^^^^^^^^^^^^^^^^^^

Most of the annotations we work with in the DynamicAnnotationFramework live naturally in tabular DataFrames.
In particular, materialized point data follows a schema where each point in a spatial annotation has a location, a supervoxel id, and a root id, as well as whatever other associated metadata there may be (e.g. cell type).
To make the LookupClient produce output in a similar format, we have a handy method where you specify one or more point columns in a dataframe.

.. code:: python

    import pandas as pd
    #Intialize a dataframe
    df = pd.DataFrame(data={'cell_type':['e', 'i'], 'pt':pts})

    df_lookup = lookup.lookup_dataframe(point_column='pt', data=df)

The resulting ``df_lookup`` no longer has a column called ``pt``, but rather three new columns:

* `pt_positon` : The original point column data
* `pt_supervoxel_id` : The supervoxel id for that point
* `pt_root_id` : The root id for that point

If the segmentation is flat, `pt_supervoxel_id` is omitted since supervoxels and root ids are the same.
Each of the suffixes (``_position``, ``_supervoxel_id``, and ``_root_id``) can be set as optional parameters.
The ``point_column`` argument can also take a list of point column names if more than one point is stored per annotation.
