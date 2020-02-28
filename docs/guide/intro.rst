Getting Started
===============

AnnotationFramework client is a package for simplifying interactions with HTML services associated with the DynamicAnnoationFramework.  Including

- `pychunkedgraph <https://www.github.com/seung-lab/pychunkedgraph>`_ (For tracking dynamic segmentations)
- `NeuroglancerJsonServer <https://www.github.com/seung-lab/NeuroglancerJsonServer>`_ (For posting/getting neuroglancer json states)
- `AnnotationFrameworkInfoService <https://www.github.com/seung-lab/AnnotationFrameworkInfoService>`_ (For storing dataset metadata information)
- `EmAnnotationSchemas <https://www.github.com/seung-lab/EmAnnotationSchemas>`_ (For storing an extensible set of schemas for annotating EM data)
- `AnnotatationEngine <https://www.github.com/seung-lab/AnnotationEngine>`_ (For storing annotations on EM data)


Assumptions
-----------

The code is setup to work flexibly with any deployment of these services, but you need to specify the server_address if that address is not 
https://www.dynamicannotationframework.com/ for each client when initializing it.  Similarly, the clients can query the info service for metadata
to simplify the interaction with a dataset, but you have to specify a dataset name.

Chunkedgraph
-------------

:class:`~annotationframeworkclient.chunkedgraph.ChunkedGraphClient` lets you look up root ids of supervoxel, and get lists of supervoxels assocaited with a rootID.

Example

::

    from annotationframeworkclient.chunkedgraph import ChunkedGraphClient

    cg = ChunkedGraphClient(dataset_name = 'pinky100')
    root_id = cg.get_root(94074249232266935)

Now if you wanted to access a different table on the chunkedgraph service not associated with a dataset you would need to specify that table_name,
and the dataset_name would not be relevant.

::

    from annotationframeworkclient.chunkedgraph import ChunkedGraphClient

    cg = ChunkedGraphClient(table_name = 'pinky100v16')
    root_id = cg.get_root(94074249232266935)

JsonService
-----------
Often it is useful to retreive shortened links someone has created for you as json, or create shortened links for people programatically.
This service simplifies both sides of that interaction

Example

::

    from annotationframeworkclient.jsonservice import JsonService
    js = JsonService()

    # retreive a neuroglance state
    state_d = js.get_state_json(4845531975188480)

    # change the name of the first layer
    state_d['layers'][0]['name']='newname'

    # make a new link with this new state
    newid = js.upload_state_json(state_d)

    # format a link
    link_url = build_neuroglancer_url(newid, 'https://neuromancer-seung-import.appspot.com')
    print(link_url)

AnnotationEngine
----------------

This lets you post annotations to the AnnotationEngine so they can be made available in the materializeddatabase for analysis.

Example

::

    from annotationframeworkclient.annotationengine import AnnotationClient
    ae = AnnotationClient(dataset_name='pinky100')

    # print out what tables already exist
    print(ae.get_tables())

    # make a new table
    r=ae.create_table('mytable', 'synapse')
    assert(r.status_code==200)

    # have some data to import
    df = # something to create a dataframe
    # must match the 'synapse' schema
    # '_' break up nested schemas

    # import the data
    ae.bulk_import_df('mytable', df)

    # get the first annotation back out
    ann_d = ae.get_annotation('mytable', 0)

    # post a copy of that annotation
    # as a single json annotation
    ae.post_annotation('mytable', ann_d)



