AnnotationEngine
================

The AnnotationClient is used to interact with the AnnotationEngine
service to create tables from existing schema, upload new data, and
download existing annotations. Note that annotations in the
AnnotationEngine are not linked to any particular segmentation, and thus
do not include any root ids. An annotation client is accessed with
``client.annotation``.

Getting existing tables
^^^^^^^^^^^^^^^^^^^^^^^

A list of the existing tables for the datastack can be found at with
``get_tables``.

.. code:: python

    all_tables = client.annotation.get_tables()
    all_tables[0]

Each table has three main properties that can be useful to know: 

* ``table_name`` : The table name, used to refer to it when uploading or downloading annotations. This is also passed through to the table in the Materialized database.
* ``schema_name`` : The name of the table’s schema from EMAnnotationSchemas (see below).
* ``max_annotation_id`` : An upper limit on the number of annotations already contained in the table.

Downloading annotations
^^^^^^^^^^^^^^^^^^^^^^^

You can download the JSON representation of a data point through the
``get_annotation`` method. This can be useful if you need to look up
information on unmaterialized data, or to see what a properly templated
annotation looks like.

.. code:: python

    table_name = all_tables[0]['table_name']      # 'ais_analysis_soma'
    annotation_id = 100
    client.annotation.get_annotation(annotation_id=annotation_id, table_name=table_name)

Create a new table
^^^^^^^^^^^^^^^^^^

One can create a new table with a specified schema with the
``create_table`` method:

.. code:: python

   client.annotation.create_table(table_name='test_table',
                                  schema_name='microns_func_coreg')


New data can be generated as a dict or list of dicts following the
schema and uploaded with ``post_annotation``. For example, a
``microns_func_coreg`` point needs to have: \* ``type`` set to
``microns_func_coreg`` \* ``pt`` set to a dict with ``position`` as a
key and the xyz location as a value. \* ``func_id`` set to an integer.

The following could would create a new annotation and then upload it to the service. Note that you get back the annotation id(s) of what you uploaded.

.. code:: python

   new_data = {'type': 'microns_func_coreg',
               'pt': {'position': [1,2,3]},
               'func_id': 0}
   client.annotation.post_annotation(table_name='test_table', data=[new_data])

There are methods to simplify annotation uploads if you have a pandas dataframe
whose structure mirrors the struction of the annotation schema you want to upload

.. code:: python

    import pandas as pd

    df = pd.DataFrame([{'id':0,
             'type': 'microns_func_coreg',
             'pt_position': [1,2,3]},
             'func_id': 0}, 
            {'id':1,
            'type': 'microns_func_coreg',
            'pt_position': [3,2,1]},
            'func_id': 2}])
    client.annotation.post_annotation_df('test_table', df)

Note that here I specified the IDs of my annotations, which you can do, 
but then its up to you to assure that the IDs don't collide with other IDs.
If you leave them blank then the service will assign the IDs for you.

There is a similar method for updating 
:func:`caveclient.annotationengine.AnnotationClientV2.update_annotation_df`

Staged Annotations
^^^^^^^^^^^^^^^^^^

Staged anotations help ensure that the annotations you post follow the appropriate schema, both by providing guides to the field names and locally validating against a schema before uploading.
The most common use case for staged annotations is to create a StagedAnnotation object for a given table, then add annotations to it individually or as a group, and finally upload to the annotation table.

To get a StagedAnnotation object, you can start with either a table name or a schema name. Here, we'll assume that there's already a table called "my_table" that is running a "cell_type_local" schema.
If we want to add new annotations to the table, we simply use the table name with ``stage_annotations``.

.. code:: python

    stage = client.annotation.stage_annotations("my_table")

This ``stage`` object retrieves the schema for the table and hosts a local collection of annotations. Every time you add an annotation, it is immediately validated against the schema. To add an annotation, use the ``add`` method:

.. code:: python

    stage.add(
        cell_type = "pyramidal_cell",
        classification_system="excitatory",
        pt_position=[100,100,10],
    )

The argument names derive from fields in the schema and you must provide all required fields. Any number of annotations can be added to the stage.
A dataframe of annotations can also be added with ``stage.add_dataframe``, and requires an exact match between column names and schema fields.
The key difference between this and posting a dataframe directly is that annotations added to a StagedAnnotations are validated locally, allowing any issues to be caught before uploading.

You can see the annotations as a list of dictionary records with ``stage.annotation_list`` or as a Pandas dataframe with ``stage.annotation_dataframe``.
Finally, if you initialized the stage with a table name, this information is stored in the ``stage`` and you can simply upload it from the client.

.. code:: python

    client.annotation.upload_staged_annotations(stage)

Updating annotations requires knowing the annotation id of the annotation you are updating, which is not required in the schema otherwise. In order to stage updated annotations, set the ``update`` parameter to ``True`` when creating the stage.

.. code:: python

    update_stage = client.annotation.stage_annotations("my_table", update=True)
    update_stage.add(
        id=1,
        cell_type = "stellate_cell",
        classification_system="excitatory",
        pt_position=[100,100,10],
    )

The ``update`` also informs the framework client to treat the annotations as an update and it will use the appropriate methods automatically when uploading with ``client.annotation.upload_staged_annotations``.

If you want to specify ids when posting new annotations, ``id_field`` can be set to True when creating the StagedAnnotation object. This will enforce an ``id`` column but still post the data as new annotations.

If you might be adding spatial data in coordinates that might be different than the resolution for the table, you can also set the ``annotation_resolution`` when creating the stage.
The stage will convert between the resolution you specify for your own annotations and the resolution that the table expects.

.. code:: python

    stage = client.annotation.stage_annotations("my_table", annotation_resolution=[8,8,40])
    stage.add(
        cell_type='pyramidal_cell',
        classification_system="excitatory",
        pt_position=[50,50,10],
    )

