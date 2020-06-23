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
