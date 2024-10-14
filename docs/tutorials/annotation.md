---
title: Annotation
---

The AnnotationClient is used to interact with the AnnotationEngine
service to create tables from existing schema, upload new data, and
download existing annotations. Note that annotations in the
AnnotationEngine are not linked to any particular segmentation, and thus
do not include any root ids. An annotation client is accessed with
`client.annotation`.

## Getting existing tables

A list of the existing tables for the datastack can be found with
[get_tables()]({{ api_paths.annotation }}.get_tables).

```python
all_tables = client.annotation.get_tables()
all_tables[0]
```

Each table has three main properties that can be useful to know:

- `table_name` : The table name, used to refer to it when uploading or
  downloading annotations. This is also passed through to the table in
  the Materialized database.
- `schema_name` : The name of the table's schema from
  EMAnnotationSchemas (see below).
- `max_annotation_id` : An upper limit on the number of annotations
  already contained in the table.

## Downloading annotations

You can download the JSON representation of a data point through the
[get_annotation()]({{ api_paths.annotation }}.get_annotation)
method. This can be useful if you need to look up
information on unmaterialized data, or to see what a properly templated
annotation looks like.

```python
table_name = all_tables[0]['table_name']      # 'ais_analysis_soma'
annotation_id = 100
client.annotation.get_annotation(annotation_ids=annotation_id, table_name=table_name)
```

## Create a new table

One can create a new table with a specified schema with the
[create_table()]({{ api_paths.annotation }}.create_table)
method:

```python
client.annotation.create_table(table_name='test_table',
                               schema_name='microns_func_coreg',
                               voxel_resolution = [1,1,1],
                               description="some text to describe your table")
```

The voxel resolution is the units your position columns will be uploaded
in [1,1,1] would imply a nm location, where as [4,4,40] would
correspond to voxels of that size. If you are uploading points from a
neuroglancer session, you want this to match the units of that
neuroglancer view.

Note there are some optional metadata parameters to
[create_table()]({{ api_paths.annotation }}.create_table)

- `notice_text` : This is text that will show up to users who access
  this data as a warning. This could be used to warn users that the
  data is not complete or checked yet, or to advertise that a
  particular publication should be cited when using this table.
- `read_permission` : one of "PRIVATE" which means only you can read
  data in this table. "PUBLIC" (default) which means anyone can read
  this table that has read permissions to this dataset. So if and only
  if you can read the segmentation results of this data, you can read
  this table. "GROUP" which means that you must share a common group
  with this user for them to be able to read. We need to make a way to
  discover what groups you are in and who you share groups with.
- `write_permission`: one of "PRIVATE" (default), which means only
  you can write to this table. "PUBLIC" which means anyone can write
  to this table that has write permissions to this dataset. Note
  although this means anyone can add data, no annotations are ever
  truly overwritten. "GROUP" which means that you must share a
  common group with this user for them to be able to write. We need to
  make a way to discover what groups you are in and who you share
  groups with.

If you change your mind about what you want for metadata, some but not
all fields can be updated with
[update_metadata()]({{ api_paths.annotation }}.update_metadata). This includes the
description, the notice_text, and the permissions, but not the name, schema or voxel
resolution.

```python
# to update description
client.annotation.update_metadata(table_name='test_table',
                                  description="a new description for my table")

# to make your table readable by anybody who can read this dataset
client.annotation.update_metadata(table_name='test_table',
                                  notice_text="This table isn't done yet, don't trust it. Contact me")

# to make your table readable by anybody who can read this dataset
client.annotation.update_metadata(table_name='test_table',
                                  read_permisison="PUBLIC")
```

New data can be generated as a dict or list of dicts following the
schema and uploaded with `post_annotation`. For example, a
`microns_func_coreg` point needs to have:

- `type` set to `microns_func_coreg`
- `pt` set to a dict with `position` as a key and
  the xyz location as a value.
- `func_id` set to an integer.

The following could would create a new annotation and then upload it to
the service. Note that you get back the annotation id(s) of what you
uploaded.

```python
new_data = {'type': 'microns_func_coreg',
            'pt': {'position': [1,2,3]},
            'func_id': 0}
client.annotation.post_annotation(table_name='test_table', data=[new_data])
```

There are methods to simplify annotation uploads if you have a pandas
dataframe whose structure mirrors the struction of the annotation schema
you want to upload

```python
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
```

Note that here I specified the IDs of my annotations, which you can do,
but then its up to you to assure that the IDs don't collide with other
IDs. If you leave them blank then the service will assign the IDs for
you.

There is a similar method for updating
[update_annotation_df()]({{ api_paths.annotation }}.update_annotation_df)

## Staged Annotations

Staged annotations help ensure that the annotations you post follow the
appropriate schema, both by providing guides to the field names and
locally validating against a schema before uploading. The most common
use case for staged annotations is to create a StagedAnnotation object
for a given table, then add annotations to it individually or as a
group, and finally upload to the annotation table.

To get a StagedAnnotation object, you can start with either a table name
or a schema name. Here, we'll assume that there's already a table
called "my_table" that is running a "cell_type_local" schema. If we
want to add new annotations to the table, we simply use the table name
with [stage_annotations()]({{ api_paths.annotation }}.stage_annotations).

```python
stage = client.annotation.stage_annotations("my_table")
```

This `stage` object retrieves the schema for the table and hosts a local
collection of annotations. Every time you add an annotation, it is
immediately validated against the schema. To add an annotation, use the
`add` method:

```python
stage.add(
    cell_type = "pyramidal_cell",
    classification_system="excitatory",
    pt_position=[100,100,10],
)
```

The argument names derive from fields in the schema and you must provide
all required fields. Any number of annotations can be added to the
stage. A dataframe of annotations can also be added with
`stage.add_dataframe`, and requires an exact match between column names
and schema fields. The key difference between this and posting a
dataframe directly is that annotations added to a StagedAnnotations are
validated locally, allowing any issues to be caught before uploading.

You can see the annotations as a list of dictionary records with
`stage.annotation_list` or as a Pandas dataframe with
`stage.annotation_dataframe`. Finally, if you initialized the stage with
a table name, this information is stored in the `stage` and you can
simply upload it from the client.

```python
client.annotation.upload_staged_annotations(stage)
```

Updating annotations requires knowing the annotation id of the
annotation you are updating, which is not required in the schema
otherwise. In order to stage updated annotations, set the `update`
parameter to `True` when creating the stage.

```python
update_stage = client.annotation.stage_annotations("my_table", update=True)
update_stage.add(
    id=1,
    cell_type = "stellate_cell",
    classification_system="excitatory",
    pt_position=[100,100,10],
)
```

The `update` also informs the framework client to treat the annotations
as an update and it will use the appropriate methods automatically when
uploading `client.annotation.upload_staged_annotations`.

If you want to specify ids when posting new annotations, `id_field` can
be set to True when creating the StagedAnnotation object. This will
enforce an `id` column but still post the data as new annotations.

If you might be adding spatial data in coordinates that might be
different than the resolution for the table, you can also set the
`annotation_resolution` when creating the stage. The stage will convert
between the resolution you specify for your own annotations and the
resolution that the table expects.

```python
stage = client.annotation.stage_annotations("my_table", annotation_resolution=[8,8,40])
stage.add(
    cell_type='pyramidal_cell',
    classification_system="excitatory",
    pt_position=[50,50,10],
)
```
