---
title: EM Annotation Schemas
---

The EMAnnotationSchemas client lets one look up the available schemas
and how they are defined. This is mostly used for programmatic
interactions between services, but can be useful when looking up schema
definitions for new tables.

## Get the list of schema

One can get the list of all available schema with the `schema` method.
Currently, new schema have to be generated on the server side, although
we aim to have a generic set available to use.

```python
client.schema.get_schemas()
```

## View a specific schema

The details of each schema can be viewed with the `schema_definition`
method, formatted as per JSONSchema.

```python
example_schema = client.schema.schema_definition('microns_func_coreg')
example_schema
```

This is mostly useful for programmatic interaction between services at
the moment, but can also be used to inspect the expected form of an
annotation by digging into the format.

```python
example_schema['definitions']['FunctionalCoregistration']
```
