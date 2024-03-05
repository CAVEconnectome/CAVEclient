---
title: Info Service
---

A datastack has a number of complex paths to various data sources that
together comprise a datastack. Rather than hardcode these paths, the
InfoService allows one to query the location of each data source. This
is also convenient in case data sources change.

An InfoClient is accessed at `client.info`.

```python
client = CAVEclient(datastack_name)
print(f"This is an info client for {client.info.datastack_name} on {client.info.server_address}")
```

## Accessing datastack information

All of the information accessible for the datastack can be seen as a
dict using `get_datastack_info()`.

```python
info.get_datastack_info()
```

Individual entries can be found as well. Use tab autocomplete to see the
various possibilities.

```python
info.graphene_source()
```

## Adjusting formatting

Because of the way neuroglancer looks up data versus cloudvolume,
sometimes one needs to convert between `gs://` style paths to
`https://storage.googleapis.com/` stype paths. All of the path sources
in the info client accept a `format_for` argument that can handle this,
and correctly adapts to graphene vs precomputed data sources.

```python
neuroglancer_style_source = info.image_source(format_for='neuroglancer')
print(f"With gs-style: { neuroglancer_style_source }")

cloudvolume_style_source = info.image_source(format_for='cloudvolume')
print(f"With https-style: { cloudvolume_style_source }")
```
