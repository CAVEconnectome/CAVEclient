---
title: Skeletonization
---

The skeleton service enables you to generate and retrieve skeletons from the server. This saves you the trouble of performing skeletonization routines on your local machine, while also saving you the trouble of understanding many of the details inside of skeletonization. Furthermore, the server offers the ability to generate multiple skeletons simultaneously, in parallel. And lastly, the server caches each skeleton that is generated so that it only ever needs to be generated once. All subsequent requests for the same skeleton will simply return the cached skeleton with notable turn-around-time improvements.

## Initializing the client

The simplest way to initialize the CAVEclient is by merely providing the datastack of interest:

```python
import caveclient as cc

client = cc.CAVEclient(<datastack_name>)
```

With a CAVEclient built, you can now investigate the current build version of the SkeletonClient:

```python
client.skeleton.get_version()
```

And you can see a list of available skeleton versions. In most cases you will want to use the highest (most recent) version provided.

```python
client.skeleton.get_versions()
```

## Retrieving a skeleton

Retrieve a skeleton using `get_skeleton()`. The simplest usage is:

```python
sk = client.skeleton.get_skeleton(
    <root_id>,
    output_format=<output_format>,
)
```

where the availale output_formats (described below) are:

* ```'dict'``` (default if unspecified)
* ```'swc'``` (a Pandas Dataframe)

If the skeleton doesn't exist in the server cache, it may take 20-60 seconds to generate the skeleton before it is returned. This function will block during that time. Any subsequent retrieval of the same skeleton should go very quickly however.

To specify a nondefault skeleton version:

```python
sk = client.skeleton.get_skeleton(
    <root_id>,
    skeleton_version=<sk_version>,
    output_format=<output_format>,
)
```

To specify a specific datastack:

```python
sk = client.skeleton.get_skeleton(
    <root_id>,
    datastack_name=<datastack_name>,
    skeleton_version=<sk_version>,
    output_format=<output_format>,
)
```

## Peering into the contents of the cache

Most end-users shouldn't need to use the following function very much, but to see the contents of the cache for a given root id, set of root ids, root id prefix, or set of prefixes:

```python
get_cache_contents(
    root_id_prefixes=<root_id>,
)
```

You can also add additional parameters as needed:

```python
get_cache_contents(
    datastack_name=<datastack_name>,
    skeleton_version=<sk_version>,
    root_id_prefixes=<root_id>,
)
```

The primary parameter, `root_id_prefixes`, can be a list of root ids:

```python
get_cache_contents(
    root_id_prefixes=[<root_id>, <root_id>, ...],
)
```

The primary parameter can also be a root id prefix, which will match any associated root ids. Since this could potentially return a large list of results, there is an optional `limit` parameter so you don't overwhelm the memory of your processing environment, e.g., a Jupyter notebook or some other Python script running on your local machine:

```python
get_cache_contents(
    root_id_prefixes=<root_id_prefix>,
    limit=<limit>,
)
```

Note that the limit only constraints the size of the return value. The internal operation of the function will still receive the full list when it passes the prefix on to CloudFiles. Consequently, calling this function for a short prefix may block for a long time.

And of course you can also pass in a list of prefixes (or a mixture of full ids and partial prefixes):

```python
get_cache_contents(
    root_id_prefixes=[<root_id_prefix>, <root_id_prefix>, ...],
    limit=<limit>,
)
```

## Querying the presence of a skeleton in the cache

The function shown above isn't necessarily the most direct way to simply inquire whether a skeleton exists in the cache for a given root id. For that purpose, the following function is better suited:

```python
skeletons_exist(
    root_ids=<root_id>,
)
```

Or:

```python
skeletons_exist(
    root_ids=[<root_id>, <root_id>, ...],
)
```

Note that this function doesn't accept prefixes, as supported by `cache_query_contents()`. Only full root ides are supported. When querying with as single root id, the return value will be a boolean. When querying with a list of ids, the return value will be a Python dictionary mapping from each id to a boolean.

This function also takes the same optional parameters described above:

```python
skeletons_exist(
    datastack_name=<datastack_name>,
    skeleton_version=<sk_version>,
    root_ids=<root_id>,  # Or [<root_id>, <root_id>, ...],
)
```

## Retrieving multiple skeletons

You can retrieve a large set of skeletons in a single function call:

```python
get_bulk_skeletons(
    root_ids=[<root_id>, <root_id>, ...],
)
```

If any skeletons are not generated yet, the default behavior is to skip those root ids and only return skeletons that are already available. But you can override this default behavior:

```python
get_bulk_skeletons(
    root_ids=[<root_id>, <root_id>, ...],
    generate_missing_skeletons=[False|True],
)
```

Any root ids for which skeletonization is required will be generated one at a time, at a cost of 20-60 seconds each. Consequently, there is a hard-coded limit of 10, after which all subsequent missing skeletons will not be returned.

By default, skeletons are returned in JSON format. However SWC is also supported, thusly:

```python
get_bulk_skeletons(
    root_ids=[<root_id>, <root_id>, ...],
    output_format=<"json"|"swc">
)
```

And the usual defaults can be overridden again:

```python
get_bulk_skeletons(
    root_ids=[<root_id>, <root_id>, ...],
    datastack_name=<datastack_name>,
    skeleton_version=<sk_version>,
)
```

## Generating multiple skeletons in parallel

`get_bulk_skeletons()` is not an effective way to produce a large number of skeletons since it operates synchronously, generating one skeleton at a time. In order to generate a large number of skeletons it is better to do so in parallel. The following function dispatches numerous root ids for skeletonization without returning anything immediately. The root ids are then distributed on the server for parallel skeletonization and eventual caching. Once they are in the cache, you can retrieve them. Of course, it can be tricky to know when they are available. That is addressed further below. Here's how to dispatch asynchronous bulk skeletonization:

```python
generate_bulk_skeletons_async(
    root_ids=[<root_id>, <root_id>, ...],
)
```

And with the usual overrides:

```python
get_bulk_skeletons(
    root_ids=[<root_id>, <root_id>, ...],
    datastack_name=<datastack_name>,
    skeleton_version=<sk_version>,
)
```

## Retrieving asynchronously generated skeletons

In order to retrieve asynchronously generated skeletons, it is necessary to _poll_ the cache for the availability of the skeletons and then eventually retrieve them. Here's an example of such a workflow:

```
# Dispatch multiple asynchronous, parallel skeletonization and caching processes
generate_bulk_skeletons_async(root_ids)

# Repeatedly query the cache for the existence of the skeletons until they are all available
while True:
    skeletons_that_exist = client.skeleton.skeletons_exist(root_ids=rids)
    num_skeletons_found = sum(skeletons_that_exist.values())
    if num_skeletons_found == len(rids):
        break
    sleep(10)  # Pause for ten seconds and check again

# Retrieve the skeletons (remember, SWC is also offered)
skeletons_json = get_bulk_skeletons(root_ids)
```