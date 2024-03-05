---
title: JSON Neuroglancer State Service
---

We store the JSON description of a Neuroglancer state in a simple
database at the JSON Service. This is a convenient way to build states
to distribute to people, or pull states to parse work by individuals.
The JSON Client is at `client.state`

```python
client.state
```

## Retrieving a state

JSON states are found simply by their ID, which you get when uploading a
state. You can download a state with `get_state_json`.

```python
example_id = 4845531975188480
example_state = client.state.get_state_json(test_id)
example_state['layers'][0]
```

## Uploading a state

You can also upload states with `upload_state_json`. If you do this, the
state id is returned by the function. Note that there is no easy way to
query what you uploaded later, so be VERY CAREFUL with this state id if
you wish to see it again.

_Note: If you are working with a Neuroglancer Viewer object or similar,
in order to upload, use viewer.state.to_json() to generate this
representation._

```python
example_state['layers'][0]['name'] = 'example_name'
new_id = client.state.upload_state_json(example_state)
```

```python
test_state = client.state.get_state_json(new_id)
test_state['layers'][0]['name']
```

## Generating a Neuroglancer URL

Once you have a state ID, you want to turn it into a well-formatted
link. So you don\'t have to remember all the endpoints, we can do this
from the state client.

```python
ngl_base = 'neuromancer-seung-import.appspot.com'
client.state.build_neuroglancer_url(new_id, ngl_base)
```

Note that the neuroglancer base can be found in the info service under
`client.info.viewer_site()`.
