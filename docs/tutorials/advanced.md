---
title: Advanced Usage
---

## Changing session configurations

It is possible to change details of how a client talks to the various servers it needs
to interface with. For instance, the `CAVEclient` will attempt to retry
specific failed requests to the server, but will only try a specific number of times,
and will only wait specific amounts of time between retries. These values can be changed
via the `set_session_defaults` method. For instance, to change the number of retries to
5, and to increase the delay between subsequent retries, you could do:

```python
from caveclient import set_session_defaults

set_session_defaults(max_retries=5, backoff_factor=0.5)
```

Note that this needs to happen before initializing the client for this to work
properly. Some of these parameters are also adjustable at the client level.

To view the current session defaults, you can use the `get_session_defaults` method:

```python
from caveclient import get_session_defaults

client.get_session_defaults()
```

More information on the available parameters can be found in the
[API documentation](../api/config.md).
