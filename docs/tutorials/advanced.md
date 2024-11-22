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

## Writing unit tests using caveclient

If you have a module that relies on caveclient and you want to write unit tests for it,
you can use the `caveclient.testing` module to help you create an initialized, mocked
client with user-specified options.
This helps deal with the complex conversation that caveclient has with various services
when initializing itself so that you get the appropriate functionality.
The function `CAVEclientMock` offers a number of options for how to build a client,
including server versions for various services, available API versions, and more.
Importantly, the module provides sensible defaults but also allows you to override
them as needed.
Note that using testing requires the [responses](https://github.com/getsentry/responses)
to be installed, which is not a dependency of caveclient.

The `CAVEclientMock` function requires you to specify which services you expect to call on
in order to ensure that your code is working as expected.
For example, to create a pytest fixture that would utilize the `chunkedgraph` module,
but otherwise use default nonsense test values, you could include this in your `conftest.py`:

```python
import pytest
from caveclient.tools.testing import CAVEclientMock

@pytest.fixture()
def test_client():
    return CAVEclientMock(
        chunkedgraph=True,
    )
```

Then, in your test module, you can use the `test_client` fixture to get a client.
Note that after mocked initialization, the client will attempt normal network requests and
thus you should mock the responses.
If you only care to get a specific value back from a given function, you can use [pytest-mock](https://github.com/pytest-dev/pytest-mock/)
to mock the response to a given function call.
For example, if you have a function `do_something_with_roots` that takes a caveclient that uses the `get_roots` function,
you could mock the `get_roots` function so that it return a specific value:

```python
from pytest_mock import mocker
from conftest import test_client
from my_module import do_something_with_roots

def test_get_roots(mocker, test_client):
    mocker.patch.object(test_client.chunkedgraph, 'get_roots', return_value=[1, 2, 3])
    test_data = # Something appropriate
    test_output = do_something_with_roots(test_client, test_data)
    assert test_output == # The expected answer
```

Note that if you your own datastack `info_file` that has a different `local_server` address than the default value (`TEST_LOCAL_SERVER` variable, which defaults to `https://local.cave.com`) you will also need to specify a `local_server` with the same value as in the `local_server` field of your info file.

### Specifying conditions

While sensible defaults are provided, you can also specify things like server versions to make sure your code
works with the versions of the services you expect.
For example, let's make a richer mock client that specifies the server versions for the `chunkedgraph`, `materailization`,
and `l2cache` services:

```python
@pytest.fixture()
def version_specified_client():
    return CAVEclientMock(
        chunkedgraph=True,
        chunkedgraph_server_version='3.0.1',
        materialization=True,
        materialization_server_version='2.0.0',
        l2cache=True,
    )
```

Note that some services like `l2cache` do not currently use a server version to offer different functionality, and this value
is not exposed for them currently. See the API documentation for more information.

You can also override default values like `server_address` or `datastack_name`:
```python
@pytest.fixture()
def server_specified_client():
    return CAVEclientMock(
        datastack_name='my_datastack',
        server_address='http://my.server.com',
        chunkedgraph=True,
        materialization=True,
    )
```

If you want to get access to the various default values of server version, datastack name, datastack info file, and api versions,
you can use functions `get_server_versions`, `get_server_information`, `default_info`, `get_api_versions` respectively.
Each of these functions will return a dictionary of the values that can be used as a kwargs input into CAVEclientMock.
If you specify your own override values, it will take precedence over the default value and you can just use the dictionary in your tests.
See the caveclient tests for an example of how to use these functions.
