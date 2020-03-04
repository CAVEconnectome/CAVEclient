Authentication Service
======================

Going forward, we’re going to need authentication tokens for
programmatic access to our services. The AuthClient handles storing and
loading your token or tokens and inserting it into requests in other
clients.

We can access the auth client from ``client.auth``. Once you have saved
a token, you probably won’t interact with this client very often,
however it has some convenient features for saving new tokens the first
time. Let’s see if you have a token already. Probably not.

.. code:: ipython3

    auth = client.auth
    print(f"My current token is: {auth.token}")

Getting a new token
^^^^^^^^^^^^^^^^^^^

It is not yet possible to get a new token programmatically, but the
function ``get_new_token()`` provides instructions for how to get and
save it.

By default, the token is saved to
``~/.cloudvolume/secrets/chunkedgraph-secret.json`` as a string under
the key ``token``. The following steps will save a token there.

*Note: I am not sure where the auth server is being hosted right now, so
we are going to use a fake token for documentation purposes*

.. code:: ipython3

    auth.get_new_token()

.. code:: ipython3

    new_token = 'fake_token_123'
    auth.save_token(token=new_token)
    print(f"My token is now: {auth.token}")

Loading saved tokens
^^^^^^^^^^^^^^^^^^^^

Try opening ``~/.cloudvolume/secrets/chunkedgraph-secret.json`` to see
what we just created.

If we had wanted to use a different file or a different json key, we
could have specified that in auth.save_token.

Because we used the default values, this token is used automatically
when we intialize a new FrameworkClient. If we wanted to use a different
token file, token key, or even directly specify a token we could do so
here.

.. code:: ipython3

    client = FrameworkClient(dataset_name)
    print(f"Now my basic token is: {client.auth.token}")
    
    client_direct = FrameworkClient(dataset_name, auth_token='another_fake_token_678')
    print(f"A directly specified token is: {client_direct.auth.token}")

If you use a FrameworkClient, the AuthClient and its token will be
automatically applied to any other services without further use.