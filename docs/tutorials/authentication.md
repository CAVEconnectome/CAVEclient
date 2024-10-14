---
title: Authentication
---

Authentication tokens are generally needed for programmatic access to
our services. The AuthClient handles storing and loading your token or
tokens and inserting it into requests in other clients.

We can access the auth client from `client.auth`. Once you have saved a
token, you probably won't interact with this client very often, however
it has some convenient features for saving new tokens the first time.
Let's see if you have a token already. Probably not.

```python
client = CAVEclient()
auth = client.auth
print(f"My current token is: {auth.token}")
```

## Getting a new token

To get a new token, you will need to manually acquire it. For
convenience, the function [client.auth.get_new_token()]({{ api_paths.auth }}.get_new_token) provides instructions for
how to get and save the token.

By default, the token is saved to
`~/.cloudvolume/secrets/cave-secret.json` as a string under the key
`token`. This makes it compatible by default with
[Cloudvolume](https://github.com/seung-lab/cloud-volume) projects, which
can come in handy. The following steps will save a token to the default
location.

```python
auth.get_new_token()
```

```python
new_token = 'abcdef1234567890' #This is the text you see after you visit the website.
auth.save_token(token=new_token)
print(f"My token is now: {auth.token}")
```

Note that requesting a new token will invalidate your previous token on
the same project. If you want to use the same token across different
computers, you will need to share the same token information.

## Loading saved tokens

Try opening `~/.cloudvolume/secrets/cave-secret.json` to see what we
just created.

If we had wanted to use a different file or a different json key, we
could have specified that in auth.save_token.

Because we used the default values, this token is used automatically
when we initialize a new CAVEclient. If we wanted to use a different
token file, token key, or even directly specify a token we could do so
here.

```python
client = CAVEclient(datastack_name)
print(f"Now my basic token is: {client.auth.token}")

client_direct = CAVEclient(datastack_name, auth_token='another_fake_token_678')
print(f"A directly specified token is: {client_direct.auth.token}")
```

If you use a CAVEclient, the AuthClient and its token will be
automatically applied to any other services without further use.
