import json
import requests
import webbrowser
import os
from .endpoints import auth_endpoints, default_server_address

default_token_location = os.path.expanduser("~/.cloudvolume/secrets")
default_token_name = "chunkedgraph-secret.json"
default_token_file = f"{default_token_location}/{default_token_name}"


class AuthClient(object):
    def __init__(
        self,
        token_file=default_token_file,
        token_key="token",
        token=None,
        server_address=default_server_address,
    ):
        self._token_file = token_file
        self._token_key = token_key

        if token is None:
            token = self._load_token(self._token_file, self._token_key)
        self._token = token

        self._server_address = server_address
        self._default_endpoint_mapping = {"auth_server_address": self._server_address}

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, new_token):
        self._token = new_token
        self._token_key = None

    def get_token(self, token_key=None, ):
        self._token_key = token_key
        self._token = self._load_token(self._token_file, self._token_key)

    def _load_token(self, token_file, token_key):
        if token_file is None:
            return None

        if os.path.exists(token_file):
            with open(token_file, "r") as f:
                token = json.load(f).get(token_key, None)
        else:
            token = None
        return token

    def get_new_token(self, open=False):
        auth_url = auth_endpoints["refresh_token"].format_map(
            self._default_endpoint_mapping
        )
        txt = f"""New Tokens need to be acquired by hand. Please follow the following steps:
                1) Go to: {auth_url}
                2) Log in with your Google credentials and copy the token shown afterward.
                3a) Save it to your computer with: client.auth.save_token(token="PASTE_YOUR_TOKEN_HERE")
                or
                3b) Set it for the current session only with client.auth.token = "PASTE_YOUR_TOKEN_HERE"
                Note: If you need to save or load multiple tokens, please read the documentation for details.
                Warning! Creating a new token will invalidate the previous token!"""
        print(txt)
        if open is True:
            webbrowser.open(auth_url)
        return None

    def save_token(
        self,
        token=None,
        token_key="token",
        overwrite=False,
        token_file=None,
        switch_token=True,
    ):
        if token is None:
            token = self.token

        if token_file is not None:
            save_token_file = token_file
        else:
            save_token_file = self._token_file

        if save_token_file is None:
            raise ValueError("No token file is set")

        if os.path.exists(save_token_file):
            with open(save_token_file, "r") as f:
                secrets = json.load(f)

            if overwrite is False and token_key in secrets:
                raise ValueError(
                    f"Key \"{token_key}\" already exists in token file \"{save_token_file}\"")
        else:
            secrets = {}

        secrets[token_key] = token
        with open(save_token_file, "w") as f:
            json.dump(secrets, f)

        if switch_token:
            self._token = token
            self._token_key = token_key
            self._token_file = save_token_file

    @property
    def request_header(self):
        if self.token is not None:
            auth_header = {"Authorization": f"Bearer {self.token}"}
            return auth_header
        else:
            return {}
