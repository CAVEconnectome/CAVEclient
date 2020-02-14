import json
import requests
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

    def get(self, token_key):
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

    def get_new_token(self):
        auth_url = auth_endpoints["refresh_token"].format_map(
            self._default_endpoint_mapping
        )
        txt = f"New Tokens need to be acquired by hand at {auth_url}\nWarning! Creating a new token will invalidate the previous token!"
        print(txt)
        return None

    def save_new_token(self, new_token=None, new_token_key="token", overwrite=False, switch_token=True):
        if new_token is None:
            new_token = self.token

        if self._token_file is None:
            raise ValueError("Token file is not set")

        if os.path.exists(self._token_file):
            with open(self._token_file, "r") as f:
                secrets = json.load(f)

            if overwrite is False:
                if new_token_key in secrets:
                    raise ValueError("Token key already exists in token file")
        else:
            secrets = {}

        secrets[new_token_key] = new_token
        with open(self._token_file, "w") as f:
            json.dump(secrets, f)

        if switch_token:
            self._token = new_token
            self._token_key = new_token_key

    @property
    def request_header(self):
        if self.token is not None:
            auth_header = {"Authorization": f"Bearer {self.token}"}
            return auth_header
        else:
            return {}
