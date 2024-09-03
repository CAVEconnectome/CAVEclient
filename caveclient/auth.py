import json
import logging
import os
import urllib
import webbrowser
from typing import Optional

import requests

from .base import (
    handle_response,
)
from .endpoints import auth_endpoints_v1, default_global_server_address

logger = logging.getLogger(__name__)

default_token_location = "~/.cloudvolume/secrets"
default_token_name = "cave-secret.json"
deprecated_token_names = ["chunkedgraph-secret.json"]
default_token_key = "token"
default_token_file = f"{default_token_location}/{default_token_name}"
deprecated_token_files = [
    f"{default_token_location}/{f}" for f in deprecated_token_names
]


def write_token(token, filepath, key, overwrite=True):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            secrets = json.load(f)

        if overwrite is False and key in secrets:
            raise ValueError(f'Key "{key}" already exists in token file "{filepath}"')
    else:
        secrets = {}

    secrets[key] = token

    secret_dir = os.path.dirname(filepath)
    if not os.path.exists(secret_dir):
        full_dir = os.path.expanduser(secret_dir)
        os.makedirs(full_dir)

    with open(filepath, "w") as f:
        json.dump(secrets, f)


def server_token_filename(server_address):
    server = urllib.parse.urlparse(server_address).netloc
    server_file = server + "-cave-secret.json"
    server_file_path = os.path.join(default_token_location, server_file)
    return os.path.expanduser(server_file_path)


class AuthClient(object):
    def __init__(
        self,
        token_file=None,
        token_key=None,
        token=None,
        server_address=default_global_server_address,
        local_server=None,
    ):
        """Client to find and use auth tokens to access the dynamic annotation framework services.

        Parameters
        ----------
        token_file : str, optional
            Path to a JSON key:value file holding your auth token.
            By default, "~/.cloudvolume/secrets/cave-secret.json"
            (will check deprecated token name "chunkedgraph-secret.json" as well)
        token_key : str, optional
            Key for the token in the token_file.
            By default, "token"

        token : str or None, optional
            Direct entry of the token as a string. If provided, overrides the files.
            If None, attempts to use the file paths.

        server_address : str, optional,
            URL to the auth server. By default, uses a default server address.
        """
        self._server_address = server_address
        self._local_server = local_server

        if token_file is None:
            self._server_file_path = server_token_filename(self._server_address)
            if os.path.isfile(self._server_file_path):
                token_file = self._server_file_path
            else:
                token_file = default_token_file
        self._token_file = os.path.expanduser(token_file)

        if token_key is None:
            token_key = default_token_key
        self._token_key = token_key

        if token is None:
            token = self._load_token(self._token_file, self._token_key)
            if token is None:
                # then check the deprecated token
                for deprecated_file in deprecated_token_files:
                    _dep_file = os.path.expanduser(deprecated_file)
                    token = self._load_token(_dep_file, self._token_key)
                    if token is not None:
                        logger.warning(
                            f"""file location {deprecated_file} is deprecated,
rename to 'cave-secret.json' or 'SERVER_ADDRESS-cave-secret.json"""
                        )
                        # then we found a token and we should break
                        break
        self._token = token

        self._default_endpoint_mapping = {"auth_server_address": self._server_address}

    @property
    def token(self):
        """Secret token used to authenticate yourself to the Connectome Annotation Versioning Engine services."""
        return self._token

    @token.setter
    def token(self, new_token):
        self._token = new_token
        self._token_key = None

    def get_token(
        self,
        token_key=None,
    ):
        """Load a token with a given key the specified token file

        Parameters
        ----------
        token_key : str or None, optional
            key in the token file JSON, by default None. If None, uses 'token'.
        """
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

    def setup_token(self, make_new=True, open=True):
        """Currently, returns instructions for getting your auth token based on the current settings and saving it to the local environment.
        New OAuth tokens are currently not able to be retrieved programmatically.

        Parameters
        ----------
        make_new : bool, optional
            If True, will make a new token, else prompt you to open a page to
            retrieve an existing token.
        open : bool, optional
            If True, opens a web browser to the web page where you can retrieve a token.
        """
        if make_new:
            return self.get_new_token(open=open)

        auth_url = auth_endpoints_v1["get_tokens"].format_map(
            self._default_endpoint_mapping
        )
        txt = f"""Tokens need to be acquired by hand. Please follow the following steps:
                1) Go to: {auth_url} to view a list of your existing tokens.
                2) Log in with your Google credentials copy one of the tokens from the dictionary (the string under the key 'token').
                3a) Save it to your computer with: client.auth.save_token(token="PASTE_YOUR_TOKEN_HERE")
                or
                3b) Set it for the current session only with client.auth.token = "PASTE_YOUR_TOKEN_HERE"
                Note: If you need to save or load multiple tokens, please read the documentation for details.
                if you want to create a new token, or have no token use ```self.get_new_token``` instead
                or use this function with the keyword argument make_new=True"""
        print(txt)
        if open:
            webbrowser.open(auth_url)
        return None

    def get_tokens(self):
        """Get the tokens setup for this users

        Returns
        -------
        list[dict]:
            a list of dictionary of tokens, each with the keys
            "id": the id of this token
            "token": the token (str)
            "user_id": the users id (should be your ID)
        """
        url = auth_endpoints_v1["get_tokens"].format_map(self._default_endpoint_mapping)
        response = requests.Session().get(url, headers=self.request_header)

        return handle_response(response)

    def get_new_token(self, open=False, no_text=False):
        """Currently, returns instructions for getting a new token based on the current settings and saving it to the local environment. New OAuth tokens are currently not able to be retrieved programmatically.

        Parameters
        ----------
        open : bool, optional
            If True, opens a web browser to the web page where you can generate a new token.
        """
        auth_url = auth_endpoints_v1["create_token"].format_map(
            self._default_endpoint_mapping
        )
        txt = f"""New Tokens need to be acquired by hand. Please follow the following steps:
                1) Go to: {auth_url} to create a new token.
                2) Log in with your Google credentials and copy the token shown afterward.
                3a) Save it to your computer with: client.auth.save_token(token="PASTE_YOUR_TOKEN_HERE")
                or
                3b) Set it for the current session only with client.auth.token = "PASTE_YOUR_TOKEN_HERE"
                Note: If you need to save or load multiple tokens, please read the documentation for details.
                Warning! Creating a new token by finishing step 2 will invalidate the previous token!"""
        if not no_text:
            print(txt)
        if open:
            webbrowser.open(auth_url)
        return None

    def save_token(
        self,
        token: Optional[str] = None,
        token_key: str = default_token_key,
        overwrite: bool = False,
        token_file: Optional[str] = None,
        switch_token: bool = True,
        write_to_server_file: bool = True,
    ):
        """Conveniently save a token in the correct format.

        After getting a new token by following the instructions in `authclient.get_new_token()`, you can save it with a fully default configuration by running:

        token = 'my_shiny_new_token'

        authclient.save_token(token=token)

        Now on next load, authclient=AuthClient() will make an authclient instance using this token.
        If you would like to specify more information about the json file where the token will be stored, see the parameters below.

        Parameters
        ----------
        token : str, optional
            New token to save, by default None
        token_key : str, optional
            Key for the token in the token_file json, by default "token"
        overwrite : bool, optional
            Allow an existing token to be changed, by default False
        token_file : str, optional
            Path to the token file, by default None. If None, uses the default file location specified above.
        switch_token : bool, optional
            If True, switch the auth client over into using the new token, by default True
        write_to_server_file: bool, optional
            If True, will write token to a server specific file to support this machine
            interacting with multiple auth servers.
        """
        if token is None:
            token = self.token

        if token_file is not None:
            save_token_file = token_file
        else:
            save_token_file = self._token_file

        if save_token_file is None:
            raise ValueError("No token file is set")
        if write_to_server_file:
            write_token(token, self._server_file_path, token_key, overwrite=overwrite)
        write_token(token, save_token_file, token_key, overwrite=overwrite)

        if switch_token:
            self._token = token
            self._token_key = token_key
            self._token_file = save_token_file

    def get_user_information(self, user_ids):
        """Get user data.

        Parameters
        ----------
        user_ids : list of int
            user_ids to look up
        """
        endpoint_mapping = self._default_endpoint_mapping
        params = {"id": ",".join(str(i) for i in user_ids)}
        url = auth_endpoints_v1["get_users"].format_map(endpoint_mapping)
        response = requests.Session().get(
            url, headers=self.request_header, params=params
        )

        return handle_response(response)

    def get_group_users(self, group_id):
        """Get users in a group

        Parameters
        ----------
        group_id : int
            ID value for a given group

        Returns
        -------
        list
            List of dicts of user ids. Returns empty list if group does not exist.
        """
        endpoint_mapping = self._default_endpoint_mapping
        endpoint_mapping["group_id"] = group_id
        url = auth_endpoints_v1["get_group_users"].format_map(endpoint_mapping)
        response = requests.Session().get(url, headers=self.request_header)

        return handle_response(response)

    @property
    def request_header(self):
        """Formatted request header with the specified token"""
        if self.token is not None:
            auth_header = {"Authorization": f"Bearer {self.token}"}
            return auth_header
        else:
            return {}

    @property
    def local_server(self):
        return self._local_server

    @local_server.setter
    def local_server(self, new_val):
        self._local_server = new_val
        self._synchronize_local_server_file()

    @property
    def local_server_filepath(self):
        if self.local_server:
            return server_token_filename(self.local_server)
        else:
            return None

    def _synchronize_local_server_file(self):
        if self.local_server:
            if os.path.exists(self.local_server_filepath):
                local_token = self._load_token(
                    self.local_server_filepath, self._token_key
                )
                if local_token != self.token:
                    self.save_token(
                        token=self.token,
                        token_file=self.local_server_filepath,
                        overwrite=True,
                    )
            else:
                self.save_token(
                    token=self.token,
                    token_file=self.local_server_filepath,
                    overwrite=True,
                )
