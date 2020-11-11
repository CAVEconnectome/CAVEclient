import json
import requests
import webbrowser
import os
from .endpoints import auth_endpoints_v1, default_global_server_address

default_token_location = "~/.cloudvolume/secrets"
default_token_name = "chunkedgraph-secret.json"
default_token_key = 'token'
default_token_file = f"{default_token_location}/{default_token_name}"


class AuthClient(object):
    """Client to find and use auth tokens to access the dynamic annotation framework services.

    Parameters
    ----------
    token_file : str, optional
        Path to a JSON key:value file holding your auth token.
        By default, "~/.cloudvolume/secrets/chunkedgraph-secret.json"

    token_key : str, optional
        Key for the token in the token_file.
        By default, "token"

    token : str or None, optional
        Direct entry of the token as a string. If provided, overrides the files.
        If None, attempts to use the file paths.

    server_address : str, optional,
        URL to the auth server. By default, uses a default server address.
    """

    def __init__(
        self,
        token_file=None,
        token_key=None,
        token=None,
        server_address=default_global_server_address,
    ):
        if token_file is None:
            token_file = default_token_file
        self._token_file = os.path.expanduser(token_file)

        if token_key is None:
            token_key = default_token_key
        self._token_key = token_key

        if token is None:
            token = self._load_token(self._token_file, self._token_key)
        self._token = token

        self._server_address = server_address
        self._default_endpoint_mapping = {
            "auth_server_address": self._server_address}

    @property
    def token(self):
        """Secret token used to authenticate yourself to the Dynamic Annotation Framework services.
        """
        return self._token

    @token.setter
    def token(self, new_token):
        self._token = new_token
        self._token_key = None

    def get_token(self, token_key=None, ):
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

    def get_new_token(self, open=False):
        """Currently, returns instructions for getting a new token based on the current settings and saving it to the local environment. New OAuth tokens are currently not able to be retrieved programmatically.

        Parameters
        ----------
        open : bool, optional
            If True, opens a web browser to the web page where you can generate a new token.
        """
        auth_url = auth_endpoints_v1["refresh_token"].format_map(
            self._default_endpoint_mapping
        )
        txt = f"""New Tokens need to be acquired by hand. Please follow the following steps:
                1) Go to: {auth_url}
                2) Log in with your Google credentials and copy the token shown afterward.
                3a) Save it to your computer with: client.auth.save_token(token="PASTE_YOUR_TOKEN_HERE")
                or
                3b) Set it for the current session only with client.auth.token = "PASTE_YOUR_TOKEN_HERE"
                Note: If you need to save or load multiple tokens, please read the documentation for details.
                Warning! Creating a new token by finishing step 2 will invalidate the previous token!"""
        print(txt)
        if open is True:
            webbrowser.open(auth_url)
        return None

    def save_token(
        self,
        token=None,
        token_key=default_token_key,
        overwrite=False,
        token_file=None,
        switch_token=True,
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
        """
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

        secret_dir, _ = os.path.split(save_token_file)
        if not os.path.exists(secret_dir):
            full_dir = os.path.expanduser(secret_dir)
            os.makedirs(full_dir)

        with open(save_token_file, "w") as f:
            json.dump(secrets, f)

        if switch_token:
            self._token = token
            self._token_key = token_key
            self._token_file = save_token_file

    @property
    def request_header(self):
        """Formatted request header with the specified token
        """
        if self.token is not None:
            auth_header = {"Authorization": f"Bearer {self.token}"}
            return auth_header
        else:
            return {}
