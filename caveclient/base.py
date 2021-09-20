import urllib
import requests
import json
import logging
import webbrowser
from .session_config import patch_session
import numpy as np
import datetime


class BaseEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class AuthException(Exception):
    pass


def _raise_for_status(r):
    http_error_msg = ""
    if isinstance(r.reason, bytes):
        # We attempt to decode utf-8 first because some servers
        # choose to localize their reason strings. If the string
        # isn't utf-8, we fall back to iso-8859-1 for all other
        # encodings. (See PR #3538)
        try:
            reason = r.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = r.reason.decode("iso-8859-1")
    else:
        reason = r.reason

    if 400 <= r.status_code < 500:
        http_error_msg = "%s Client Error: %s for url: %s content: %s" % (
            r.status_code,
            reason,
            r.url,
            r.content,
        )
        json_data = None
        if r.headers.get("content-type") == "application/json":
            json_data = r.json()

        if r.status_code == 403:
            if json_data:
                if "error" in json_data.keys():
                    if json_data["error"] == "missing_tos":
                        webbrowser.open(json_data["data"]["tos_form_url"])

    elif 500 <= r.status_code < 600:
        http_error_msg = "%s Server Error: %s for url: %s content:%s" % (
            r.status_code,
            reason,
            r.url,
            r.content,
        )

    if http_error_msg:
        raise requests.HTTPError(http_error_msg, response=r)
    warning = r.headers.get("Warning")
    if warning:
        logging.warning(warning)


def handle_response(response, as_json=True):
    """Deal with potential errors in endpoint response and return json for default case"""
    _raise_for_status(response)
    _check_authorization_redirect(response)
    if as_json:
        return response.json()
    else:
        return response


def _check_authorization_redirect(response):
    if len(response.history) == 0:
        pass
    else:
        first_url = response.history[0].url
        urlp = urllib.parse.urlparse(first_url)

        raise AuthException(
            f"""You have not setup a token to access
{first_url}
with the current auth configuration.\n
Read the documentation at 
https://caveclient.readthedocs.io/en/latest/guide/authentication.html
or follow instructions under 
client.auth.get_new_token() for how to set a valid API token.
after initializing a global client with
client=CAVEclient(server_address="{urlp.scheme +"://"+ urlp.netloc}")"""
        )


def _api_versions(server_name, server_address, endpoints_common, auth_header):
    """Asks a server what API versions are available, if possible"""
    url_mapping = {server_name: server_address}
    url_base = endpoints_common.get("get_api_versions", None)
    if url_base is not None:
        url = url_base.format_map(url_mapping)
        response = requests.get(url, headers=auth_header)
        _raise_for_status(response)
        return response.json()
    else:
        return None


def _api_endpoints(
    api_version,
    server_name,
    server_address,
    endpoints_common,
    endpoint_versions,
    auth_header,
):
    "Gets the latest client API version"
    if api_version == "latest":
        try:
            avail_vs_server = _api_versions(
                server_name, server_address, endpoints_common, auth_header
            )
            avail_vs_server = set(avail_vs_server)
        except:
            avail_vs_server = None

        avail_vs_client = set(endpoint_versions.keys())

        if avail_vs_server is None:
            api_version = max(avail_vs_client)
        else:
            api_version = max(avail_vs_client.intersection(avail_vs_server))

    endpoints = endpoints_common.copy()
    ep_to_add = endpoint_versions.get(api_version, None)
    if ep_to_add is None:
        raise ValueError("No corresponding API version")
    endpoints.update(ep_to_add)
    return endpoints, api_version


class ClientBase(object):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
    ):
        self._server_address = server_address
        self._default_url_mapping = {server_name: self._server_address}
        self.verify = verify
        self.session = requests.Session()
        patch_session(
            self.session,
            max_retries=max_retries,
            pool_block=pool_block,
            pool_maxsize=pool_maxsize,
        )

        self.session.verify = verify
        head_val = auth_header.get("Authorization", None)
        if head_val is not None:
            token = head_val.split(" ")[1]
            cookie_obj = requests.cookies.create_cookie(
                name="middle_auth_token", value=token
            )
            self.session.cookies.set_cookie(cookie_obj)
        self.session.headers.update(auth_header)

        self._api_version = api_version
        self._endpoints = endpoints

    @property
    def default_url_mapping(self):
        return self._default_url_mapping

    @property
    def server_address(self):
        return self._server_address

    @property
    def api_version(self):
        return self._api_version

    @staticmethod
    def raise_for_status(r):
        """Raises :class:`HTTPError`, if one occurred."""

        _raise_for_status(r)


class ClientBaseWithDataset(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        dataset_name,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
    ):

        super(ClientBaseWithDataset, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
        )
        self._dataset_name = dataset_name

    @property
    def dataset_name(self):
        return self._dataset_name


class ClientBaseWithDatastack(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        datastack_name,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
    ):

        super(ClientBaseWithDatastack, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
        )
        self._datastack_name = datastack_name

    @property
    def datastack_name(self):
        return self._datastack_name
