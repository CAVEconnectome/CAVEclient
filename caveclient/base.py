import datetime
import json
import logging
import operator
import urllib
import webbrowser
from functools import wraps
from typing import Callable

import numpy as np
import requests
from packaging.version import Version
from packaging.version import parse as parse_version

import pandas as pd

from .session_config import patch_session

logger = logging.getLogger(__name__)


class BaseEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.ndarray, pd.Series, pd.Index)):
            return obj.tolist()
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class AuthException(Exception):
    pass


def _raise_for_status(r, log_warning=True):
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
        try:
            d = json.loads(r.content)
            reason = d.get("message", reason)
        except json.decoder.JSONDecodeError:
            pass
        http_error_msg = "%s Server Error: %s for url: %s content:%s" % (
            r.status_code,
            reason,
            r.url,
            r.content,
        )

    if http_error_msg:
        raise requests.HTTPError(http_error_msg, response=r)
    if log_warning:
        warning = r.headers.get("Warning")
        if warning:
            logger.warning(warning)


def handle_response(response, as_json=True, log_warning=True):
    """Deal with potential errors in endpoint response and return json for default case"""
    _raise_for_status(response, log_warning=log_warning)
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
https://caveconnectome.github.io/CAVEclient/tutorials/authentication/
or follow instructions under 
client.auth.get_new_token() for how to set a valid API token.
after initializing a global client with
client=CAVEclient(server_address="{urlp.scheme +"://"+ urlp.netloc}")"""
        )


def _api_versions(
    server_name, server_address, endpoints_common, auth_header, verify=True
):
    """Asks a server what API versions are available, if possible"""
    url_mapping = {server_name: server_address}
    url_base = endpoints_common.get("get_api_versions", None)
    if url_base is not None:
        url = url_base.format_map(url_mapping)
        response = requests.get(url, headers=auth_header, verify=verify)
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
    fallback_version=None,
    verify=True,
):
    "Gets the latest client API version"
    if api_version == "latest":
        try:
            avail_vs_server = _api_versions(
                server_name,
                server_address,
                endpoints_common,
                auth_header,
                verify=verify,
            )
            avail_vs_server = set(avail_vs_server)
        except:  # noqa: E722
            avail_vs_server = None

        avail_vs_client = set(endpoint_versions.keys())

        if avail_vs_server is None:
            if fallback_version is None:
                api_version = max(avail_vs_client)
            else:
                api_version = fallback_version
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
        over_client=None,
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
        self._fc = over_client

    @property
    def fc(self):
        return self._fc

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
    def raise_for_status(r, log_warning=True):
        """Raises [requests.HTTPError][], if one occurred."""

        _raise_for_status(r, log_warning=log_warning)


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
        over_client=None,
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
            over_client=over_client,
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
        over_client=None,
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
            over_client=over_client,
        )
        self._datastack_name = datastack_name

    @property
    def datastack_name(self):
        return self._datastack_name


def parametrized(dec):
    """This decorator allows you to easily create decorators that take arguments"""
    # REF: https://stackoverflow.com/questions/5929107/decorators-with-parameters

    @wraps(dec)
    def layer(*args, **kwargs):
        @wraps(dec)
        def repl(f):
            return dec(f, *args, **kwargs)

        return repl

    return layer


def _extract_constraint_info(constraint: str) -> tuple[str, Callable, Version]:
    """
    Extracts the operator and version number from a version constraint.

    Parameters
    ----------
    constraint
        Version constraint described as a comparison operator followed by the version
        number. For example, "<=1.0.0" would indicate that this method is only
        compatible with server versions less than or equal to 1.0.0.

    Returns
    -------
    :
        The complement name of the constraint.
    :
        The complement operator of the constraint.
    :
        The version object of the constraint.

    """
    if "<=" == constraint[:2]:
        complement_name = ">"
        complement_operator = operator.gt
        constraint = constraint[2:]
    elif ">=" == constraint[:2]:
        complement_name = "<"
        complement_operator = operator.lt
        constraint = constraint[2:]
    elif "==" == constraint[:2]:
        complement_name = "!="
        complement_operator = operator.ne
        constraint = constraint[2:]
    elif "!=" == constraint[:2]:
        complement_name = "=="
        complement_operator = operator.eq
        constraint = constraint[2:]
    elif "<" == constraint[0]:
        complement_name = ">="
        complement_operator = operator.ge
        constraint = constraint[1:]
    elif ">" == constraint[0]:
        complement_name = "<="
        complement_operator = operator.le
        constraint = constraint[1:]
    else:
        raise ValueError(f"Constraint {constraint} not recognized.")
    constraint = parse_version(constraint)
    return complement_name, complement_operator, constraint


@parametrized
def check_version_compatibility(
    method: Callable, method_constraint: str = None, kwarg_use_constraints: dict = None
) -> Callable:
    """
    This decorator is used to check the compatibility features in the client and
    server versions. If the server version is not compatible with the constraint, an
    error will be raised.

    Parameters
    ----------
    method
        Method to be decorated.
    method_constraint
        Version constraint for the method, described as a comparison operator
        followed by the version number. For example, "<=1.0.0" would indicate that this
        method is only compatible with server versions less than or equal to 1.0.0.
    kwarg_use_constraints
        Dictionary with some number of the method's keyword arguments as keys and
        version constraints as values. Version constraints are described as a
        comparison operator followed by the version number. For example, "<=1.0.0"
        would indicate that the keyword argument is only compatible with server versions
        less than or equal to 1.0.0. An error will be raised only if the user both
        provides the keyword argument (even if passing in the default value!) and the
        server version is not compatible with the constraint.
    """

    @wraps(method)
    def wrapper(*args, **kwargs):
        self = args[0]
        server_version = parse_version(self.version)

        if method_constraint is not None:
            complement_name, complement_operator, constraint = _extract_constraint_info(
                method_constraint
            )

            if complement_operator(server_version, constraint):
                msg = (
                    f"Method {method.__name__} is not permitted "
                    f"for server version {complement_name}{constraint}, your server "
                    f"version is {server_version}. Contact your system "
                    "administrator to update the server version."
                )
                raise ValueError(msg)

        for kwarg, value in kwarg_use_constraints.items():
            complement_name, complement_operator, constraint = _extract_constraint_info(
                value
            )

            if kwarg in kwargs and complement_operator(server_version, constraint):
                msg = (
                    f"Use of {kwarg} in {method.__name__} is not permitted "
                    f"for server version {complement_name}{constraint}, your server "
                    f"version is {server_version}. Contact your system "
                    "administrator to update the server version."
                )
                raise ValueError(msg)

        out = method(*args, **kwargs)
        return out

    return wrapper
