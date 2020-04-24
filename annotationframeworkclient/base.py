import requests
import json


def _api_versions(server_name, server_address, endpoints_common, auth_header):
    """Asks a server what API versions are available, if possible
    """
    url_mapping = {server_name: server_address}
    url_base = endpoints_common.get('get_api_versions', None)
    if url_base is not None:
        url = url_base.format_map(url_mapping)
        response = requests.get(url, headers=auth_header)
        response.raise_for_status()
        return response.json()
    else:
        return None


def _api_endpoints(api_version, server_name, server_address, endpoints_common, endpoint_versions, auth_header):
    "Gets the latest client API version"
    if api_version == 'latest':
        try:
            avail_vs_server = _api_versions(server_name,
                                            server_address,
                                            endpoints_common,
                                            auth_header
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
    endpoints.update(endpoint_versions[api_version])
    return endpoints


class ClientBase(object):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name,
                 ):
        self._server_address = server_address
        self._default_url_mapping = {server_name: self._server_address}

        self.session = requests.Session()
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
