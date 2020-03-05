import numpy as np
import requests
import datetime
import time

from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg
from .auth import AuthClient


def package_bounds(bounds):
    bounds_str = []
    for b in bounds:
        bounds_str.append("-".join(str(b2) for b2 in b))
    bounds_str = "_".join(bounds_str)
    return bounds_str


class ChunkedGraphClient(object):
    """Client to interface with the PyChunkedGraph service

    Parameters
    ----------
    server_address : str or None, optional
        URL where the PyChunkedGraph service is running. If None, defaults to www.dynamicannotationframework.com
    dataset_name : str or None, optional
        Name of the dataset. If None, requires specification of the table name. By default, None.
    table_name : str or None, optional
        Name of the chunkedgraph table associated with the dataset. If the dataset_name is specified and table name is not, this can be looked up automatically. By default, None.
    auth_client : auth.AuthClient or None, optional
        Instance of an AuthClient with token to handle authorization. If None, does not specify a token.
    timestamp : datetime.datetime or None, optional
        Default UTC timestamp to use for chunkedgraph queries. 
    """

    def __init__(self, server_address=None, dataset_name=None,
                 table_name=None, auth_client=None, timestamp=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address
        if table_name is None:
            info_client = infoservice.InfoServiceClient(server_address=self._server_address)
            pcg_vs = info_client.pychunkedgraph_viewer_source(dataset_name=dataset_name)
            table_name = pcg_vs.split('/')[-1]
        self.table_name = table_name
        self._default_timestamp = timestamp

        if auth_client is None:
            auth_client = AuthClient()

        self.session = requests.Session()
        self.session.headers.update(auth_client.request_header)

        self.info_cache = dict()

        self._default_url_mapping = {"cg_server_address": self._server_address,
                                     "table_id": self.table_name}

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def get_root_id(self, supervoxel_id, timestamp=None):
        """Get the root id for a specified supervoxel

        Parameters
        ----------
        supervoxel_id : np.uint64
            Supervoxel id value
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by default None. If None, uses the current time.

        Returns
        -------
        np.uint64
            Root ID containing the supervoxel.
        """
        if timestamp is None:
            if self._default_timestamp is not None:
                timestamp = self._default_timestamp
            else:
                timestamp = datetime.datetime.utcnow()

        endpoint_mapping = self.default_url_mapping
        url = cg['handle_root'].format_map(endpoint_mapping)
        url = f"{url}?timestamp={time.mktime(timestamp.timetuple())}"

        response = self.session.post(url, json=[supervoxel_id])

        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_merge_log(self, root_id):
        """Returns the merge log for a given object

        Parameters
        ----------
        root_id : np.uint64
            Root id of an object to get merge information.

        Returns
        -------
        list
            List of merge events in the history of the object.
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['merge_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])

        assert(response.status_code == 200)
        return response.json()

    def get_change_log(self, root_id):
        """Get the change log (splits and merges) for an object

        Parameters
        ----------
        root_id : np.uint64
            Object root id to look up

        Returns
        -------
        list
            List of split and merge events in the object history
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['change_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])

        assert(response.status_code == 200)
        return response.json()

    def get_leaves(self, root_id, bounds=None):
        """Get all supervoxels for a root_id

        Parameters
        ----------
        root_id : np.uint64
            Root id to query
        bounds: np.array or None, optional
            If specified, returns supervoxels within a 3x2 numpy array of bounds [[minx,maxx],[miny,maxy],[minz,maxz]]
            If None, finds all supervoxels.

        Returns
        -------
        list
            List of supervoxel ids
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['leaves_from_root'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)

        response = self.session.post(url, json=[root_id], params=query_d)

        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_children(self, node_id):
        """Get the children of a node in the hierarchy

        Parameters
        ----------
        node_id : np.uint64
            Node id to query

        Returns
        -------
        list
            List of np.uint64 ids of child nodes.
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['node_id'] = node_id
        url = cg['handle_children'].format_map(endpoint_mapping)

        response = self.session.post(url)

        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_contact_sites(self, root_id, bounds, calc_partners=False):
        """Get contacts for a root id

        Parameters
        ----------
        root_id : np.uint64
            Object root id
        bounds: np.array
            Bounds within a 3x2 numpy array of bounds [[minx,maxx],[miny,maxy],[minz,maxz]] for which to find contacts. Running this query without bounds is too slow.
        calc_partners : bool, optional
            If True, get partner root ids. By default, False.
        Returns
        -------
        dict
            Dict relating ids to contacts
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['contact_sites'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)
        query_d['partners'] = calc_partners
        response = self.session.post(url, json=[root_id], params=query_d)
        contact_d = response.json()
        return {int(k): v for k, v in contact_d.items()}

    @property
    def cloudvolume_path(self):
        return cg['cloudvolume_path'].format_map(self.default_url_mapping)
