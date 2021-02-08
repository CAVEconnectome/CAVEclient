import numpy as np
import requests
import datetime
import time
import json
from . import endpoints
from . import infoservice
from .endpoints import chunkedgraph_api_versions, chunkedgraph_endpoints_common, default_global_server_address
from .base import _api_endpoints, _api_versions, ClientBase, handle_response
from .auth import AuthClient

SERVER_KEY = 'cg_server_address'


class CGEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def package_bounds(bounds):
    if (bounds.shape != (3,2)):
        raise ValueError('Bounds must be a 3x2 matrix (x,y,z) x (min,max) in chunkedgraph resolution voxel units')
    
    bounds_str = []
    for b in bounds:
        bounds_str.append("-".join(str(b2) for b2 in b))
    bounds_str = "_".join(bounds_str)
    return bounds_str


def package_timestamp(timestamp):
    if timestamp is None:
        query_d = None
    else:
        query_d = {'timestamp': time.mktime(timestamp.timetuple())}
    return query_d


def ChunkedGraphClient(server_address=None,
                       table_name=None,
                       auth_client=None,
                       api_version='latest',
                       timestamp=None
                       ):
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header

    endpoints, api_version = _api_endpoints(api_version, SERVER_KEY, server_address,
                                            chunkedgraph_endpoints_common, chunkedgraph_api_versions, auth_header)

    ChunkedClient = client_mapping[api_version]
    return ChunkedClient(server_address,
                         auth_header,
                         api_version,
                         endpoints,
                         SERVER_KEY,
                         timestamp=timestamp,
                         table_name=table_name)


class ChunkedGraphClientV1(ClientBase):
    """ ChunkedGraph Client for the v1 API
    """

    def __init__(self, server_address, auth_header, api_version, endpoints, server_key=SERVER_KEY, timestamp=None, table_name=None):
        super(ChunkedGraphClientV1, self).__init__(server_address,
                                                   auth_header,
                                                   api_version,
                                                   endpoints,
                                                   server_key)
        self._default_url_mapping['table_id'] = table_name
        self._default_timestamp = timestamp
        self._table_name = table_name
        self._segmentation_info = None

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @property
    def table_name(self):
        return self._table_name

    def _process_timestamp(self, timestamp):
        """Process timestamp with default logic
        """
        if timestamp is None:
            if self._default_timestamp is not None:
                return self._default_timestamp
            else:
                return datetime.datetime.utcnow()
        else:
            return timestamp

    def get_roots(self, supervoxel_ids, timestamp=None, stop_level=None):
        """Get the root id for a specified supervoxel

        Parameters
        ----------
        supervoxel_ids : np.array(np.uint64)
            Supervoxel ids values
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by default None. If None, uses the current time.
        stop_level : int or None, optional
            If True, looks up ids only up to a given stop level. Default is None.

        Returns
        -------
        np.array(np.uint64)
            Root IDs containing each supervoxel.
        """
        if stop_level is not None:
            return self._get_level_roots(supervoxel_ids, stop_level=stop_level, timestamp=timestamp)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['get_roots'].format_map(endpoint_mapping)
        query_d = package_timestamp(self._process_timestamp(timestamp))
        data = np.array(supervoxel_ids, dtype=np.uint64).tobytes()

        response = self.session.post(url, data=data, params=query_d)
        handle_response(response, as_json=False)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_root_id(self, supervoxel_id, timestamp=None, level2=False):
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
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['supervoxel_id'] = supervoxel_id

        url = self._endpoints['handle_root'].format_map(endpoint_mapping)
        query_d = package_timestamp(self._process_timestamp(timestamp))
        if level2:
            query_d['stop_layer'] = 2

        response = self.session.get(url, params=query_d)
        handle_response(response, as_json=False)
        return np.int64(response.json()['root_id'])

    def _get_level_roots(self, supervoxel_ids, stop_level, timestamp=None):
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
        endpoint_mapping = self.default_url_mapping

        url = self._endpoints['handle_roots'].format_map(endpoint_mapping)
        query_d = package_timestamp(self._process_timestamp(timestamp))
        query_d['stop_layer'] = stop_level
        data = {'node_ids': supervoxel_ids}
        response = self.session.post(
            url, data=json.dumps(data, cls=CGEncoder), params=query_d)
        return handle_response(response, as_json=True).get('root_ids', None)

    def get_merge_log(self, root_id):
        """Get the merge log (splits and merges) for an object

        Parameters
        ----------
        root_id : np.uint64
            Object root id to look up

        Returns
        -------
        list
            List of merge events in the history of the object.
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = self._endpoints['merge_log'].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

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
        url = self._endpoints['change_log'].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

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
        url = self._endpoints['leaves_from_root'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)

        response = self.session.get(url, params=query_d)
        handle_response(response, as_json=False)
        return np.int64(response.json()['leaf_ids'])

    def do_merge(self, supervoxels, coords, resolution=(4, 4, 40)):
        """Perform a merge on the chunkeded graph

        Args:
            supervoxels (iterable): a N long list of supervoxels to merge
            coords (np.array): a Nx3 array of coordinates of the supervoxels in units of resolution
            resolution (tuple, optional): what to multiple the coords by to get nm. Defaults to (4,4,40).
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['do_merge'].format_map(endpoint_mapping)

        data = []
        for svid, coor in zip(supervoxels, coords):
            row = np.concatenate([[svid], np.array(coor)*resolution])
            data.append(row)
        params = {"priority": False}
        response = self.session.post(url, data=json.dumps(data, cls=CGEncoder),
                                     params=params,
                                     headers={'Content-Type': 'application/json'})
        handle_response(response)

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
        url = self._endpoints['handle_children'].format_map(endpoint_mapping)

        response = self.session.post(url)
        handle_response(response, as_json=False)
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
        url = self._endpoints['contact_sites'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)
        query_d['partners'] = calc_partners
        response = self.session.post(url, json=[root_id], params=query_d)
        contact_d = handle_response(response)
        return {int(k): v for k, v in contact_d.items()}

    def find_path(self, root_id, src_pt, dst_pt, precision_mode=False):
        """find a path between two locations on a root_id using the supervoxel lvl2 graph.

        Args:
            root_id (np.int64): the root id to search on
            src_pt (np.array): len(3) xyz location of the start location in nm
            dst_pt ([type]): len(3) xyz location of the end location in nm
            precision_mode (bool, optional): Whether to perform the search in precision mode. Defaults to False.

        Returns:
            centroids_list: centroids
            l2_path: l2_path
            failed_l2_ids: failed_l2_ids
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = self._endpoints['find_path'].format_map(endpoint_mapping)
        query_d = {}
        query_d['precision_mode'] = precision_mode

        nodes = [[root_id] + src_pt.tolist(),
                 [root_id] + dst_pt.tolist()]

        response = self.session.post(url,
                                     data=json.dumps(nodes, cls=CGEncoder),
                                     params=query_d,
                                     headers={'Content-Type': 'application/json'})
        resp_d = handle_response(response)
        centroids = np.array(resp_d['centroids_list'])
        failed_l2_ids = np.array(resp_d['failed_l2_ids'], dtype=np.uint64)
        l2_path = np.array(resp_d['l2_path'])

        return centroids, l2_path, failed_l2_ids
    
    def get_subgraph(self, root_id, bounds):
        """Get subgraph of root id within a bounding box

        Args:
            root_id ([int64]): root (or seg_id/node_id) of chunkedgraph to query
            bounds ([np.array]): 3x2 bounding box (x,y,z)x (min,max) in chunkedgraph coordinates
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = self._endpoints['get_subgraph'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)
        
        response = self.session.get(url, params=query_d)
        rd = handle_response(response)
        return np.int64(rd['nodes']), np.double(rd['affinities']), np.int32(rd['areas'])

    def level2_chunk_graph(self, root_id):
        """Get graph of level 2 chunks, the smallest agglomeration level above supervoxels.

        Parameters
        ----------
        root_id : int
            Root id of object

        Returns
        -------
        edge_list : list
            Edge array of level 2 ids
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = self._endpoints['lvl2_graph'].format_map(endpoint_mapping)
        r = handle_response(self.session.get(url))
        return r['edge_graph']

    def remesh_level2_chunks(self, chunk_ids):
        """Submit specific level 2 chunks to be remeshed in case of a problem.

        Parameters
        ----------
        chunk_ids : list
            List of level 2 chunk ids.
        """

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['remesh_level2_chunks'].format_map(
            endpoint_mapping)
        data = {'new_lvl2_ids': [int(x) for x in chunk_ids]}
        r = self.session.post(url, json=data)
        r.raise_for_status()

    @property
    def cloudvolume_path(self):
        return self._endpoints['cloudvolume_path'].format_map(self.default_url_mapping)

    @property
    def segmentation_info(self):
        """Complete segmentation metadata
        """
        if self._segmentation_info is None:
            url = self._endpoints['info'].format_map(self.default_url_mapping)
            response = self.session.get(url)
            self._segmentation_info = handle_response(response)
        return self._segmentation_info

    @property
    def base_resolution(self):
        """MIP 0 resolution for voxels assumed by the ChunkedGraph

        Returns
        -------
        list
            3-long list of x/y/z voxel dimensions in nm
        """
        return self.segmentation_info['scales'][0].get('resolution')


class ChunkedGraphClientLegacy(ClientBase):
    """Client to interface with the PyChunkedGraph service

    Parameters
    ----------
    server_address : str or None, optional
        URL where the PyChunkedGraph service is running. If None, defaults to www.dynamicannotationframework.com
    table_name : str or None, optional
        Name of the chunkedgraph table associated with the dataset. If the datastack_name is specified and table name is not, this can be looked up automatically. By default, None.
    auth_client : auth.AuthClient or None, optional
        Instance of an AuthClient with token to handle authorization. If None, does not specify a token.
    timestamp : datetime.datetime or None, optional
        Default UTC timestamp to use for chunkedgraph queries.
    """

    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_key=SERVER_KEY,
                 timestamp=None,
                 table_name=None):
        super(ChunkedGraphClientLegacy, self).__init__(server_address,
                                                       auth_header,
                                                       api_version,
                                                       endpoints,
                                                       server_key)

        self._default_url_mapping['table_id'] = table_name
        self._default_timestamp = timestamp
        self._table_name = table_name

    @ property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @ property
    def table_name(self):
        return self._table_name

    def get_roots(self, supervoxel_ids, timestamp=None):
        """Get the root id for a specified supervoxel

        Parameters
        ----------
        supervoxel_ids : np.array(np.uint64)
            Supervoxel ids values
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by default None. If None, uses the current time.

        Returns
        -------
        np.array(np.uint64)
            Root IDs containing each supervoxel.
        """
        if timestamp is None:
            if self._default_timestamp is not None:
                timestamp = self._default_timestamp
            else:
                timestamp = datetime.datetime.utcnow()

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['get_roots'].format_map(endpoint_mapping)

        if timestamp is None:
            timestamp = self._default_timestamp
        if timestamp is not None:
            query_d = {
                'timestamp': time.mktime(timestamp.timetuple())
            }
        else:
            query_d = None
        data = np.array(supervoxel_ids, dtype=np.uint64).tobytes()

        response = self.session.post(url, data=data, params=query_d)
        handle_response(response, as_json=False)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_root_id(self, supervoxel_id, timestamp=None, level2=False):
        """Get the root id for a specified supervoxel

        Parameters
        ----------
        supervoxel_id : np.uint64
            Supervoxel id value
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by default None. If None, uses the current time.
        level2 : bool, optional
            Return true to get the level 2 id of a supervoxel.

        Returns
        -------
        np.uint64
            Root ID containing the supervoxel (or level 2 id if requested)
        """
        if timestamp is None:
            if self._default_timestamp is not None:
                timestamp = self._default_timestamp
            else:
                timestamp = datetime.datetime.utcnow()

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['supervoxel_id'] = supervoxel_id
        url = self._endpoints['handle_root'].format_map(endpoint_mapping)

        if timestamp is None:
            timestamp = self._default_timestamp

        query_d = {}
        if timestamp is not None:
            query_d['timestamp'] = time.mktime(timestamp.timetuple())
        if level2:
            query_d['stop_layer'] = 2

        response = self.session.get(url, params=query_d)
        return np.int64(handle_response(response)['root_id'])

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
        url = self._endpoints['merge_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])
        return handle_response(response)

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
        url = self._endpoints['change_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])
        return handle_response(response)

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
        url = self._endpoints['leaves_from_root'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)

        response = self.session.get(url, params=query_d)
        return np.int64(handle_response(response)['leaf_ids'])

    def do_merge(self, supervoxels, coords, resolution=(4, 4, 40)):
        """Perform a merge on the chunkeded graph

        Args:
            supervoxels (iterable): a N long list of supervoxels to merge
            coords (np.array): a Nx3 array of coordinates of the supervoxels in units of resolution
            resolution (tuple, optional): what to multiple the coords by to get nm. Defaults to (4,4,40).
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['do_merge'].format_map(endpoint_mapping)

        data = []
        for svid, coor in zip(supervoxels, coords):
            row = np.concatenate([[svid], np.array(coor)*resolution])
            data.append(row)
        params = {"priority": False}
        response = self.session.post(url, data=json.dumps(data, cls=CGEncoder),
                                     params=params,
                                     headers={'Content-Type': 'application/json'})
        return handle_response(response)

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
        url = self._endpoints['handle_children'].format_map(endpoint_mapping)

        response = self.session.post(url)
        handle_response(response, as_json=False)
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
        url = self._endpoints['contact_sites'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)
        query_d['partners'] = calc_partners
        response = self.session.post(url, json=[root_id], params=query_d)
        contact_d = handle_response(response)
        return {int(k): v for k, v in contact_d.items()}

    def find_path(self, root_id, src_pt, dst_pt, precision_mode=False):
        """find a path between two locations on a root_id using the supervoxel lvl2 graph.

        Args:
            root_id (np.int64): the root id to search on
            src_pt (np.array): len(3) xyz location of the start location in nm
            dst_pt ([type]): len(3) xyz location of the end location in nm
            precision_mode (bool, optional): Whether to perform the search in precision mode. Defaults to False.

        Returns:
            centroids_list: centroids
            l2_path: l2_path
            failed_l2_ids: failed_l2_ids
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = self._endpoints['find_path'].format_map(endpoint_mapping)
        query_d = {}
        query_d['precision_mode'] = precision_mode

        nodes = [[root_id] + src_pt.tolist(),
                 [root_id] + dst_pt.tolist()]

        response = self.session.post(url,
                                     data=json.dumps(nodes, cls=CGEncoder),
                                     params=query_d,
                                     headers={'Content-Type': 'application/json'})
        resp_d = handle_response(response)
        centroids = np.array(resp_d['centroids_list'])
        failed_l2_ids = np.array(resp_d['failed_l2_ids'], dtype=np.uint64)
        l2_path = np.array(resp_d['l2_path'])

        return centroids, l2_path, failed_l2_ids


client_mapping = {0: ChunkedGraphClientLegacy,
                  1: ChunkedGraphClientV1,
                  'latest': ChunkedGraphClientV1}
