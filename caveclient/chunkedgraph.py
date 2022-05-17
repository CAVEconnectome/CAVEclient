import numpy as np
import pandas as pd
import requests
import datetime
import time
import json
import pytz
from . import endpoints
from . import infoservice
from .endpoints import (
    chunkedgraph_api_versions,
    chunkedgraph_endpoints_common,
    default_global_server_address,
)
from .base import (
    _api_endpoints,
    _api_versions,
    ClientBase,
    BaseEncoder,
    handle_response,
)
from .auth import AuthClient
from typing import Iterable
from urllib.parse import urlencode
import networkx as nx


SERVER_KEY = "cg_server_address"


def package_bounds(bounds):
    if bounds.shape != (3, 2):
        raise ValueError(
            "Bounds must be a 3x2 matrix (x,y,z) x (min,max) in chunkedgraph resolution voxel units"
        )

    bounds_str = []
    for b in bounds:
        bounds_str.append("-".join(str(b2) for b2 in b))
    bounds_str = "_".join(bounds_str)
    return bounds_str


def package_timestamp(timestamp, name="timestamp"):
    if timestamp is None:
        query_d = {}
    else:
        if timestamp.tzinfo is None:
            timestamp = pytz.UTC.localize(timestamp)
        else:
            timestamp = timestamp.astimezone(datetime.timezone.utc)

        query_d = {name: timestamp.timestamp()}
    return query_d


def root_id_int_list_check(
    root_id,
    make_unique=False,
):
    if isinstance(root_id, int) or isinstance(root_id, np.uint64):
        root_id = [root_id]
    elif isinstance(root_id, str):
        try:
            root_id = np.uint64(root_id)
        except ValueError:
            raise ValueError(
                "When passing a string for 'root_id' make sure the string can be converted to a uint64"
            )
    elif isinstance(root_id, np.ndarray) or isinstance(root_id, list):
        if make_unique:
            root_id = np.unique(root_id).astype(np.uint64)
        else:
            root_id = np.array(root_id, dtype=np.uint64)
    else:
        raise ValueError("root_id has to be list or uint64")

    return root_id


def ChunkedGraphClient(
    server_address=None,
    table_name=None,
    auth_client=None,
    api_version="latest",
    timestamp=None,
    verify=True,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
):
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header

    endpoints, api_version = _api_endpoints(
        api_version,
        SERVER_KEY,
        server_address,
        chunkedgraph_endpoints_common,
        chunkedgraph_api_versions,
        auth_header,
    )

    ChunkedClient = client_mapping[api_version]
    return ChunkedClient(
        server_address,
        auth_header,
        api_version,
        endpoints,
        SERVER_KEY,
        timestamp=timestamp,
        table_name=table_name,
        verify=verify,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
    )


class ChunkedGraphClientV1(ClientBase):
    """ChunkedGraph Client for the v1 API"""

    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_key=SERVER_KEY,
        timestamp=None,
        table_name=None,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
    ):
        super(ChunkedGraphClientV1, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_key,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )
        self._default_url_mapping["table_id"] = table_name
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
        """Process timestamp with default logic"""
        if timestamp is None:
            if self._default_timestamp is not None:
                return self._default_timestamp
            else:
                return datetime.datetime.utcnow()
        else:
            return timestamp

    def get_roots(self, supervoxel_ids, timestamp=None, stop_layer=None):
        """Get the root id for a specified supervoxel

        Parameters
        ----------
        supervoxel_ids : np.array(np.uint64)
            Supervoxel ids values
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by default None. If None, uses the current time.
        stop_layer : int or None, optional
            If True, looks up ids only up to a given stop layer. Default is None.

        Returns
        -------
        np.array(np.uint64)
            Root IDs containing each supervoxel.
        """

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["get_roots"].format_map(endpoint_mapping)
        query_d = package_timestamp(self._process_timestamp(timestamp))
        if stop_layer is not None:
            query_d["stop_layer"] = stop_layer
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
        endpoint_mapping["supervoxel_id"] = supervoxel_id

        url = self._endpoints["handle_root"].format_map(endpoint_mapping)
        query_d = package_timestamp(self._process_timestamp(timestamp))
        if level2:
            query_d["stop_layer"] = 2

        response = self.session.get(url, params=query_d)
        return np.int64(handle_response(response, as_json=True)["root_id"])

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
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["merge_log"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def get_change_log(self, root_id, filtered=True):
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
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["change_log"].format_map(endpoint_mapping)
        params = {"filtered": filtered}
        response = self.session.get(url, params=params)

        return handle_response(response)

    def get_user_operations(
        self,
        user_id: int,
        timestamp_start: datetime.datetime,
        include_undo: bool = True,
        timestamp_end: datetime.datetime = None,
    ):
        """get operation details for a user_id

        Args:
            user_id (int): userID to query (use 0 for all users [admin only])
            timestamp_start (datetime.datetime, optional): timestamp to start filter (UTC).
            include_undo (bool, optional): whether to include undos. Defaults to True.
            timestamp_end (datetime.datetime, optional): timestamp to end filter (UTC). Defaults to now.
        """
        endpoint_mapping = self.default_url_mapping

        url = self._endpoints["user_operations"].format_map(endpoint_mapping)
        params = {"include_undo": include_undo}

        if user_id > 0:
            params = {"user_id": user_id}
        if timestamp_start is not None:
            params.update(
                package_timestamp(
                    self._process_timestamp(timestamp_start), "start_time"
                )
            )
        if timestamp_end is not None:
            params.update(
                package_timestamp(self._process_timestamp(timestamp_end), "end_time")
            )
        response = self.session.get(url, params=params)

        d = handle_response(response)
        df = pd.DataFrame(d)
        df["timestamp"] = df["timestamp"].map(
            lambda x: datetime.datetime.fromtimestamp(x / 1000, pytz.UTC)
        )
        return df

    def get_tabular_change_log(self, root_ids, filtered=True):
        """Get a detailed changelog for neurons

        Parameters
        ----------
        root_ids : list of np.uint64
            Object root ids to look up

        Returns
        -------
            dict of dataframe
        """
        root_ids = [int(r) for r in np.unique(root_ids)]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["root_ids"] = root_ids
        url = self._endpoints["tabular_change_log"].format_map(endpoint_mapping)
        params = {"filtered": filtered}
        data = json.dumps({"root_ids": root_ids}, cls=BaseEncoder)

        response = self.session.get(url, data=data, params=params)
        res_dict = handle_response(response)

        changelog_dict = {}
        for k in res_dict.keys():
            changelog_dict[int(k)] = pd.DataFrame(json.loads(res_dict[k]))

        return changelog_dict

    def get_leaves(self, root_id, bounds=None, stop_layer: int = None):
        """Get all supervoxels for a root_id

        Parameters
        ----------
        root_id : np.uint64
            Root id to query
        bounds: np.array or None, optional
            If specified, returns supervoxels within a 3x2 numpy array of bounds [[minx,maxx],[miny,maxy],[minz,maxz]]
            If None, finds all supervoxels.
        stop_layer: int, optional
            If specified, returns chunkedgraph nodes at layer =stop_layer
            default will be stop_layer=1 (supervoxels)

        Returns
        -------
        list
            List of supervoxel ids (or nodeids if stop_layer>1)
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["leaves_from_root"].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d["bounds"] = package_bounds(bounds)
        if stop_layer is not None:
            query_d["stop_layer"] = int(stop_layer)
        response = self.session.get(url, params=query_d)
        return np.int64(handle_response(response)["leaf_ids"])

    def do_merge(self, supervoxels, coords, resolution=(4, 4, 40)):
        """Perform a merge on the chunkeded graph

        Args:
            supervoxels (iterable): a N long list of supervoxels to merge
            coords (np.array): a Nx3 array of coordinates of the supervoxels in units of resolution
            resolution (tuple, optional): what to multiple the coords by to get nm. Defaults to (4,4,40).
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["do_merge"].format_map(endpoint_mapping)

        data = []
        for svid, coor in zip(supervoxels, coords):
            pos = np.array(coor) * resolution
            row = [svid, pos[0], pos[1], pos[2]]
            data.append(row)
        params = {"priority": False}
        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            params=params,
            headers={"Content-Type": "application/json"},
        )
        handle_response(response)

    def preview_split(
        self,
        source_points,
        sink_points,
        root_id,
        source_supervoxels=None,
        sink_supervoxels=None,
        return_additional_ccs=False,
    ):
        """Get supervoxel connected components from a preview multicut split.

        Parameters
        ----------
        source_points : array or list
            Nx3 list or array of 3d points in nm coordinates for source points (red).
        sink_points : array or list
            Mx3 list or array of 3d points in nm coordinates for sink points (blue).
        root_id : int
            root id of object to do split preview.
        source_supervoxels : array, list or None, optional
            If providing source supervoxels, an N-length array of supervoxel ids or Nones matched to source points. If None, treats as a full array of Nones. By default None
        sink_supervoxels : array, list or None, optional
            If providing sink supervoxels, an M-length array of supervoxel ids or Nones matched to source points. If None, treats as a full array of Nones. By default None
        return_additional_ccs : bool, optional
            If True, returns any additional connected components beyond the ones with source and sink points. In most situations, this can be ignored. By default, False.

        Returns
        -------
        source_connected_component
            List of supervoxel ids in the component with the most source points.
        sink_connected_component
            List of supervoxel ids in the component with the most sink points.
        successful_split
            Boolean value that is True if the split worked.
        other_connected_components (optional)
            List of lists of supervoxel ids for any other resulting connected components. Only returned if `return_additional_ccs` is True.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["preview_split"].format_map(endpoint_mapping)

        categories = ["sources", "sinks"]
        pts = [source_points, sink_points]
        svs = [source_supervoxels, sink_supervoxels]
        for pt_list, sv_list in zip(pts, svs):
            if sv_list is not None:
                if len(pt_list) != len(sv_list):
                    raise ValueError(
                        "If supervoxels are provided, they must have the same length as points"
                    )

        data = {}
        for cat, pt_list, sv_list in zip(categories, pts, svs):
            if sv_list is None:
                sv_list = [None] * len(pt_list)
            sv_list = [x if x is not None else root_id for x in sv_list]

            out = []
            for svid, pt in zip(sv_list, pt_list):
                out.append([svid, pt[0], pt[1], pt[2]])

            data[cat] = out

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json"},
        )
        r = handle_response(response)
        source_cc = r["supervoxel_connected_components"][0]
        sink_cc = r["supervoxel_connected_components"][1]
        if len(r["supervoxel_connected_components"]) == 2:
            other_ccs = []
        else:
            other_ccs = r["supervoxel_connected_components"][2:]

        success = not r["illegal_split"]

        if return_additional_ccs:
            return source_cc, sink_cc, success, other_ccs
        else:
            return source_cc, sink_cc, success

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
        endpoint_mapping["root_id"] = node_id
        url = self._endpoints["handle_children"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return np.array(handle_response(response)["children_ids"], dtype=np.int64)

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
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["contact_sites"].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d["bounds"] = package_bounds(bounds)
        query_d["partners"] = calc_partners
        response = self.session.get(url, json=[root_id], params=query_d)
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
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["find_path"].format_map(endpoint_mapping)
        query_d = {}
        query_d["precision_mode"] = precision_mode

        nodes = [[root_id] + src_pt.tolist(), [root_id] + dst_pt.tolist()]

        response = self.session.post(
            url,
            data=json.dumps(nodes, cls=BaseEncoder),
            params=query_d,
            headers={"Content-Type": "application/json"},
        )
        resp_d = handle_response(response)
        centroids = np.array(resp_d["centroids_list"])
        failed_l2_ids = np.array(resp_d["failed_l2_ids"], dtype=np.uint64)
        l2_path = np.array(resp_d["l2_path"])

        return centroids, l2_path, failed_l2_ids

    def get_subgraph(self, root_id, bounds):
        """Get subgraph of root id within a bounding box

        Args:
            root_id ([int64]): root (or seg_id/node_id) of chunkedgraph to query
            bounds ([np.array]): 3x2 bounding box (x,y,z)x (min,max) in chunkedgraph coordinates
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["get_subgraph"].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d["bounds"] = package_bounds(bounds)

        response = self.session.get(url, params=query_d)
        rd = handle_response(response)
        return np.int64(rd["nodes"]), np.double(rd["affinities"]), np.int32(rd["areas"])

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
        endpoint_mapping["root_id"] = root_id
        url = self._endpoints["lvl2_graph"].format_map(endpoint_mapping)
        r = handle_response(self.session.get(url))
        return r["edge_graph"]

    def remesh_level2_chunks(self, chunk_ids):
        """Submit specific level 2 chunks to be remeshed in case of a problem.

        Parameters
        ----------
        chunk_ids : list
            List of level 2 chunk ids.
        """

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["remesh_level2_chunks"].format_map(endpoint_mapping)
        data = {"new_lvl2_ids": [int(x) for x in chunk_ids]}
        r = self.session.post(url, json=data)
        r.raise_for_status()

    def get_operation_details(self, operation_ids: Iterable[int]):
        """get the details of a list of operations

        Args:
            operation_ids (Iterable[int]): list of operation IDss

        Returns:
            dict: a dict of dictss of operation info, keys are operationids
            values are a dictionary of operation info for the operation
        """
        if isinstance(operation_ids, np.ndarray):
            operation_ids = operation_ids.tolist()

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["operation_details"].format_map(endpoint_mapping)
        query_d = {"operation_ids": operation_ids}
        query_str = urlencode(query_d)
        url = url + "?" + query_str
        r = self.session.get(url)
        r.raise_for_status()
        return r.json()

    def get_lineage_graph(
        self, root_id, timestamp_past=None, timestamp_future=None, as_nx_graph=False
    ):
        """Returns the lineage graph for a root id, optionally cut off in the past or the future.

        Parameters
        ----------
        root_id : int
            Object root id
        timestamp_past : datetime.datetime or None, optional
            Cutoff for the lineage graph backwards in time. By default, None.
        timestamp_future : datetime.datetime or None, optional
            Cutoff for the lineage graph going forwards in time. By default, None.
        as_nx_graph: bool
            if True, a networkx graph is returned

        Returns
        -------
        dict
            Dictionary describing the lineage graph and operations for the root id.
        """
        root_id = root_id_int_list_check(root_id, make_unique=True)

        endpoint_mapping = self.default_url_mapping

        params = {}
        if timestamp_past is not None:
            params.update(package_timestamp(timestamp_past, name="timestamp_past"))
        if timestamp_future is not None:
            params.update(package_timestamp(timestamp_future, name="timestamp_future"))

        url = self._endpoints["handle_lineage_graph"].format_map(endpoint_mapping)
        data = json.dumps({"root_ids": root_id}, cls=BaseEncoder)
        r = handle_response(self.session.post(url, data=data, params=params))

        if as_nx_graph:
            return nx.node_link_graph(r)
        else:
            return r

    def get_latest_roots(self, root_id, timestamp_future=None):
        """Returns root ids that are the latest successors of a given root id.

        Parameters
        ----------
        root_id : int
            Object root id
        timestamp_future : datetime.datetime or None, optional
            Cutoff for the search going forwards in time. By default, None.

        Returns
        -------
        np.ndarray
            1d array with all latest successors
        """
        root_id = root_id_int_list_check(root_id, make_unique=True)

        timestamp_past = self.get_root_timestamps(root_id).min()

        lineage_graph = self.get_lineage_graph(
            root_id,
            timestamp_past=timestamp_past,
            timestamp_future=timestamp_future,
            as_nx_graph=True,
        )

        out_degree_dict = dict(lineage_graph.out_degree)
        nodes = np.array(list(out_degree_dict.keys()))
        out_degrees = np.array(list(out_degree_dict.values()))
        return nodes[out_degrees == 0]

    def get_original_roots(self, root_id, timestamp_past=None):
        """Returns root ids that are the latest successors of a given root id.

        Parameters
        ----------
        root_id : int
            Object root id
        timestamp_past : datetime.datetime or None, optional
            Cutoff for the search going backwards in time. By default, None.

        Returns
        -------
        np.ndarray
            1d array with all latest successors
        """
        root_id = root_id_int_list_check(root_id, make_unique=True)

        timestamp_future = self.get_root_timestamps(root_id).max()

        lineage_graph = self.get_lineage_graph(
            root_id,
            timestamp_past=timestamp_past,
            timestamp_future=timestamp_future,
            as_nx_graph=True,
        )

        in_degree_dict = dict(lineage_graph.in_degree)
        nodes = np.array(list(in_degree_dict.keys()))
        in_degrees = np.array(list(in_degree_dict.values()))
        return nodes[in_degrees == 0]

    def is_latest_roots(self, root_ids, timestamp=None):
        """Check whether these root_ids are still a root at this timestamp

        Parameters
        ----------
            root_ids ([type]): root ids to check
            timestamp (datetime.dateime, optional): timestamp to check whether these IDs are valid root_ids. Defaults to None (assumes now).

        Returns:
            np.array[bool]: boolean array of whether these are valid root_ids
        """
        root_ids = root_id_int_list_check(root_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["is_latest_roots"].format_map(endpoint_mapping)

        if timestamp is None:
            timestamp = self._default_timestamp
        if timestamp is not None:
            query_d = package_timestamp(self._process_timestamp(timestamp))
        else:
            query_d = None
        data = {"node_ids": root_ids}
        r = handle_response(
            self.session.post(
                url, data=json.dumps(data, cls=BaseEncoder), params=query_d
            )
        )
        return np.array(r["is_latest"], bool)

    def is_valid_nodes(self, node_ids, start_timestamp=None, end_timestamp=None):
        """Check whether nodes are valid for given timestamp range

        Valid is defined as existing in the chunkedgraph. This makes no statement
        about these IDs being roots, supervoxel or anything in-between. It also
        does not take into account whether a root id has since been edited.

        Parameters
        ----------
            node ids ([type]): node ids to check
            start_timestamp (datetime.dateime, optional): timestamp to check whether these IDs were valid after this timestamp. Defaults to None (assumes now).
            end_timestamp (datetime.dateime, optional): timestamp to check whether these IDs were valid before this timestamp. Defaults to None (assumes now).

        Returns:
            np.array[np.Boolean]: boolean array of whether these are valid IDs
        """
        node_ids = root_id_int_list_check(node_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["valid_nodes"].format_map(endpoint_mapping)

        if end_timestamp is None:
            end_timestamp = self._default_timestamp

        if start_timestamp is None:
            start_timestamp = datetime.datetime(2000, 1, 1)

        if start_timestamp is not None:
            query_d = package_timestamp(
                self._process_timestamp(start_timestamp), name="start_timestamp"
            )
        else:
            query_d = {}

        if end_timestamp is not None:
            query_d.update(
                package_timestamp(
                    self._process_timestamp(end_timestamp), name="end_timestamp"
                )
            )

        data = {"node_ids": node_ids}
        r = handle_response(
            self.session.get(
                url, data=json.dumps(data, cls=BaseEncoder), params=query_d
            )
        )
        valid_ids = np.array(r["valid_roots"], np.uint64)

        return np.isin(node_ids, valid_ids)

    def get_root_timestamps(self, root_ids):
        """Retrieves timestamps when roots where created.

        Parameters
        ----------
        root_ids: Iterable,
            Iterable of seed root ids.

        Returns
        -------

        """
        root_ids = root_id_int_list_check(root_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["root_timestamps"].format_map(endpoint_mapping)

        data = {"node_ids": root_ids}
        r = handle_response(
            self.session.post(url, data=json.dumps(data, cls=BaseEncoder))
        )

        return np.array(
            [datetime.datetime.fromtimestamp(ts, pytz.UTC) for ts in r["timestamp"]]
        )

    def get_past_ids(self, root_ids, timestamp_past=None, timestamp_future=None):
        """For a set of root ids, get the list of ids at a past or future time point that could contain parts of the same object.

        Parameters
        ----------
        root_ids: Iterable,
            Iterable of seed root ids.
        timestamp_past : datetime.datetime or None, optional
            Time of a point in the past for which to look up root ids. Default is None.
        timestamp_future : datetime.datetime or None, optional
            Time of a point in the future for which to look up root ids. Not implemented on the server currently. Default is None.

        Returns
        -------
        dict
            Dict with keys `future_id_map` and `past_id_map`. Each is a dict whose keys are the supplied root ids and whose values
            are the list of related root ids at the past/future time stamp.
        """
        root_ids = root_id_int_list_check(root_ids, make_unique=True)

        endpoint_mapping = self.default_url_mapping

        params = {}
        if timestamp_past is not None:
            params.update(package_timestamp(timestamp_past, name="timestamp_past"))
        if timestamp_future is not None:
            params.update(package_timestamp(timestamp_future, name="timestamp_future"))

        data = {"root_ids": np.array(root_ids, dtype=np.uint64)}
        url = self._endpoints["past_id_mapping"].format_map(endpoint_mapping)
        r = handle_response(
            self.session.get(url, data=json.dumps(data, cls=BaseEncoder), params=params)
        )

        # Convert id keys as strings to ints
        past_keys = list(r["past_id_map"].keys())
        for k in past_keys:
            dat = r["past_id_map"].pop(k)
            r["past_id_map"][int(k)] = dat

        fut_keys = list(r["future_id_map"].keys())
        for k in fut_keys:
            dat = r["future_id_map"].pop(k)
            r["future_id_map"][int(k)] = dat
        return r

    def get_delta_roots(
        self,
        timestamp_past: datetime.datetime,
        timestamp_future: datetime.datetime = datetime.datetime.utcnow(),
    ):
        """get the list of roots that have changed between timetamp_past and timestamp_future


        Args:
            timestamp_past (datetime.datetime): past timepoint to query
            timestamp_future (datetime.datetime, optional): future timepoint to query. Defaults to datetime.datetime.utcnow().

        Returns:
            old_roots (np.ndarray): roots that have expired in that interval
            new_roots (np.ndarray): roots that are new in that interval
        """
        endpoint_mapping = self.default_url_mapping
        params = package_timestamp(timestamp_past, name="timestamp_past")
        params.update(package_timestamp(timestamp_future, name="timestamp_future"))

        url = self._endpoints["delta_roots"].format_map(endpoint_mapping)
        r = handle_response(self.session.get(url, params=params))
        return np.array(r["old_roots"]), np.array(r["new_roots"])

    @property
    def cloudvolume_path(self):
        return self._endpoints["cloudvolume_path"].format_map(self.default_url_mapping)

    @property
    def segmentation_info(self):
        """Complete segmentation metadata"""
        if self._segmentation_info is None:
            url = self._endpoints["info"].format_map(self.default_url_mapping)
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
        return self.segmentation_info["scales"][0].get("resolution")


client_mapping = {
    1: ChunkedGraphClientV1,
    "latest": ChunkedGraphClientV1,
}
