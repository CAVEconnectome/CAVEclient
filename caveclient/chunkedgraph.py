"""PyChunkedgraph service python interface"""

import datetime
import json
import logging
from typing import Iterable, Optional, Tuple, Union
from urllib.parse import urlencode

import networkx as nx
import numpy as np
import pandas as pd
import pytz

from .auth import AuthClient
from .base import (
    BaseEncoder,
    ClientBase,
    _api_endpoints,
    _check_version_compatibility,
    handle_response,
)
from .endpoints import (
    chunkedgraph_api_versions,
    chunkedgraph_endpoints_common,
    default_global_server_address,
)

SERVER_KEY = "cg_server_address"

logger = logging.getLogger(__name__)


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


def package_split_data(
    root_id, source_points, sink_points, source_supervoxels, sink_supervoxels
):
    """Create the data for preview or executed split operations"""
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
    return data


def root_id_int_list_check(
    root_id,
    make_unique=False,
):
    if isinstance(root_id, (int, np.uint64, np.int64)):
        root_id = [root_id]
    elif isinstance(root_id, str):
        try:
            root_id = np.uint64(root_id)
        except ValueError:
            raise ValueError(
                "When passing a string for 'root_id' make sure the string can be converted to a uint64"
            )
    elif isinstance(root_id, (list, np.ndarray, pd.Series, pd.Index)):
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
) -> "ChunkedGraphClientV1":
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
        verify=verify,
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

    @property
    def timestamp(self) -> Optional[datetime.datetime]:
        """The default timestamp for queries which expect a timestamp. If None, uses the
        current time."""
        if self.fc is None or self.fc.timestamp is None:
            return self._default_timestamp
        else:
            return self.fc.timestamp

    @timestamp.setter
    def timestamp(self, value: Optional[datetime.datetime]):
        if self.fc is not None and self.fc.timestamp is not None:
            msg = (
                "Cannot set `timestamp` when attached to a CAVEclient with a version, "
                "set a version at the CAVEclient level instead."
            )
            raise ValueError(msg)
        if value is None or isinstance(value, datetime.datetime):
            self._default_timestamp = value
        else:
            raise TypeError("`timestamp` must be a datetime object or None.")

    def _process_timestamp(
        self, timestamp: Optional[datetime.datetime]
    ) -> datetime.datetime:
        """Process timestamp with default logic"""
        if timestamp is None:
            if self.timestamp is not None:
                # refers to the framework client if it exists, otherwise uses the
                # value set for this chunkedgraph client
                return self.timestamp
            else:
                return datetime.datetime.now(datetime.timezone.utc)
        else:
            return timestamp

    def get_roots(self, supervoxel_ids, timestamp=None, stop_layer=None) -> np.ndarray:
        """Get the root ID for a list of supervoxels.

        Parameters
        ----------
        supervoxel_ids : list or np.array of int
            Supervoxel IDs to look up.
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by
            default None. If None, uses the `timestamp` property for this client, which
            defaults to the current time.
        stop_layer : int or None, optional
            If True, looks up IDs only up to a given stop layer. Default is None.

        Returns
        -------
        np.array of np.uint64
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

    def get_root_id(self, supervoxel_id, timestamp=None, level2=False) -> np.int64:
        """Get the root ID for a specified supervoxel.

        Parameters
        ----------
        supervoxel_id : int
            Supervoxel id value
        timestamp : datetime.datetime, optional
            UTC datetime to specify the state of the chunkedgraph at which to query, by
            default None. If None, uses the `timestamp` property for this client, which
            defaults to the current time.

        Returns
        -------
        np.int64
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

    def get_merge_log(self, root_id) -> list:
        """Get the merge log (splits and merges) for an object.

        Parameters
        ----------
        root_id : int
            Object root ID to look up.

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

    def get_change_log(self, root_id, filtered=True) -> dict:
        """Get the change log (splits and merges) for an object.

        Parameters
        ----------
        root_id : int
            Object root ID to look up.
        filtered : bool
            Whether to filter the change log to only include splits and merges which
            affect the final state of the object (`filtered=True`), as opposed to
            including edit history for objects which as some point were split from
            the query object `root_id` (`filtered=False`). Defaults to True.

        Returns
        -------
        dict
            Dictionary summarizing split and merge events in the object history,
            containing the following keys:

            - "n_merges": int
                - Number of merges
            - "n_splits": int
                - Number of splits
            - "operations_ids": list of int
                - Identifiers for each operation
            - "past_ids": list of int
                - Previous root ids for this object
            - "user_info": dict of dict
                - Dictionary keyed by user (string) to a dictionary specifying how many
                  merges and splits that user performed on this object
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
    ) -> pd.DataFrame:
        """
        Get operation details for a user ID. Currently, this is only available to
        admins.


        Parameters
        ----------
        user_id : int
            User ID to query (use 0 for all users (admin only)).
        timestamp_start : datetime.datetime, optional
            Timestamp to start filter (UTC).
        include_undo : bool, optional
            Whether to include undos. Defaults to True.
        timestamp_end : datetime.datetime, optional
            Timestamp to end filter (UTC). Defaults to now.

        Returns
        -------
        pd.DataFrame
            DataFrame including the following columns:

            - "operation_id": int
                - Identifier for the operation.
            - "timestamp": datetime.datetime
                - Timestamp of the operation.
            - "user_id": int
                - User who performed the operation.
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

    def get_tabular_change_log(self, root_ids, filtered=True) -> dict:
        """Get a detailed changelog for neurons.

        Parameters
        ----------
        root_ids : list of int
            Object root IDs to look up.
        filtered : bool
            Whether to filter the change log to only include splits and merges which
            affect the final state of the object (`filtered=True`), as opposed to
            including edit history for objects which as some point were split from
            the query objects in `root_ids` (`filtered=False`). Defaults to True.

        Returns
        -------
        dict of pd.DataFrame
            The keys are the root IDs, and the values are DataFrames with the
            following columns and datatypes:

            - "operation_id": int
                - Identifier for the operation.
            - "timestamp": int
                - Timestamp of the operation, provided in *milliseconds*. To convert to
                datetime, use ``datetime.datetime.utcfromtimestamp(timestamp/1000)``.
            - "user_id": int
                - User who performed the operation.
            - "before_root_ids: list of int
                - Root IDs of objects that existed before the operation.
            - "after_root_ids: list of int
                - Root IDs of objects created by the operation. Note that this only
                records the root id that was kept as part of the query object, so there
                will only be one in this list.
            - "is_merge": bool
                - Whether the operation was a merge.
            - "user_name": str
                - Name of the user who performed the operation.
            - "user_affiliation": str
                - Affiliation of the user who performed the operation.
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

    def get_leaves(self, root_id, bounds=None, stop_layer: int = None) -> np.ndarray:
        """Get all supervoxels for a root ID.

        Parameters
        ----------
        root_id : int
            Root ID to query.
        bounds: np.array or None, optional
            If specified, returns supervoxels within a 3x2 numpy array of bounds
            ``[[minx,maxx],[miny,maxy],[minz,maxz]]``. If None, finds all supervoxels.
        stop_layer: int, optional
            If specified, returns chunkedgraph nodes at layer `stop_layer`
            default will be `stop_layer=1` (supervoxels).

        Returns
        -------
        np.array of np.int64
            Array of supervoxel IDs (or node ids if `stop_layer>1`).
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

    def do_merge(self, supervoxels, coords, resolution=(4, 4, 40)) -> None:
        """Perform a merge on the chunked graph.

        Parameters
        ----------
        supervoxels : iterable
            An N-long list of supervoxels to merge.
        coords : np.array
            An Nx3 array of coordinates of the supervoxels in units of `resolution`.
        resolution : tuple, optional
            What to multiply `coords` by to get nanometers. Defaults to (4,4,40).
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

    def undo_operation(self, operation_id) -> dict:
        """Undo an operation.

        Parameters
        ----------
        operation_id : int
            Operation ID to undo.

        Returns
        -------
        dict
        """
        # TODO clarify what the return is here
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["undo"].format_map(endpoint_mapping)

        data = {"operation_id": operation_id}
        params = {"priority": False}
        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            params=params,
            headers={"Content-Type": "application/json"},
        )
        r = handle_response(response)
        return r

    def execute_split(
        self,
        source_points,
        sink_points,
        root_id,
        source_supervoxels=None,
        sink_supervoxels=None,
    ) -> Tuple[int, list]:
        """Execute a multicut split based on points or supervoxels.

        Parameters
        ----------
        source_points : array or list
            Nx3 list or array of 3d points in nm coordinates for source points (red).
        sink_points : array or list
            Mx3 list or array of 3d points in nm coordinates for sink points (blue).
        root_id : int
            Root ID of object to do split preview.
        source_supervoxels : array, list or None, optional
            If providing source supervoxels, an N-length array of supervoxel IDs or
            Nones matched to source points. If None, treats as a full array of Nones.
            By default None.
        sink_supervoxels : array, list or None, optional
            If providing sink supervoxels, an M-length array of supervoxel IDs or Nones
            matched to source points. If None, treats as a full array of Nones.
            By default None.

        Returns
        -------
        operation_id : int
            Unique ID of the split operation
        new_root_ids : list of int
            List of new root IDs resulting from the split operation.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["execute_split"].format_map(endpoint_mapping)

        data = package_split_data(
            root_id, source_points, sink_points, source_supervoxels, sink_supervoxels
        )
        params = {"priority": False}
        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            params=params,
            headers={"Content-Type": "application/json"},
        )
        r = handle_response(response)
        return r["operation_id"], r["new_root_ids"]

    def preview_split(
        self,
        source_points,
        sink_points,
        root_id,
        source_supervoxels=None,
        sink_supervoxels=None,
        return_additional_ccs=False,
    ) -> Tuple[list, list, bool, list]:
        """Get supervoxel connected components from a preview multicut split.

        Parameters
        ----------
        source_points : array or list
            Nx3 list or array of 3d points in nm coordinates for source points (red).
        sink_points : array or list
            Mx3 list or array of 3d points in nm coordinates for sink points (blue).
        root_id : int
            Root ID of object to do split preview.
        source_supervoxels : array, list or None, optional
            If providing source supervoxels, an N-length array of supervoxel IDs or
            Nones matched to source points. If None, treats as a full array of Nones.
            By default None.
        sink_supervoxels : array, list or None, optional
            If providing sink supervoxels, an M-length array of supervoxel IDs or Nones
            matched to source points. If None, treats as a full array of Nones.
            By default None.
        return_additional_ccs : bool, optional
            If True, returns any additional connected components beyond the ones with
            source and sink points. In most situations, this can be ignored.
            By default, False.

        Returns
        -------
        source_connected_component : list
            Supervoxel IDs in the component with the most source points.
        sink_connected_component : list
            Supervoxel IDs in the component with the most sink points.
        successful_split : bool
            True if the split worked.
        other_connected_components (optional) : list of lists of int
            List of lists of supervoxel IDs for any other resulting connected components.
            Only returned if `return_additional_ccs` is True.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["preview_split"].format_map(endpoint_mapping)

        data = package_split_data(
            root_id, source_points, sink_points, source_supervoxels, sink_supervoxels
        )

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

    def get_children(self, node_id) -> np.ndarray:
        """Get the children of a node in the chunked graph hierarchy.

        Parameters
        ----------
        node_id : int
            Node ID to query.

        Returns
        -------
        np.array of np.int64
            IDs of child nodes.
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["root_id"] = node_id
        url = self._endpoints["handle_children"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return np.array(handle_response(response)["children_ids"], dtype=np.int64)

    def get_contact_sites(self, root_id, bounds, calc_partners=False) -> dict:
        """Get contacts for a root ID.

        Parameters
        ----------
        root_id : int
            Root ID to query.
        bounds: np.array
            Bounds within a 3x2 numpy array of bounds
            ``[[minx,maxx],[miny,maxy],[minz,maxz]]`` for which to find contacts.
            Running this query without bounds is too slow.
        calc_partners : bool, optional
            If True, get partner root IDs. By default, False.
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

    def find_path(
        self, root_id, src_pt, dst_pt, precision_mode=False
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Find a path between two locations on a root ID using the level 2 chunked
        graph.


        Parameters
        ----------
        root_id : int
            Root ID to query.
        src_pt : np.array
            3-element array of xyz coordinates in nm for the source point.
        dst_pt : np.array
            3-element array of xyz coordinates in nm for the destination point.
        precision_mode : bool, optional
            Whether to perform the search in precision mode. Defaults to False.

        Returns
        -------
        centroids_list : np.array
            Array of centroids along the path.
        l2_path : np.array of int
            Array of level 2 chunk IDs along the path.
        failed_l2_ids : np.array of int
            Array of level 2 chunk IDs that failed to find a path.
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

    def get_subgraph(
        self, root_id, bounds
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Get subgraph of root id within a bounding box.

        Parameters
        ----------
        root_id : int
            Root (or any node ID) of chunked graph to query.
        bounds : np.array
            3x2 bounding box (x,y,z) x (min,max) in chunked graph coordinates.

        Returns
        -------
        np.array of np.int64
            Node IDs in the subgraph.
        np.array of np.double
            Affinities of edges in the subgraph.
        np.array of np.int32
            Areas of nodes in the subgraph.
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

    @_check_version_compatibility(kwarg_use_constraints={"bounds": ">=2.15.0"})
    def level2_chunk_graph(self, root_id, bounds=None) -> list:
        """
        Get graph of level 2 chunks, the smallest agglomeration level above supervoxels.


        Parameters
        ----------
        root_id : int
            Root id of object
        bounds : np.array
            3x2 bounding box (x,y,z) x (min,max) in chunked graph coordinates (use
            `client.chunkedgraph.base_resolution` to view this default resolution for
            your chunkedgraph client). Note that the result will include any level 2
            nodes which have chunk boundaries within some part of this bounding box,
            meaning that the representative point for a given level 2 node could still
            be slightly outside of these bounds. If None, returns all level 2 chunks
            for the root ID.

        Returns
        -------
        list of list
            Edge list for level 2 chunked graph. Each element of the list is an edge,
            and each edge is a list of two node IDs (source and target).
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["root_id"] = root_id

        query_d = {}
        if bounds is not None:
            query_d["bounds"] = package_bounds(bounds)

        url = self._endpoints["lvl2_graph"].format_map(endpoint_mapping)
        response = self.session.get(url, params=query_d)

        r = handle_response(response)

        # TODO in theory, could remove this check if we are confident in the server
        # version fix
        used_bounds = response.headers.get("Used-Bounds")
        used_bounds = used_bounds == "true" or used_bounds == "True"
        if bounds is not None and not used_bounds:
            warning = (
                "Bounds were not used for this query, even though it was requested. "
                "This is likely because your system is running a version of the "
                "chunkedgraph that does not support this feature. Please contact "
                "your system administrator to update the chunkedgraph."
            )
            raise ValueError(warning)

        return r["edge_graph"]

    def remesh_level2_chunks(self, chunk_ids) -> None:
        """Submit specific level 2 chunks to be remeshed in case of a problem.

        Parameters
        ----------
        chunk_ids : list
            List of level 2 chunk IDs.
        """

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["remesh_level2_chunks"].format_map(endpoint_mapping)
        data = {"new_lvl2_ids": [int(x) for x in chunk_ids]}
        r = self.session.post(url, json=data)
        r.raise_for_status()

    def get_operation_details(self, operation_ids: Iterable[int]) -> dict:
        """Get the details of a list of operations.

        Parameters
        ----------
        operation_ids: Iterable of int
            List/array of operation IDs.

        Returns
        -------
        dict of str to dict
            A dict of dicts of operation info, keys are operation IDs (as strings),
            values are a dictionary of operation info for the operation. These
            dictionaries contain the following keys:

            - "added_edges"/"removed_edges": list of list of int
                - List of edges added (if a merge) or removed (if a split) by this
                operation. Each edge is a list of two supervoxel IDs (source and
                target).
            - "roots": list of int
                - List of root IDs that were created by this operation.
            - "sink_coords": list of list of int
                - List of sink coordinates for this operation. The sink is one of the
                points placed by the user when specifying the operation. Each sink
                coordinate is a list of three integers (x, y, z), corresponding to
                spatial coordinates in segmentation voxel space.
            - "source_coords": list of list of int
                - List of source coordinates for this operation. The source is one of the
                points placed by the user when specifying the operation. Each source
                coordinate is a list of three integers (x, y, z), corresponding to
                spatial coordinates in segmentation voxel space.
            - "timestamp": str
                - Timestamp of the operation.
            - "user": str
                - User ID number who performed the operation (as a string).
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
        self,
        root_id,
        timestamp_past=None,
        timestamp_future=None,
        as_nx_graph=False,
        exclude_links_to_future=False,
        exclude_links_to_past=False,
    ) -> Union[dict, nx.DiGraph]:
        """
        Returns the lineage graph for a root ID, optionally cut off in the past or
        the future.

        Each change in the chunked graph creates a new root ID for the object after
        that change. This function returns a graph of all root IDs for a given object,
        tracing the history of the object in terms of merges and splits.


        Parameters
        ----------
        root_id : int
            Object root ID.
        timestamp_past : datetime.datetime or None, optional
            Cutoff for the lineage graph backwards in time. By default, None.
        timestamp_future : datetime.datetime or None, optional
            Cutoff for the lineage graph going forwards in time. By default, uses the
            `timestamp` property for this client, which defaults to the current time.
        as_nx_graph: bool
            If True, a NetworkX graph is returned.
        exclude_links_to_future: bool
            If True, links from nodes before `timestamp_future` to after
            `timestamp_future` are removed. If False, the link(s) which has one node
            before timestamp and one node after timestamp is kept.
        exclude_links_to_past: bool
            If True, links from nodes before `timestamp_past` to after `timestamp_past`
            are removed. If False, the link(s) which has one node before timestamp and
            one node after timestamp is kept.

        Returns
        -------
        dict
            Dictionary describing the lineage graph and operations for the root ID. Not
            returned if `as_nx_graph` is True. The dictionary contains the following
            keys:

            - "directed" : bool
                - Whether the graph is directed.
            - "graph" : dict
                - Dictionary of graph attributes.
            - "links" : list of dict
                - Each element of the list is a dictionary describing an edge in the
                lineage graph as "source" and "target" keys.
            - "multigraph" : bool
                - Whether the graph is a multigraph.
            - "nodes" : list of dict
                - Each element of the list is a dictionary describing a node in the
                lineage graph, usually with "id", "timestamp", and "operation_id"
                keys.
        nx.DiGraph
            NetworkX directed graph of the lineage graph. Only returned if `as_nx_graph`
            is True.
        """
        root_id = root_id_int_list_check(root_id, make_unique=True)

        endpoint_mapping = self.default_url_mapping

        params = {}
        if timestamp_past is not None:
            params.update(package_timestamp(timestamp_past, name="timestamp_past"))
        if timestamp_future is not None:
            params.update(package_timestamp(timestamp_future, name="timestamp_future"))
        else:
            params.update(package_timestamp(self.timestamp), name="timestamp_future")

        url = self._endpoints["handle_lineage_graph"].format_map(endpoint_mapping)
        data = json.dumps({"root_ids": root_id}, cls=BaseEncoder)
        r = handle_response(self.session.post(url, data=data, params=params))

        if exclude_links_to_future or exclude_links_to_past:
            bad_ids = []
            for node in r["nodes"]:
                node_ts = datetime.datetime.fromtimestamp(node["timestamp"])
                node_ts = node_ts.astimezone(datetime.timezone.utc)
                if (
                    exclude_links_to_past and (node_ts < timestamp_past)
                    if timestamp_past is not None
                    else False
                ):
                    bad_ids.append(node["id"])
                if (
                    exclude_links_to_future and (node_ts > timestamp_future)
                    if timestamp_future is not None
                    else False
                ):
                    bad_ids.append(node["id"])

            r["nodes"] = [node for node in r["nodes"] if node["id"] not in bad_ids]
            r["links"] = [
                link
                for link in r["links"]
                if link["source"] not in bad_ids and link["target"] not in bad_ids
            ]

        if as_nx_graph:
            return nx.node_link_graph(r)
        else:
            return r

    def get_latest_roots(
        self, root_id, timestamp=None, timestamp_future=None
    ) -> np.ndarray:
        """
        Returns root IDs that are related to the given `root_id` at a given
        timestamp. Can be used to find the "latest" root IDs associated with an object.


        Parameters
        ----------
        root_id : int
            Object root ID.
        timestamp : datetime.datetime or None, optional
            Timestamp of where to query IDs from. If None, uses the `timestamp` property
            for this client, which defaults to the current time.
        timestamp_future : datetime.datetime or None, optional
            DEPRECATED name, use `timestamp` instead.
            Timestamp to suggest IDs from (note can be in the past relative to the
            root). By default, None.

        Returns
        -------
        np.ndarray
            1d array with all latest successors.
        """
        root_id = root_id_int_list_check(root_id, make_unique=True)

        timestamp_root = self.get_root_timestamps(root_id).min()
        if timestamp_future is not None:
            logger.warning("timestamp_future is deprecated, use timestamp instead")
            timestamp = timestamp_future

        timestamp = self._process_timestamp(timestamp)

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

        # or if timestamp_root is less than timestamp_future
        if (timestamp is None) or (timestamp_root < timestamp):
            lineage_graph = self.get_lineage_graph(
                root_id,
                timestamp_past=timestamp_root,
                timestamp_future=timestamp,
                exclude_links_to_future=True,
                as_nx_graph=True,
            )
            # then we want the leaves of the tree
            out_degree_dict = dict(lineage_graph.out_degree)
            nodes = np.array(list(out_degree_dict.keys()))
            out_degrees = np.array(list(out_degree_dict.values()))
            return nodes[out_degrees == 0]
        else:
            # then timestamp is in fact in the past
            lineage_graph = self.get_lineage_graph(
                root_id,
                timestamp_future=timestamp_root,
                timestamp_past=timestamp,
                as_nx_graph=True,
            )
            in_degree_dict = dict(lineage_graph.in_degree)
            nodes = np.array(list(in_degree_dict.keys()))
            in_degrees = np.array(list(in_degree_dict.values()))
            return nodes[in_degrees == 0]

    def get_original_roots(self, root_id, timestamp_past=None) -> np.ndarray:
        """Returns root IDs that are the latest successors of a given root ID.

        Parameters
        ----------
        root_id : int
            Object root ID.
        timestamp_past : datetime.datetime or None, optional
            Cutoff for the search going backwards in time. By default, None.

        Returns
        -------
        np.ndarray
            1d array with all latest successors.
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

    def is_latest_roots(self, root_ids, timestamp=None) -> np.ndarray:
        """Check whether these root IDs are still a root at this timestamp.

        Parameters
        ----------
        root_ids : array-like of int
            Root IDs to check.
        timestamp : datetime.datetime, optional
            Timestamp to check whether these IDs are valid root IDs in the chunked
            graph. If None, uses the `timestamp` property for this client, which
            defaults to the current time.

        Returns
        -------
        np.array of bool
            Array of whether these are valid root IDs.
        """
        root_ids = root_id_int_list_check(root_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["is_latest_roots"].format_map(endpoint_mapping)

        if timestamp is None:
            timestamp = self.timestamp
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

    def suggest_latest_roots(
        self,
        root_id,
        timestamp=None,
        stop_layer=None,
        return_all=False,
        return_fraction_overlap=False,
    ):
        """
        Suggest latest roots for a given root id, based on overlap of component
        chunk IDs. Note that edits change chunk IDs, and so this effectively measures
        the fraction of unchanged chunks at a given chunk layer, which sets the size
        scale of chunks. Higher layers are coarser.


        Parameters
        ----------
        root_id : int
            Root ID of the potentially outdated object.
        timestamp : datetime, optional
            Datetime at which "latest" roots are being computed, by default None. If
            None, uses the `timestamp` property for this client, which defaults to the
            current time. Note that this has to be a timestamp after the creation of the
            `root_id`.
        stop_layer : int, optional
            Chunk level at which to compute overlap, by default None.
            No value will take the 4th from the top layer, which emphasizes speed and
            works well for larger objects. Lower values are slower but more
            fine-grained. Values under 2 (i.e. supervoxels) are not recommended except
            in extremely fine grained scenarios.
        return_all : bool, optional
            If True, return all current IDs sorted from most overlap to least, by
            default False. If False, only the top is returned.
        return_fraction_overlap : bool, optional
            If True, return all fractions sorted by most overlap to least, by default
            False. If False, only the top value is returned.
        """
        curr_ids = self.get_latest_roots(root_id, timestamp=timestamp)

        if root_id in curr_ids:
            if return_all:
                if return_fraction_overlap:
                    return [root_id], [1]
                else:
                    return [root_id]
            else:
                if return_fraction_overlap:
                    return root_id, 1
                else:
                    return root_id

        delta_layers = 4
        if stop_layer is None:
            stop_layer = (
                self.segmentation_info.get("graph", {}).get("n_layers", 6)
                - delta_layers
            )
        stop_layer = max(1, stop_layer)

        chunks_orig = self.get_leaves(root_id, stop_layer=stop_layer)
        while len(chunks_orig) == 0:
            stop_layer -= 1
            if stop_layer == 1:
                raise ValueError(
                    f"There were no children for root_id={root_id} at level 2, something is wrong with the chunkedgraph"
                )
            chunks_orig = self.get_leaves(root_id, stop_layer=stop_layer)

        chunk_list = np.array(
            [
                len(
                    np.intersect1d(
                        chunks_orig,
                        self.get_leaves(oid, stop_layer=stop_layer),
                        assume_unique=True,
                    )
                )
                / len(chunks_orig)
                for oid in curr_ids
            ]
        )
        order = np.argsort(chunk_list)[::-1]
        if not return_all:
            order = order[0]
        if return_fraction_overlap:
            return curr_ids[order], chunk_list[order]
        else:
            return curr_ids[order]

    def is_valid_nodes(
        self, node_ids, start_timestamp=None, end_timestamp=None
    ) -> np.ndarray:
        """Check whether nodes are valid for given timestamp range.

        Valid is defined as existing in the chunked graph. This makes no statement
        about these IDs being roots, supervoxel or anything in-between. It also
        does not take into account whether a root ID has since been edited.


        Parameters
        ----------
        node_ids : array-like of int
            Node IDs to check.
        start_timestamp : datetime.datetime, optional
            Timestamp to check whether these IDs were valid after this timestamp.
            Defaults to None (assumes now).
        end_timestamp : datetime.datetime, optional
            Timestamp to check whether these IDs were valid before this timestamp.
            If None, uses the `timestamp` property for this client, which defaults to
            the current time.

        Returns
        -------
        np.array of bool
            Array of whether these are valid IDs.
        """
        node_ids = root_id_int_list_check(node_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["valid_nodes"].format_map(endpoint_mapping)

        if end_timestamp is None:
            end_timestamp = self.timestamp

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

    @_check_version_compatibility(
        kwarg_use_constraints={
            "latest": ["<2,>=1.25.0", "<3,>=2.17.0"],
            "timestamp": ["<2,>=1.25.0", "<3,>=2.17.0"],
        }
    )
    def get_root_timestamps(
        self, root_ids, latest: bool = False, timestamp: datetime.datetime = None
    ) -> np.ndarray:
        """Retrieves timestamps when roots where created.

        Parameters
        ----------
        root_ids: Iterable of int
            Iterable of root IDs to query.
        latest: bool, optional
            If False,  returns the first timestamp that the root_id was valid for each root ID.
            If True, returns the newest/latest timestamp for each root ID.
            Note, this will return the timestamp at which the query was run when the root is currently valid.
            This means that you will get a different answer if you make this same query at a later time
            if you don't specify a timestamp parameter.
        timestamp: datetime.datetime, optional
            Timestamp to query when using latest=True. Use this to provide consistent
            results for a particular timestamp. If an ID is still valid at a point in
            the future past this timestamp, the query will still return this timestamp
            as the latest moment in time. An error will occur if you provide a timestamp
            for which the root ID is not valid. If None, uses the `timestamp` property
            for this client, which defaults to the current time.

        Returns
        -------
        np.array of datetime.datetime
            Array of timestamps when `root_ids` were created.
        """
        root_ids = root_id_int_list_check(root_ids, make_unique=False)

        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["root_timestamps"].format_map(endpoint_mapping)

        data = {"node_ids": root_ids}
        params = {"latest": latest}

        timestamp = self._process_timestamp(timestamp)
        if timestamp is not None:
            params.update(package_timestamp(timestamp, name="timestamp"))

        r = handle_response(
            self.session.post(
                url, data=json.dumps(data, cls=BaseEncoder), params=params
            )
        )
        if latest:
            delta_t = datetime.timedelta(milliseconds=1)
        else:
            delta_t = datetime.timedelta(milliseconds=0)

        return np.array(
            [
                datetime.datetime.fromtimestamp(ts, pytz.UTC) - delta_t
                for ts in r["timestamp"]
            ]
        )

    def get_past_ids(
        self, root_ids, timestamp_past=None, timestamp_future=None
    ) -> dict:
        """
        For a set of root IDs, get the list of IDs at a past or future time point
        that could contain parts of the same object.


        Parameters
        ----------
        root_ids : Iterable of int
            Iterable of root IDs to query.
        timestamp_past : datetime.datetime or None, optional
            Time of a point in the past for which to look up root ids. Default is None.
        timestamp_future : datetime.datetime or None, optional
            Time of a point in the future for which to look up root ids. Not
            implemented on the server currently. Default is None.

        Returns
        -------
        dict
            Dict with keys "future_id_map" and "past_id_map". Each is a dict whose keys
            are the supplied `root_ids` and whose values are the list of related
            root IDs at `timestamp_past`/`timestamp_future`.
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
        timestamp_future: datetime.datetime = datetime.datetime.now(
            datetime.timezone.utc
        ),
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get the list of roots that have changed between `timetamp_past` and
        `timestamp_future`.


        Parameters
        ----------
        timestamp_past : datetime.datetime
            Past timepoint to query
        timestamp_future : datetime.datetime, optional
            Future timepoint to query. Defaults to
            ``datetime.datetime.now(datetime.timezone.utc)``.

        Returns
        -------
        old_roots : np.ndarray of np.int64
            Roots that have expired in that interval.
        new_roots : np.ndarray of np.int64
            Roots that are new in that interval.
        """

        endpoint_mapping = self.default_url_mapping
        params = package_timestamp(timestamp_past, name="timestamp_past")
        params.update(package_timestamp(timestamp_future, name="timestamp_future"))

        url = self._endpoints["delta_roots"].format_map(endpoint_mapping)
        r = handle_response(self.session.get(url, params=params))
        return np.array(r["old_roots"]), np.array(r["new_roots"])

    def get_oldest_timestamp(self) -> datetime.datetime:
        """Get the oldest timestamp in the database.

        Returns
        -------
        datetime.datetime
            Oldest timestamp in the database.
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["oldest_timestamp"].format_map(endpoint_mapping)
        response = handle_response(self.session.get(url))
        return datetime.datetime.fromisoformat(response["iso"])

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
