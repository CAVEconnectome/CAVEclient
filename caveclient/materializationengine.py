import itertools
import json
import logging
import re
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Iterable, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytz
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from IPython.display import HTML
from requests import HTTPError

from .auth import AuthClient
from .base import (
    BaseEncoder,
    ClientBase,
    _api_endpoints,
    handle_response,
)
from .endpoints import materialization_api_versions, materialization_common
from .mytimer import MyTimeIt
from .tools.table_manager import TableManager, ViewManager

logger = logging.getLogger(__name__)

SERVER_KEY = "me_server_address"

DEFAULT_COMPRESSION = "zstd"


def deserialize_query_response(response):
    """Deserialize pyarrow responses"""
    content_type = response.headers.get("Content-Type")
    if content_type == "data.arrow":
        with pa.ipc.open_stream(response.content) as reader:
            df = reader.read_pandas()
        return df
    elif content_type == "x-application/pyarrow":
        try:
            return pa.deserialize(response.content)
        except NameError:
            (
                "Deserialization of this request requires an older version of Pyarrow (version 3 works).\
                Update Materialization Deployment or locally downgrade Pyarrow."
            )
    else:
        raise ValueError(
            f'Unknown response type: {response.headers.get("Content-Type")}'
        )


def convert_position_columns(df, given_resolution, desired_resolution):
    """function to take a dataframe with x,y,z position columns and convert
    them to the desired resolution from the given resolution

    Args:
        df (pd.DataFrame): dataframe to alter
        given_resolution (Iterable[float]): what the given resolution is
        desired_resoultion (Iterable[float]): what the desired resolution is

    Returns:
        pd.DataFrame: [description]
    """
    gr = np.array(given_resolution)
    dr = np.array(desired_resolution)
    sf = gr / dr
    posmap = {"x": 0, "y": 1, "z": 2}
    if np.all(sf == 1):
        return df
    else:
        grps = itertools.groupby(df.columns, key=lambda x: x[:-2])
        for _, g in grps:
            gl = list(g)
            t = "".join([k[-1:] for k in gl])
            if t == "xyz":
                for col in gl:
                    df[col] = df[col] * sf[posmap[col[-1]]]

    return df


def concatenate_position_columns(df, inplace=False):
    """function to take a dataframe with x,y,z position columns and replace them
    with one column per position with an xyz numpy array.  Edits occur

    Args:
        df (pd.DataFrame): dataframe to alter
        inplace (bool): whether to perform edits in place

    Returns:
        pd.DataFrame: [description]
    """
    if inplace:
        df2 = df
    else:
        df2 = df.copy()
    grps = itertools.groupby(df2.columns, key=lambda x: x[:-2])
    for base, g in grps:
        gl = list(g)
        t = "".join([k[-1:] for k in gl])
        if t == "xyz":
            df2[base] = [np.array(x) for x in df2[gl].values.tolist()]
            if inplace:
                df2.drop(gl, axis=1, inplace=inplace)
            else:
                df2 = df2.drop(gl, axis=1, inplace=inplace)
    return df2


def convert_timestamp(ts: datetime):
    if ts == "now":
        ts = datetime.now(timezone.utc)

    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return pytz.UTC.localize(dt=ts)
        else:
            return ts.astimezone(timezone.utc)
    elif isinstance(ts, float):
        return datetime.fromtimestamp(ts)
    elif ts is None:
        return pd.Timestamp.max.to_pydatetime()
    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
    return dt.replace(tzinfo=timezone.utc)


def string_format_timestamp(ts):
    if isinstance(ts, datetime):
        return datetime.strftime(ts, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return ts


def MaterializationClient(
    server_address,
    datastack_name=None,
    auth_client=None,
    cg_client=None,
    synapse_table=None,
    api_version="latest",
    version=None,
    verify=True,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    desired_resolution=None,
    over_client=None,
) -> "MaterializationClientType":
    """Factory for returning AnnotationClient

    Parameters
    ----------
    server_address : str
        server_address to use to connect to (i.e. https://minniev1.microns-daf.com)
    datastack_name : str
        Name of the datastack.
    auth_client : AuthClient or None, optional
        Authentication client to use to connect to server. If None, do not use authentication.
    api_version : str or int (default: latest)
        What version of the api to use, 0: Legacy client (i.e www.dynamicannotationframework.com)
        2: new api version, (i.e. minniev1.microns-daf.com)
        'latest': default to the most recent (current 2)
    cg_client: caveclient.chunkedgraph.ChunkedGraphClient
        chunkedgraph client for live materializations
    synapse_table: str
        default synapse table for queries
    version : default version to query
        if None will default to latest version
    desired_resolution : Iterable[float] or None, optional
        If given, should be a list or array of the desired resolution you want queries returned in
        useful for materialization queries.

    Returns
    -------
    ClientBaseWithDatastack
        List of datastack names for available datastacks on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(
        api_version,
        SERVER_KEY,
        server_address,
        materialization_common,
        materialization_api_versions,
        auth_header,
        fallback_version=2,
        verify=verify,
    )

    MatClient = client_mapping[api_version]
    return MatClient(
        server_address,
        auth_header,
        api_version,
        endpoints,
        SERVER_KEY,
        datastack_name,
        cg_client=cg_client,
        synapse_table=synapse_table,
        version=version,
        verify=verify,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
        desired_resolution=desired_resolution,
    )


class MaterializationClientV2(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        datastack_name,
        cg_client=None,
        synapse_table=None,
        version=None,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
        desired_resolution=None,
    ):
        super(MaterializationClientV2, self).__init__(
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
        self._version = version
        self._cg_client = cg_client
        self.synapse_table = synapse_table
        self.desired_resolution = desired_resolution
        self._tables = None
        self._views = None

    @property
    def datastack_name(self):
        """The name of the datastack."""
        return self._datastack_name

    @property
    def cg_client(self):
        """The chunked graph client."""
        if self._cg_client is None:
            if self.fc is not None:
                self._cg_client = self.fc.chunkedgraph
            else:
                raise ValueError("No chunkedgraph client specified")
        return self._cg_client

    @property
    def version(self) -> int:
        """The version of the materialization. Can be used to set up the
        client to default to a specific version when timestamps or versions are not
        specified in queries. If not set, defaults to the most recent version.

        Note that if this materialization client is attached to a CAVEclient,
        the version must be set at the CAVEclient level.
        """
        if self.fc is not None and self.fc.version is not None:
            return self.fc.version
        if self._version is None:
            self._version = self.most_recent_version()
        return self._version

    @version.setter
    def version(self, x: Optional[int]):
        if self.fc is not None and self.fc.version is not None:
            msg = (
                "Cannot set `version` for materialization client when attached to a "
                "CAVEclient with a version, set at the CAVEclient level instead."
            )
            raise ValueError(msg)
        if int(x) in self.get_versions(expired=True):
            self._version = int(x)
        else:
            raise ValueError("Version not in materialized database")

    @property
    def homepage(self) -> HTML:
        """The homepage for the materialization engine."""
        url = (
            f"{self._server_address}/materialize/views/datastack/{self._datastack_name}"
        )
        return HTML(f'<a href="{url}" target="_blank">Materialization Engine</a>')

    @property
    def tables(self) -> TableManager:
        """The table manager for the materialization engine."""
        if self._tables is None:
            if self.fc is not None and self.fc._materialize is not None:
                self._tables = TableManager(self.fc)
            else:
                raise ValueError("No full CAVEclient specified")
        return self._tables

    @property
    def views(self) -> ViewManager:
        """The view manager for the materialization engine."""
        if self._views is None:
            if self.fc is not None and self.fc._materialize is not None:
                self._views = ViewManager(self.fc)
            else:
                raise ValueError("No full CAVEclient specified")
        return self._views

    def most_recent_version(self, datastack_name=None) -> int:
        """
        Get the most recent version of materialization for this datastack name

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack, by default None.
            If None, uses the one specified in the client.
            Will be set correctly if you are using the framework_client

        Returns
        -------
        np.int
            Most recent version of materialization for this datastack name
        """

        versions = self.get_versions(datastack_name=datastack_name)
        return np.max(np.array(versions))

    def get_versions(self, datastack_name=None, expired=False):
        """Get the versions available

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack, by default None.
            If None, uses the one specified in the client.
        expired : bool, optional
            Whether to include expired versions, by default False.

        Returns
        -------
        dict
            Dictionary of versions available
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        url = self._endpoints["versions"].format_map(endpoint_mapping)
        query_args = {"expired": expired}
        response = self.session.get(url, params=query_args)
        self.raise_for_status(response)
        return response.json()

    def get_tables(self, datastack_name=None, version: Optional[int] = None):
        """Gets a list of table names for a datastack

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack, by default None.
            If None, uses the one specified in the client.
            Will be set correctly if you are using the framework_client
        version : int or None, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.

        Returns
        -------
        list
            List of table names
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version
        url = self._endpoints["tables"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_annotation_count(self, table_name: str, datastack_name=None, version=None):
        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["version"] = version

        url = self._endpoints["table_count"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_version_metadata(
        self, version: Optional[int] = None, datastack_name: str = None
    ):
        """Get metadata about a version

        Parameters
        ----------
        version : int or None, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        datastack_name : str or None, optional
            Datastack name, by default None. If None, defaults to the value set in the client.

        Returns
        -------
        dict
            Dictionary of metadata about the version
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version
        url = self._endpoints["version_metadata"].format_map(endpoint_mapping)

        response = self.session.get(url)
        d = handle_response(response)
        d["time_stamp"] = convert_timestamp(d["time_stamp"])
        d["expires_on"] = convert_timestamp(d["expires_on"])
        return d

    def get_timestamp(self, version: Optional[int] = None, datastack_name: str = None):
        """Get datetime.datetime timestamp for a materialization version.

        Parameters
        ----------
        version : int or None, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        datastack_name : str or None, optional
            Datastack name, by default None. If None, defaults to the value set in the client.

        Returns
        -------
        datetime.datetime
            Datetime when the materialization version was frozen.
        """
        meta = self.get_version_metadata(version=version, datastack_name=datastack_name)
        return convert_timestamp(meta["time_stamp"])

    @cached(cache=TTLCache(maxsize=100, ttl=60 * 60 * 12))
    def get_versions_metadata(self, datastack_name=None, expired=False):
        """Get the metadata for all the versions that are presently available and valid

        Parameters
        ----------
        datastack_name : str or None, optional
            Datastack name, by default None. If None, defaults to the value set in the client.
        expired : bool, optional
            Whether to include expired versions, by default False.

        Returns
        -------
        list[dict]
            List of metadata dictionaries
        """

        if datastack_name is None:
            datastack_name = self.datastack_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        url = self._endpoints["versions_metadata"].format_map(endpoint_mapping)
        query_args = {"expired": expired}
        response = self.session.get(url, params=query_args)
        d = handle_response(response)
        for md in d:
            md["time_stamp"] = convert_timestamp(md["time_stamp"])
            md["expires_on"] = convert_timestamp(md["expires_on"])
        return d

    @cached(cache=TTLCache(maxsize=100, ttl=60 * 60 * 12))
    def get_table_metadata(
        self,
        table_name: str,
        datastack_name=None,
        version: Optional[int] = None,
        log_warning: bool = True,
    ):
        """Get metadata about a table

        Parameters
        ----------
        table_name : str
            name of table to mark for deletion
        datastack_name : str or None, optional
            Name of the datastack_name. If None, uses the one specified in the client.
        version : int, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        log_warning : bool, optional
            Whether to print out warnings to the logger. Defaults to True.

        Returns
        -------
        dict
            Metadata dictionary for table
        """

        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["version"] = version

        url = self._endpoints["metadata"].format_map(endpoint_mapping)

        response = self.session.get(url)
        metadata_d = handle_response(response, log_warning=log_warning)
        vx = metadata_d.pop("voxel_resolution_x", None)
        vy = metadata_d.pop("voxel_resolution_y", None)
        vz = metadata_d.pop("voxel_resolution_z", None)
        metadata_d["voxel_resolution"] = [vx, vy, vz]
        return metadata_d

    def _format_query_components(
        self,
        datastack_name,
        version,
        tables,
        select_columns,
        suffix_map,
        filter_in_dict,
        filter_out_dict,
        filter_equal_dict,
        filter_spatial_dict,
        filter_regex_dict,
        return_pyarrow,
        split_positions,
        offset,
        limit,
        desired_resolution,
        use_view=False,
        random_sample: int = None,
    ):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version
        data = {}
        query_args = {}
        query_args["return_pyarrow"] = return_pyarrow
        query_args["arrow_format"] = return_pyarrow
        query_args["split_positions"] = split_positions
        if random_sample:
            query_args["random_sample"] = random_sample
        if len(tables) == 1:
            if use_view:
                endpoint_mapping["view_name"] = tables[0]
                url = self._endpoints["view_query"].format_map(endpoint_mapping)
            else:
                endpoint_mapping["table_name"] = tables[0]
                url = self._endpoints["simple_query"].format_map(endpoint_mapping)
        else:
            data["tables"] = tables
            url = self._endpoints["join_query"].format_map(endpoint_mapping)

        if filter_in_dict is not None:
            data["filter_in_dict"] = filter_in_dict
        if filter_out_dict is not None:
            data["filter_notin_dict"] = filter_out_dict
        if filter_equal_dict is not None:
            data["filter_equal_dict"] = filter_equal_dict
        if filter_spatial_dict is not None:
            data["filter_spatial_dict"] = filter_spatial_dict
        if filter_regex_dict is not None:
            data["filter_regex_dict"] = filter_regex_dict
        if select_columns is not None:
            if isinstance(select_columns, list):
                data["select_columns"] = select_columns
            elif isinstance(select_columns, dict):
                data["select_column_map"] = select_columns
            else:
                raise ValueError(
                    "select columns should be a dictionary with tables as keys and values of column names in table (no suffixes)"
                )
        if offset is not None:
            data["offset"] = offset
        if suffix_map is not None:
            if isinstance(suffix_map, list):
                data["suffixes"] = suffix_map
            elif isinstance(suffix_map, dict):
                data["suffix_map"] = suffix_map
            else:
                raise ValueError(
                    "suffixes should be a dictionary with tables as keys and values as suffixes"
                )
        if limit is not None:
            assert limit > 0
            data["limit"] = limit
        if desired_resolution is not None:
            data["desired_resolution"] = desired_resolution
        if return_pyarrow:
            encoding = DEFAULT_COMPRESSION
        else:
            encoding = "gzip"

        return url, data, query_args, encoding

    def _resolve_merge_reference(
        self, merge_reference, table, datastack_name, materialization_version
    ):
        if merge_reference:
            md = self.get_table_metadata(
                table_name=table,
                datastack_name=datastack_name,
                version=materialization_version,
                log_warning=False,
            )
            if md["reference_table"] is None:
                target_table = None
            else:
                if len(md["reference_table"]) == 0:
                    target_table = None
                else:
                    target_table = md["reference_table"]
        else:
            target_table = None
        if target_table is not None:
            tables = [[table, "target_id"], [md["reference_table"], "id"]]
            if self._api_version == 2:
                suffix_map = ["", "ref"]
            elif self._api_version > 2:
                suffix_map = {table: "", md["reference_table"]: "_ref"}
        else:
            tables = [table]

            suffix_map = None
        return tables, suffix_map

    def query_table(
        self,
        table: str,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        filter_regex_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        return_df: bool = True,
        split_positions: bool = False,
        materialization_version: Optional[int] = None,
        timestamp: Optional[datetime] = None,
        metadata: bool = True,
        merge_reference: bool = True,
        desired_resolution: Iterable = None,
        get_counts: bool = False,
        random_sample: int = None,
        log_warning: bool = True,
    ):
        """Generic query on materialization tables

        Parameters
        ----------
        table : str
            Table to query
        filter_in_dict : dict, optional
            Keys are column names, values are allowed entries, by default None
        filter_out_dict : dict, optional
            Keys are column names, values are not allowed entries, by default None
        filter_equal_dict : dict, optional
            Keys are column names, values are specified entry, by default None
        filter_spatial_dict : dict, optional
            Keys are column names, values are bounding boxes expressed in units of the
            voxel_resolution of this dataset. Bounding box is [[min_x, min_y,min_z],[max_x, max_y, max_z]], by default None
        filter_regex_dict : dict, optional
            Keys are column names, values are regex strings, by default None
        select_columns : list of str, optional
            Columns to select, by default None
        offset : int, optional
            Result offset to use, by default None. Will only return top K results.
        limit : int, optional
            Maximum results to return (server will set upper limit,
            see get_server_config), by default None
        datastack_name : str, optional
            Datastack to query, by default None. If None, defaults to one
            specified in client.
        return_df : bool, optional
            Whether to return as a dataframe, by default True. If False, data is
            returned as json (slower).
        split_positions : bool, optional
            Whether to break position columns into x,y,z columns, by default False.
            If False data is returned as one column with [x,y,z] array (slower)
        materialization_version : int, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        timestamp : datetime.datetime, optional
            Timestamp to query, by default None. If passsed will do a live query.
            Error if also passing a materialization version
        metadata : bool, optional
            Toggle to return metadata (default True), by default True. If True
            (and return_df is also True), return table and query metadata in the
            df.attr dictionary.
        merge_reference : bool, optional
            Toggle to automatically join reference table, by default True. If True,
            metadata will be queries and if its a reference table it will perform a
            join on the reference table to return the rows of that
        desired_resolution : Iterable[float], optional
            Desired resolution you want all spatial points returned in, by default None.
            If None, defaults to one specified in client, if that is None then points
            are returned as stored in the table and should be in the resolution
            specified in the table metadata
        get_counts : bool, optional
            Whether to get counts of the query, by default False
        random_sample : int, optional
            If given, will do a tablesample of the of the table to return that many
            annotations
        log_warning : bool, optional
            Whether to log warnings, by default True

        Returns
        -------
        pd.DataFrame
            A pandas dataframe of results of query
        """

        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        if timestamp is not None:
            if materialization_version is not None:
                raise ValueError("cannot specify timestamp and materialization version")
            else:
                return self.live_query(
                    table,
                    timestamp,
                    filter_in_dict=filter_in_dict,
                    filter_out_dict=filter_out_dict,
                    filter_equal_dict=filter_equal_dict,
                    filter_spatial_dict=filter_spatial_dict,
                    filter_regex_dict=filter_regex_dict,
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    datastack_name=datastack_name,
                    split_positions=split_positions,
                    post_filter=True,
                    metadata=metadata,
                    merge_reference=merge_reference,
                    desired_resolution=desired_resolution,
                    random_sample=random_sample,
                    log_warning=log_warning,
                )
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

        tables, suffix_map = self._resolve_merge_reference(
            merge_reference, table, datastack_name, materialization_version
        )

        url, data, query_args, encoding = self._format_query_components(
            datastack_name,
            materialization_version,
            tables,
            select_columns,
            suffix_map,
            {table: filter_in_dict} if filter_in_dict is not None else None,
            {table: filter_out_dict} if filter_out_dict is not None else None,
            {table: filter_equal_dict} if filter_equal_dict is not None else None,
            {table: filter_spatial_dict} if filter_spatial_dict is not None else None,
            {table: filter_regex_dict} if filter_regex_dict is not None else None,
            return_df,
            True,
            offset,
            limit,
            desired_resolution,
            random_sample=random_sample,
        )
        if get_counts:
            query_args["count"] = True

        headers = {"Content-Type": "application/json", "Accept-Encoding": encoding}

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers=headers,
            params=query_args,
            stream=~return_df,
        )
        self.raise_for_status(response, log_warning=log_warning)
        if return_df:
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)
                if desired_resolution is not None:
                    if not response.headers.get("dataframe_resolution", None):
                        if len(desired_resolution) != 3:
                            raise ValueError(
                                "desired resolution needs to be of length 3, for xyz"
                            )
                        vox_res = self.get_table_metadata(
                            table,
                            datastack_name,
                            materialization_version,
                            log_warning=False,
                        )["voxel_resolution"]
                        df = convert_position_columns(df, vox_res, desired_resolution)
            if metadata:
                attrs = self._assemble_attributes(
                    tables,
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                        "regex": filter_regex_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=timestamp is not None,
                    timestamp=string_format_timestamp(timestamp),
                    materialization_version=materialization_version,
                    desired_resolution=response.headers.get(
                        "dataframe_resolution", desired_resolution
                    ),
                    column_names=response.headers.get("column_names", None),
                )
                df.attrs.update(attrs)
            if split_positions:
                return df
            else:
                return concatenate_position_columns(df, inplace=True)
        else:
            return response.json()

    def join_query(
        self,
        tables,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        filter_regex_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        suffixes: list = None,
        datastack_name: str = None,
        return_df: bool = True,
        split_positions: bool = False,
        materialization_version: int = None,
        metadata: bool = True,
        desired_resolution: Iterable = None,
        random_sample: int = None,
        log_warning: bool = True,
    ):
        """Generic query on materialization tables

        Parameters
        ----------
        tables : list of lists with length 2 or 'str'
            list of two lists: first entries are table names, second entries are the
            columns used for the join.
        filter_in_dict : dict of dicts, optional
            outer layer: keys are table names
            inner layer: keys are column names, values are allowed entries, by default None
        filter_out_dict : dict of dicts, optional
            outer layer: keys are table names
            inner layer: keys are column names, values are not allowed entries, by default None
        filter_equal_dict : dict of dicts, optional
            outer layer: keys are table names
            inner layer: keys are column names, values are specified entry, by default None
        filter_spatial_dict : dict of dicts, optional
            outer layer: keys are table names, inner layer: keys are column names.
            Values are bounding boxes as [[min_x, min_y,min_z],[max_x, max_y, max_z]],
            expressed in units of the voxel_resolution of this dataset. Defaults to None.
        filter_regex_dict : dict of dicts, optional
            outer layer: keys are table names. inner layer: keys are column names,
            values are regex strings. Defaults to None
        select_columns : dict of lists of str, optional
            keys are table names,values are the list of columns from that table.
            Defaults to None, which will select all tables.  Will be passed to server
            as select_column_maps. Passing a list will be passed as select_columns
            which is deprecated.
        offset : int, optional
            result offset to use. Defaults to None. Will only return top K results.
        limit : int, optional
            maximum results to return (server will set upper limit, see get_server_config)
        suffixes : dict, optional
            suffixes to use for duplicate columns, keys are table names, values are the suffix
        datastack_name : str, optional
            datastack to query. If None defaults to one specified in client.
        return_df : bool, optional
            whether to return as a dataframe default True, if False, data is returned
            as json (slower)
        split_positions : bool, optional
            whether to break position columns into x,y,z columns default False, if False
            data is returned as one column with [x,y,z] array (slower)
        materialization_version : int, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        metadata : bool, optional
            toggle to return metadata If True (and return_df is also True), return
            table and query metadata in the df.attr dictionary.
        desired_resolution : Iterable, optional
            What resolution to convert position columns to. Defaults to None will use
            defaults.
        random_sample : int, optional
            if given, will do a tablesample of the table to return that many annotations
        log_warning : bool, optional
            Whether to log warnings, by default True

        Returns
        -------
        pd.DataFrame
            a pandas dataframe of results of query
        """

        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name
        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        url, data, query_args, encoding = self._format_query_components(
            datastack_name,
            materialization_version,
            tables,
            select_columns,
            suffixes,
            filter_in_dict,
            filter_out_dict,
            filter_equal_dict,
            filter_spatial_dict,
            filter_regex_dict,
            return_df,
            True,
            offset,
            limit,
            desired_resolution,
            random_sample=random_sample,
        )

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json", "Accept-Encoding": encoding},
            params=query_args,
            stream=~return_df,
        )
        self.raise_for_status(response, log_warning=log_warning)
        if return_df:
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)

            if metadata:
                attrs = self._assemble_attributes(
                    tables,
                    suffixes=suffixes,
                    desired_resolution=response.headers.get(
                        "dataframe_resolution", desired_resolution
                    ),
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                        "regex": filter_regex_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=False,
                    timestamp=None,
                    materialization_version=materialization_version,
                    column_names=response.headers.get("column_names", None),
                )
                df.attrs.update(attrs)
            if split_positions:
                return df
            else:
                return concatenate_position_columns(df, inplace=True)

    def map_filters(self, filters, timestamp, timestamp_past):
        """Translate a list of filter dictionaries from a point in the
        future to a point in the past

        Parameters
        ----------
        filters : list[dict]
            filter dictionaries with root_ids
        timestamp : datetime.datetime
            timestamp to query
        timestamp_past : datetime.datetime
            timestamp to query from

        Returns
        -------
        list[dict]
            filter dictionaries with past root_ids
        dict
            mapping of future root_ids to past root_ids
        """
        timestamp = convert_timestamp(timestamp)
        timestamp_past = convert_timestamp(timestamp_past)

        new_filters = []
        root_ids = []
        for filter_dict in filters:
            if filter_dict is not None:
                for col, val in filter_dict.items():
                    if col.endswith("root_id"):
                        if not isinstance(val, (Iterable, np.ndarray)):
                            root_ids.append([val])
                        else:
                            root_ids.append(val)

        # if there are no root_ids then we can safely return now
        if len(root_ids) == 0:
            return filters, {}
        root_ids = np.unique(np.concatenate(root_ids))

        filter_timed_end = self.cg_client.is_latest_roots(root_ids, timestamp=timestamp)
        filter_timed_start = self.cg_client.get_root_timestamps(root_ids) < timestamp
        filter_timestamp = np.logical_and(filter_timed_start, filter_timed_end)
        if not np.all(filter_timestamp):
            roots_too_old = root_ids[~filter_timed_end]
            roots_too_recent = root_ids[~filter_timed_start]

            if len(roots_too_old) > 0:
                too_old_str = f"{roots_too_old} are expired, "
            else:
                too_old_str = ""
            if len(roots_too_recent) > 0:
                too_recent_str = f"{roots_too_recent} are too recent, "
            else:
                too_recent_str = ""

            raise ValueError(
                f"Timestamp incompatible with IDs: {too_old_str}{too_recent_str} use chunkedgraph client to find valid ID(s)"
            )

        id_mapping = self.cg_client.get_past_ids(
            root_ids, timestamp_past=timestamp_past, timestamp_future=timestamp
        )
        for filter_dict in filters:
            if filter_dict is None:
                new_filters.append(filter_dict)
            else:
                new_dict = {}
                for col, root_ids in filter_dict.items():
                    if col.endswith("root_id"):
                        if not isinstance(root_ids, (Iterable, np.ndarray)):
                            new_dict[col] = id_mapping["past_id_map"][root_ids]
                        else:
                            new_dict[col] = np.concatenate(
                                [id_mapping["past_id_map"][v] for v in root_ids]
                            )
                    else:
                        new_dict[col] = root_ids
                new_filters.append(new_dict)
        return new_filters, id_mapping["future_id_map"]

    def _update_rootids(self, df: pd.DataFrame, timestamp: datetime, future_map: dict):
        # post process the dataframe to update all the root_ids columns
        # with the most up to date get roots
        if len(future_map) == 0:
            future_map = None

        if future_map is not None:
            # pyarrow can make dataframes read only. Copying resets that.
            df = df.copy()

        sv_columns = [c for c in df.columns if c.endswith("supervoxel_id")]
        with MyTimeIt("is_latest_roots"):
            all_root_ids = np.empty(0, dtype=np.int64)

            # go through the columns and collect all the root_ids to check
            # to see if they need updating
            for sv_col in sv_columns:
                root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                # use the future map to update rootIDs
                if future_map is not None:
                    df.replace({root_id_col: future_map}, inplace=True)
                all_root_ids = np.append(all_root_ids, df[root_id_col].values.copy())

            uniq_root_ids = np.unique(all_root_ids)

            del all_root_ids
            uniq_root_ids = uniq_root_ids[uniq_root_ids != 0]
            # logging.info(f"uniq_root_ids {uniq_root_ids}")

            is_latest_root = self.cg_client.is_latest_roots(
                uniq_root_ids, timestamp=timestamp
            )
            latest_root_ids = uniq_root_ids[is_latest_root]
            latest_root_ids = np.concatenate([[0], latest_root_ids])

            # go through the columns and collect all the supervoxel ids to update
            all_svids = np.empty(0, dtype=np.int64)
            all_is_latest = []
            all_svid_lengths = []
            for sv_col in sv_columns:
                with MyTimeIt(f"find svids {sv_col}"):
                    root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                    svids = df[sv_col].values
                    root_ids = df[root_id_col]
                    is_latest_root = np.isin(root_ids, latest_root_ids)
                    all_is_latest.append(is_latest_root)
                    n_svids = len(svids[~is_latest_root])
                    all_svid_lengths.append(n_svids)
                    logger.info(f"{sv_col} has {n_svids} to update")
                    all_svids = np.append(all_svids, svids[~is_latest_root])
        logger.info(f"num zero svids: {np.sum(all_svids==0)}")
        logger.info(f"all_svids dtype {all_svids.dtype}")
        logger.info(f"all_svid_lengths {all_svid_lengths}")
        with MyTimeIt("get_roots"):
            # find the up to date root_ids for those supervoxels
            updated_root_ids = self.cg_client.get_roots(all_svids, timestamp=timestamp)
            del all_svids

        # loop through the columns again replacing the root ids with their updated
        # supervoxelids
        k = 0
        for is_latest_root, n_svids, sv_col in zip(
            all_is_latest, all_svid_lengths, sv_columns
        ):
            with MyTimeIt(f"replace_roots {sv_col}"):
                root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                root_ids = df[root_id_col].values.copy()

                uroot_id = updated_root_ids[k : k + n_svids]
                k += n_svids
                root_ids[~is_latest_root] = uroot_id
                # ran into an isssue with pyarrow producing read only columns
                df[root_id_col] = None
                df[root_id_col] = root_ids

        return df

    def ingest_annotation_table(
        self,
        table_name: str,
        datastack_name: str = None,
    ):
        """Trigger supervoxel lookup and root ID lookup of new annotations in a table.

        Parameters
        ----------
        table_name : str
            Table to trigger
        datastack_name : str, optional
            Datastack to trigger it. Defaults to what is set in client.

        Returns
        -------
        dict
            Status code of response from server
        """
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["ingest_annotation_table"].format_map(endpoint_mapping)
        response = self.session.post(url)
        return handle_response(response)

    def lookup_supervoxel_ids(
        self,
        table_name: str,
        annotation_ids: list = None,
        datastack_name: str = None,
    ) -> dict:
        """Trigger supervoxel lookups of new annotations in a table.


        Parameters
        ----------
        table_name : str
            Table to trigger
        annotation_ids : list, optional
            List of annotation ids to lookup. Default is None, which will trigger
            lookup of entire table.
        datastack_name : str, optional
            Datastack to trigger it. Defaults to what is set in client.

        Returns
        -------
        dict
            Status code of response from server
        """
        if datastack_name is None:
            datastack_name = self.datastack_name

        if annotation_ids is not None:
            data = {"annotation_ids": annotation_ids}
        else:
            data = {}
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["lookup_supervoxel_ids"].format_map(endpoint_mapping)
        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={
                "Content-Type": "application/json",
                "Accept-Encoding": "",
            },
        )
        return handle_response(response)

    def live_live_query(
        self,
        table: str,
        timestamp: datetime,
        joins=None,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        split_positions: bool = False,
        metadata: bool = True,
        suffixes: dict = None,
        desired_resolution: Iterable = None,
        allow_missing_lookups: bool = False,
        random_sample: int = None,
        log_warning: bool = True,
    ):
        """Beta method for querying cave annotation tables with rootIDs and annotations
        at a particular timestamp.  Note: this method requires more explicit mapping of
        filters and selection to table as its designed to test a more general endpoint
        that should eventually support complex joins.

        Parameters
        ----------
        table:
            Principle table to query
        timestamp:
            Timestamp to query
        joins: list of lists of str, optional
            List of joins, where each join is a list of [table1,column1, table2, column2]
        filter_in_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and list
            values to accept.
        filter_out_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and list
            values to reject.
        filter_equal_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and values
            to equate.
        filter_spatial_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and values
            of 2x3 list of bounds.
        select_columns: dict of lists of str, optional
            A dictionary with tables as keys, values are lists of columns to select.
        offset:
            Value to offset query by.
        limit:
            Limit of query.
        datastack_name:
            Datastack to query. Defaults to set by client.
        split_positions:
            Whether to split positions into separate columns, True is faster.
        metadata:
            Whether to attach metadata to dataframe.
        suffixes:
            What suffixes to use on joins, keys are table_names, values are suffixes.
        desired_resolution:
            What resolution to convert position columns to.
        allow_missing_lookups:
            If there are annotations without supervoxels and root IDs yet, allow results.
        random_sample:
            If given, will do a table sample of the table to return that many annotations.
        log_warning:
            Whether to log warnings.

        Returns
        -------
        :
            Results of query

        Examples
        --------
        >>> from caveclient import CAVEclient
        >>> client = CAVEclient('minnie65_public_v117')
        >>> live_live_query("table_name", datetime.datetime.now(datetime.timezone.utc),
        >>>    joins=[[table_name, table_column, joined_table, joined_column],
        >>>             [joined_table, joincol2, third_table, joincol_third]]
        >>>    suffixes={
        >>>        "table_name":"suffix1",
        >>>        "joined_table":"suffix2",
        >>>        "third_table":"suffix3"
        >>>    },
        >>>    select_columns= {
        >>>        "table_name":[ "column","names"],
        >>>        "joined_table":["joined_colum"]
        >>>    },
        >>>    filter_in_dict= {
        >>>        "table_name":{
        >>>            "column_name":[included,values]
        >>>        }
        >>>    },
        >>>    filter_out_dict= {
        >>>        "table_name":{
        >>>            "column_name":[excluded,values]
        >>>        }
        >>>    },
        >>>    filter_equal_dict"={
        >>>        "table_name":{
        >>>            "column_name":value
        >>>        },
        >>>    filter_spatial_dict"= {
        >>>        "table_name": {
        >>>        "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
        >>>    }
        >>>    filter_regex_dict"= {
        >>>        "table_name": {
        >>>        "column_name": "regex_string"
        >>>     }
        """

        logging.warning(
            "Deprecation: this method is to facilitate beta testing of this feature, \
            it will likely get removed in future versions. "
        )
        timestamp = convert_timestamp(timestamp)
        return_df = True
        if datastack_name is None:
            datastack_name = self.datastack_name
        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        data = {}
        query_args = {}
        query_args["return_pyarrow"] = True
        query_args["arrow_format"] = True
        query_args["merge_reference"] = False
        query_args["allow_missing_lookups"] = allow_missing_lookups
        if random_sample:
            query_args["random_sample"] = random_sample
        data["table"] = table
        data["timestamp"] = timestamp
        url = self._endpoints["live_live_query"].format_map(endpoint_mapping)
        if joins is not None:
            data["join_tables"] = joins
        if filter_in_dict is not None:
            data["filter_in_dict"] = filter_in_dict
        if filter_out_dict is not None:
            data["filter_notin_dict"] = filter_out_dict
        if filter_equal_dict is not None:
            data["filter_equal_dict"] = filter_equal_dict
        if filter_spatial_dict is not None:
            data["filter_spatial_dict"] = filter_spatial_dict
        if select_columns is not None:
            data["select_columns"] = select_columns
        if offset is not None:
            data["offset"] = offset
        if limit is not None:
            assert limit > 0
            data["limit"] = limit
        if suffixes is not None:
            data["suffixes"] = suffixes
        encoding = DEFAULT_COMPRESSION

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={
                "Content-Type": "application/json",
                "Accept-Encoding": encoding,
            },
            params=query_args,
            stream=~return_df,
            verify=self.verify,
        )
        self.raise_for_status(response, log_warning=log_warning)

        if desired_resolution is None:
            desired_resolution = self.desired_resolution

        with MyTimeIt("deserialize"):
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)
                if desired_resolution is not None:
                    if len(desired_resolution) != 3:
                        raise ValueError(
                            "desired resolution needs to be of length 3, for xyz"
                        )
                    vox_res = self.get_table_metadata(
                        table_name=table,
                        datastack_name=datastack_name,
                        log_warning=False,
                    )["voxel_resolution"]
                    df = convert_position_columns(df, vox_res, desired_resolution)
            if not split_positions:
                concatenate_position_columns(df, inplace=True)

        if metadata:
            try:
                attrs = self._assemble_attributes(
                    table,
                    join_query=False,
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=timestamp is not None,
                    timestamp=string_format_timestamp(timestamp),
                    materialization_version=None,
                    desired_resolution=desired_resolution,
                )
                df.attrs.update(attrs)
            except HTTPError as e:
                raise Exception(
                    e.message
                    + " Metadata could not be loaded, try with metadata=False if not needed"
                )
        return df

    def live_query(
        self,
        table: str,
        timestamp: datetime,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        filter_regex_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        split_positions: bool = False,
        post_filter: bool = True,
        metadata: bool = True,
        merge_reference: bool = True,
        desired_resolution: Iterable = None,
        random_sample: int = None,
        log_warning: bool = True,
    ):
        """Generic query on materialization tables

        Parameters
        ----------
        table : str
            Table to query
        timestamp : datetime.datetime
            Time to materialize (in utc). Pass
            datetime.datetime.now(datetime.timezone.utc) for present time.
        filter_in_dict : dict, optional
            Keys are column names, values are allowed entries.
        filter_out_dict : dict, optional
            Keys are column names, values are not allowed entries.
        filter_equal_dict : dict, optional
            Keys are column names, values are specified entry.
        filter_spatial_dict : dict, optional
            Keys are column names, values are bounding boxes expressed in units of the
            voxel_resolution of this dataset. Bounding box is
            [[min_x, min_y,min_z],[max_x, max_y, max_z]].
        filter_regex_dict : dict, optional
            Keys are column names, values are regex strings.
        select_columns : list of str, optional
            Columns to select.
        offset : int, optional
            Offset in query result.
        limit : int, optional
            Maximum results to return (server will set upper limit, see get_server_config).
        datastack_name : str, optional
            Datastack to query. If None, defaults to one specified in client.
        split_positions : bool, optional
            Whether to break position columns into x,y,z columns. If False data is
            returned as one column with [x,y,z] array (slower).
        post_filter : bool, optional
            Whether to filter down the result based upon the filters specified. If False,
            it will return the query with present root_ids in the root_id columns, but the
            rows will reflect the filters translated into their past IDs. So if, for example,
            a cell had a false merger split off since the last materialization, those
            annotations on that incorrect portion of the cell will be included if this is
            False, but will be filtered down if this is True.
        metadata : bool, optional
            Toggle to return metadata. If True (and return_df is also True), return table
            and query metadata in the df.attr dictionary.
        merge_reference : bool, optional
            Toggle to automatically join reference table. If True, metadata will be queries
            and if its a reference table it will perform a join on the reference table to
            return the rows of that table.
        desired_resolution : Iterable, optional
            Desired resolution you want all spatial points returned in. If None, defaults to
            one specified in client, if that is None then points are returned as stored in
            the table and should be in the resolution specified in the table metadata.
        random_sample : int, optional
            If given, will do a tablesample of the table to return that many annotations.
        log_warning : bool, optional
            Whether to log warnings.

        Returns
        -------
        pd.DataFrame
            A pandas dataframe of results of query
        """

        timestamp = convert_timestamp(timestamp)
        return_df = True
        if self.cg_client is None:
            raise ValueError("You must have a cg_client to run live_query")

        if datastack_name is None:
            datastack_name = self.datastack_name
        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        with MyTimeIt("find_mat_version"):
            # we want to find the most recent materialization
            # in which the timestamp given is in the future
            mds = self.get_versions_metadata()
            materialization_version = None
            # make sure the materialization's are increasing in ID/time
            for md in sorted(mds, key=lambda x: x["id"]):
                ts = md["time_stamp"]
                if timestamp >= ts:
                    materialization_version = md["version"]
                    if timestamp == ts:
                        # If timestamp equality to a version, use the standard query_table.
                        return self.query_table(
                            table=table,
                            filter_in_dict=filter_in_dict,
                            filter_out_dict=filter_out_dict,
                            filter_equal_dict=filter_equal_dict,
                            filter_spatial_dict=filter_spatial_dict,
                            filter_regex_dict=filter_regex_dict,
                            select_columns=select_columns,
                            offset=offset,
                            limit=limit,
                            datastack_name=datastack_name,
                            split_positions=split_positions,
                            materialization_version=materialization_version,
                            metadata=metadata,
                            merge_reference=merge_reference,
                            desired_resolution=desired_resolution,
                            return_df=True,
                            random_sample=random_sample,
                            log_warning=log_warning,
                        )
                    else:
                        timestamp_start = ts
            # if none of the available versions are before
            # this timestamp, then we cannot support the query
            if materialization_version is None:
                raise (
                    ValueError(
                        """The timestamp you passed is not recent enough
                for the materialization versions that are available"""
                    )
                )

        # first we want to translate all these filters into the IDss at the
        # most recent materialization
        with MyTimeIt("map_filters"):
            past_filters, future_map = self.map_filters(
                [filter_in_dict, filter_out_dict, filter_equal_dict],
                timestamp,
                timestamp_start,
            )
            past_filter_in_dict, past_filter_out_dict, past_equal_dict = past_filters
            if past_equal_dict is not None:
                # when doing a filter equal in the past
                # we translate it to a filter_in, as 1 ID might
                # be multiple IDs in the past.
                # so we want to update the filter_in dict
                cols = [col for col in past_equal_dict.keys()]
                for col in cols:
                    if col.endswith("root_id"):
                        if past_filter_in_dict is None:
                            past_filter_in_dict = {}
                        past_filter_in_dict[col] = past_equal_dict.pop(col)
                if len(past_equal_dict) == 0:
                    past_equal_dict = None

        tables, suffix_map = self._resolve_merge_reference(
            merge_reference, table, datastack_name, materialization_version
        )
        with MyTimeIt("package query"):
            url, data, query_args, encoding = self._format_query_components(
                datastack_name,
                materialization_version,
                tables,
                select_columns,
                suffix_map,
                {table: past_filter_in_dict}
                if past_filter_in_dict is not None
                else None,
                {table: past_filter_out_dict}
                if past_filter_out_dict is not None
                else None,
                {table: past_equal_dict} if past_equal_dict is not None else None,
                {table: filter_spatial_dict}
                if filter_spatial_dict is not None
                else None,
                {table: filter_regex_dict} if filter_regex_dict is not None else None,
                True,
                True,
                offset,
                limit,
                desired_resolution,
                random_sample=random_sample,
            )
            logger.debug(f"query_args: {query_args}")
            logger.debug(f"query data: {data}")
        with MyTimeIt("query materialize"):
            response = self.session.post(
                url,
                data=json.dumps(data, cls=BaseEncoder),
                headers={
                    "Content-Type": "application/json",
                    "Accept-Encoding": encoding,
                },
                params=query_args,
                stream=~return_df,
                verify=self.verify,
            )
            self.raise_for_status(response, log_warning=log_warning)

        if desired_resolution is None:
            desired_resolution = self.desired_resolution

        with MyTimeIt("deserialize"):
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)
                if desired_resolution is not None:
                    if not response.headers.get("dataframe_resolution", None):
                        if len(desired_resolution) != 3:
                            raise ValueError(
                                "desired resolution needs to be of length 3, for xyz"
                            )
                        vox_res = self.get_table_metadata(
                            table,
                            datastack_name,
                            materialization_version,
                            log_warning=False,
                        )["voxel_resolution"]
                        df = convert_position_columns(df, vox_res, desired_resolution)
            if not split_positions:
                concatenate_position_columns(df, inplace=True)
        # post process the dataframe to update all the root_ids columns
        # with the most up to date get roots
        df = self._update_rootids(df, timestamp, future_map)

        # apply the original filters to remove rows
        # from this result which are not relevant
        if post_filter:
            with MyTimeIt("post_filter"):
                if filter_in_dict is not None:
                    for col, val in filter_in_dict.items():
                        df = df[df[col].isin(val)]
                if filter_out_dict is not None:
                    for col, val in filter_out_dict.items():
                        df = df[~df[col].isin(val)]
                if filter_equal_dict is not None:
                    for col, val in filter_equal_dict.items():
                        df = df[df[col] == val]
        if metadata:
            attrs = self._assemble_attributes(
                table,
                join_query=False,
                filters={
                    "inclusive": filter_in_dict,
                    "exclusive": filter_out_dict,
                    "equal": filter_equal_dict,
                    "spatial": filter_spatial_dict,
                    "regex": filter_regex_dict,
                },
                select_columns=select_columns,
                offset=offset,
                limit=limit,
                live_query=timestamp is not None,
                timestamp=string_format_timestamp(timestamp),
                materialization_version=None,
                desired_resolution=response.headers.get(
                    "dataframe_resolution", desired_resolution
                ),
            )
            df.attrs.update(attrs)

        return df

    def synapse_query(
        self,
        pre_ids: Union[int, Iterable, np.ndarray] = None,
        post_ids: Union[int, Iterable, np.ndarray] = None,
        bounding_box: Optional[Union[list, np.ndarray]] = None,
        bounding_box_column: str = "post_pt_position",
        timestamp: datetime = None,
        remove_autapses: bool = True,
        include_zeros: bool = True,
        limit: int = None,
        offset: int = None,
        split_positions: bool = False,
        desired_resolution: Iterable[float] = None,
        materialization_version: Optional[int] = None,
        synapse_table: str = None,
        datastack_name: str = None,
        metadata: bool = True,
    ) -> pd.DataFrame:
        """Convenience method for querying synapses.

        Will use the synapse table
        specified in the info service by default. It will also remove autapses by
        default. NOTE: This is not designed to allow querying of the entire synapse table.
        A query with no filters will return only a limited number of rows (configured
        by the server) and will do so in a non-deterministic fashion. Please contact
        your dataset administrator if you want access to the entire table.

        Parameters
        ----------
        pre_ids:
            Pre-synaptic cell(s) to query.
        post_ids:
            Post-synaptic cell(s) to query.
        bounding_box:
            [[min_x, min_y, min_z],[max_x, max_y, max_z]] bounding box to filter
            synapse locations. Expressed in units of the voxel_resolution of this dataset.
        bounding_box_column:
            Which synapse location column to filter by.
        timestamp:
            Timestamp to query. If passed recalculate query at timestamp, do not pass
            with materialization_version.
        remove_autapses:
            Whether to remove autapses from query results.
        include_zeros:
            Whether to include synapses to/from id=0 (out of segmentation).
        limit:
            Number of synapses to limit. Server-side limit still applies.
        offset:
            Number of synapses to offset query.
        split_positions:
            Whether to split positions into separate columns, True is faster.
        desired_resolution:
            List or array of the desired resolution you want queries returned in
            useful for materialization queries.
        materialization_version:
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        metadata:
            Whether to attach metadata to dataframe in the df.attr dictionary.

        Returns
        -------
        :
            Results of query.
        """
        filter_in_dict = {}
        filter_equal_dict = {}
        filter_out_dict = None
        filter_equal_dict = {}
        filter_spatial_dict = None
        if synapse_table is None:
            if self.synapse_table is None:
                raise ValueError(
                    "Must define synapse table in class init or pass to method"
                )
            synapse_table = self.synapse_table

        if not include_zeros:
            filter_out_dict = {"pre_pt_root_id": [0], "post_pt_root_id": [0]}

        if pre_ids is not None:
            if isinstance(pre_ids, (Iterable, np.ndarray)):
                filter_in_dict["pre_pt_root_id"] = pre_ids
            else:
                filter_equal_dict["pre_pt_root_id"] = pre_ids

        if post_ids is not None:
            if isinstance(post_ids, (Iterable, np.ndarray)):
                filter_in_dict["post_pt_root_id"] = post_ids
            else:
                filter_equal_dict["post_pt_root_id"] = post_ids
        if bounding_box is not None:
            filter_spatial_dict = {bounding_box_column: bounding_box}

        df = self.query_table(
            synapse_table,
            filter_in_dict=filter_in_dict,
            filter_out_dict=filter_out_dict,
            filter_equal_dict=filter_equal_dict,
            filter_spatial_dict=filter_spatial_dict,
            offset=offset,
            limit=limit,
            split_positions=split_positions,
            desired_resolution=desired_resolution,
            materialization_version=materialization_version,
            timestamp=timestamp,
            datastack_name=datastack_name,
            metadata=metadata,
            merge_reference=False,
        )

        if metadata:
            df.attrs["remove_autapses"] = remove_autapses

        if remove_autapses:
            return df.query("pre_pt_root_id!=post_pt_root_id")
        else:
            return df

    def _assemble_attributes(
        self, tables, suffixes=None, desired_resolution=None, is_view=False, **kwargs
    ):
        if isinstance(tables, str):
            tables = [tables]
        if isinstance(desired_resolution, str):
            desired_resolution = [float(r) for r in desired_resolution.split(", ")]
        join_query = len(tables) > 1
        materialization_version = kwargs.get("materialization_version", None)
        attrs = {
            "datastack_name": self.datastack_name,
        }
        if not join_query:
            attrs["join_query"] = False

            if is_view:
                meta = self.get_view_metadata(
                    tables[0],
                    log_warning=False,
                    materialization_version=materialization_version,
                )
            else:
                try:
                    meta = self.get_table_metadata(
                        tables[0], log_warning=False, version=materialization_version
                    )
                except HTTPError:
                    meta = self.fc.annotation.get_table_metadata(tables[0])

            for k, v in meta.items():
                if re.match("^table", k):
                    attrs[k] = v
                else:
                    attrs[f"table_{k}"] = v
            if desired_resolution is None:
                attrs["dataframe_resolution"] = attrs["table_voxel_resolution"]
            else:
                attrs["dataframe_resolution"] = desired_resolution
        else:
            attrs["join_query"] = True
            attrs["tables"] = {}
            table_attrs = attrs["tables"]
            if suffixes is None:
                suffixes = ["_x", "_y"]
            for (tname, jcol), s in zip(tables, suffixes):
                table_attrs[tname] = {}
                try:
                    meta = self.get_table_metadata(tname, log_warning=False)
                except HTTPError:
                    meta = self.fc.annotation.get_table_metadata(tname)
                for k, v in meta.items():
                    if re.match("^table", k):
                        table_attrs[tname][k] = v
                    else:
                        table_attrs[tname][f"table_{k}"] = v
                table_attrs[tname]["join_column"] = jcol
                table_attrs[tname]["suffix"] = s

            if desired_resolution is None:
                res = []
                for tn in attrs["tables"]:
                    res.append(attrs["tables"][tn]["table_voxel_resolution"])
                if np.atleast_2d(np.unique(np.array(res), axis=0)).shape[0] == 1:
                    attrs["dataframe_resolution"] = list(res[0])
                else:
                    attrs["dataframe_resolution"] = "mixed_resolutions"
            else:
                attrs["dataframe_resolution"] = desired_resolution

        attrs.update(kwargs)
        return json.loads(json.dumps(attrs, cls=BaseEncoder))


def _tables_metadata_key(matclient, *args, **kwargs):
    if "version" in kwargs:
        version = kwargs["version"]
    else:
        version = matclient.version
    if "datastack_name" in kwargs:
        datastack_name = kwargs["datastack_name"]
    else:
        datastack_name = matclient.datastack_name
    return hashkey(datastack_name, version)


class MaterializationClientV3(MaterializationClientV2):
    def __init__(self, *args, **kwargs):
        super(MaterializationClientV3, self).__init__(*args, **kwargs)

    @property
    def tables(self) -> TableManager:
        """The table manager for the materialization engine."""
        if self._tables is None:
            if self.fc is not None and self.fc._materialize is not None:
                metadata = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    metadata.append(
                        executor.submit(
                            self.get_tables_metadata,
                        )
                    )
                    metadata.append(
                        executor.submit(self.fc.schema.schema_definition_all)
                    )

                if (
                    metadata[0].result() is not None
                    and metadata[1].result() is not None
                ):
                    tables = TableManager(
                        self.fc, metadata[0].result(), metadata[1].result()
                    )
                else:
                    # TODO fix this for when the metadata is not available
                    tables = None
                self._tables = tables
            else:
                raise ValueError("No full CAVEclient specified")
        return self._tables

    @property
    def views(self) -> ViewManager:
        """The view manager for the materialization engine."""
        if self._views is None:
            if self.fc is not None and self.fc._materialize is not None:
                metadata = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    metadata.append(
                        executor.submit(
                            self.get_views,
                        )
                    )
                    metadata.append(executor.submit(self.get_view_schemas))

                views = ViewManager(self.fc, metadata[0].result(), metadata[1].result())
                self._views = views
            else:
                raise ValueError("No full CAVEclient specified")
        return self._views

    @cached(cache=TTLCache(maxsize=100, ttl=60 * 60 * 12), key=_tables_metadata_key)
    def get_tables_metadata(
        self,
        datastack_name=None,
        version: Optional[int] = None,
        log_warning: bool = True,
    ) -> dict:
        """Get metadata about tables

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack_name. If None, uses the one specified in the client.
        version :
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        log_warning :
            Whether to print out warnings to the logger. Defaults to True.

        Returns
        -------
        :
            Metadata dictionary for table
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version

        url = self._endpoints["all_tables_metadata"].format_map(endpoint_mapping)

        response = self.session.get(url)
        all_metadata = handle_response(response, log_warning=log_warning)
        for metadata_d in all_metadata:
            vx = metadata_d.pop("voxel_resolution_x", None)
            vy = metadata_d.pop("voxel_resolution_y", None)
            vz = metadata_d.pop("voxel_resolution_z", None)
            metadata_d["voxel_resolution"] = [vx, vy, vz]
        return all_metadata

    def live_live_query(
        self,
        table: str,
        timestamp: datetime,
        joins=None,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        filter_regex_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        split_positions: bool = False,
        metadata: bool = True,
        suffixes: dict = None,
        desired_resolution: Iterable = None,
        allow_missing_lookups: bool = False,
        allow_invalid_root_ids: bool = False,
        random_sample: int = None,
        log_warning: bool = True,
    ):
        """Beta method for querying cave annotation tables with root IDs and annotations
        at a particular timestamp.  Note: this method requires more explicit mapping of
        filters and selection to table as its designed to test a more general endpoint
        that should eventually support complex joins.

        Parameters
        ----------
        table:
            Principle table to query
        timestamp:
            Timestamp to query
        joins: list of lists of str, optional
            List of joins, where each join is a list of [table1,column1, table2, column2]
        filter_in_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and list
            values to accept.
        filter_out_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and list
            values to reject.
        filter_equal_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and values
            to equate.
        filter_spatial_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and values
            of 2x3 list of bounds.
        filter_regex_dict: dict of dicts, optional
            A dictionary with tables as keys, values are dicts with column keys and values
            of regex strings.
        select_columns: dict of lists of str, optional
            A dictionary with tables as keys, values are lists of columns to select.
        offset:
            Value to offset query by.
        limit:
            Limit of query.
        datastack_name:
            Datastack to query. Defaults to set by client.
        split_positions:
            Whether to split positions into separate columns, True is faster.
        metadata:
            Whether to attach metadata to dataframe.
        suffixes:
            What suffixes to use on joins, keys are table_names, values are suffixes.
        desired_resolution:
            What resolution to convert position columns to.
        allow_missing_lookups:
            If there are annotations without supervoxels and root IDs yet, allow results.
        allow_invalid_root_ids:
            If True, ignore root ids not valid at the given timestamp, otherwise raise
            an error.
        random_sample:
            If given, will do a table sample of the table to return that many annotations.
        log_warning:
            Whether to log warnings.

        Returns
        -------
        :
            Results of query

        Examples
        --------
        >>> from caveclient import CAVEclient
        >>> client = CAVEclient('minnie65_public_v117')
        >>> live_live_query("table_name", datetime.datetime.now(datetime.timezone.utc),
        >>>    joins=[[table_name, table_column, joined_table, joined_column],
        >>>             [joined_table, joincol2, third_table, joincol_third]]
        >>>    suffixes={
        >>>        "table_name":"suffix1",
        >>>        "joined_table":"suffix2",
        >>>        "third_table":"suffix3"
        >>>    },
        >>>    select_columns= {
        >>>        "table_name":[ "column","names"],
        >>>        "joined_table":["joined_colum"]
        >>>    },
        >>>    filter_in_dict= {
        >>>        "table_name":{
        >>>            "column_name":[included,values]
        >>>        }
        >>>    },
        >>>    filter_out_dict= {
        >>>        "table_name":{
        >>>            "column_name":[excluded,values]
        >>>        }
        >>>    },
        >>>    filter_equal_dict"={
        >>>        "table_name":{
        >>>            "column_name":value
        >>>        },
        >>>    filter_spatial_dict"= {
        >>>        "table_name": {
        >>>        "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
        >>>    }
        >>>    filter_regex_dict"= {
        >>>        "table_name": {
        >>>        "column_name": "regex_string"
        >>>     }
        """

        logging.warning(
            "Deprecation: this method is to facilitate beta testing of this feature, \
it will likely get removed in future versions. "
        )
        timestamp = convert_timestamp(timestamp)
        return_df = True
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        data = {}
        query_args = {}
        query_args["return_pyarrow"] = True
        query_args["arrow_format"] = True
        query_args["merge_reference"] = False
        query_args["allow_missing_lookups"] = allow_missing_lookups
        query_args["allow_invalid_root_ids"] = allow_invalid_root_ids
        if random_sample:
            query_args["random_sample"] = random_sample
        data["table"] = table
        data["timestamp"] = timestamp

        url = self._endpoints["live_live_query"].format_map(endpoint_mapping)
        if joins is not None:
            data["join_tables"] = joins
        if filter_in_dict is not None:
            data["filter_in_dict"] = filter_in_dict
        if filter_out_dict is not None:
            data["filter_notin_dict"] = filter_out_dict
        if filter_equal_dict is not None:
            data["filter_equal_dict"] = filter_equal_dict
        if filter_spatial_dict is not None:
            data["filter_spatial_dict"] = filter_spatial_dict
        if filter_regex_dict is not None:
            data["filter_regex_dict"] = filter_regex_dict
        if select_columns is not None:
            data["select_columns"] = select_columns
        if offset is not None:
            data["offset"] = offset
        if limit is not None:
            assert limit > 0
            data["limit"] = limit
        if suffixes is not None:
            data["suffixes"] = suffixes
        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        if desired_resolution is not None:
            data["desired_resolution"] = desired_resolution
        encoding = DEFAULT_COMPRESSION

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={
                "Content-Type": "application/json",
                "Accept-Encoding": encoding,
            },
            params=query_args,
            stream=~return_df,
            verify=self.verify,
        )
        self.raise_for_status(response, log_warning=log_warning)

        with MyTimeIt("deserialize"):
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)
                if desired_resolution is not None:
                    if not response.headers.get("dataframe_resolution", None):
                        if len(desired_resolution) != 3:
                            raise ValueError(
                                "desired resolution needs to be of length 3, for xyz"
                            )
                        vox_res = self.get_table_metadata(
                            table,
                            datastack_name,
                            log_warning=False,
                        )["voxel_resolution"]
                        df = convert_position_columns(df, vox_res, desired_resolution)

            if not split_positions:
                concatenate_position_columns(df, inplace=True)

        if metadata:
            try:
                attrs = self._assemble_attributes(
                    table,
                    join_query=False,
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                        "regex": filter_regex_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=timestamp is not None,
                    timestamp=string_format_timestamp(timestamp),
                    materialization_version=None,
                    desired_resolution=response.headers.get(
                        "dataframe_resolution", desired_resolution
                    ),
                )
                df.attrs.update(attrs)
            except HTTPError as e:
                raise Exception(
                    e.message
                    + " Metadata could not be loaded, try with metadata=False if not needed"
                )
        return df

    def get_views(self, version: Optional[int] = None, datastack_name: str = None):
        """
        Get all available views for a version

        Parameters
        ----------
        version :
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        datastack_name :
            Datastack to query. If None, uses the one specified in the client.

        Returns
        -------
        list
            List of views
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if version is None:
            version = self.version
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version
        url = self._endpoints["get_views"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self.verify)
        self.raise_for_status(response)
        return response.json()

    def get_view_metadata(
        self,
        view_name: str,
        materialization_version: Optional[int] = None,
        datastack_name: str = None,
        log_warning: bool = True,
    ):
        """Get metadata for a view

        Parameters
        ----------
        view_name :
            Name of view to query.
        materialization_version :
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        log_warning :
            Whether to log warnings.

        Returns
        -------
        dict
            Metadata of view
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if materialization_version is None:
            materialization_version = self.version

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["view_name"] = view_name
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = materialization_version

        url = self._endpoints["get_view_metadata"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self.verify)
        self.raise_for_status(response, log_warning=log_warning)
        return response.json()

    def get_view_schema(
        self,
        view_name: str,
        materialization_version: Optional[int] = None,
        datastack_name: str = None,
        log_warning: bool = True,
    ):
        """Get schema for a view

        Parameters
        ----------
        view_name:
            Name of view to query.
        materialization_version:
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        log_warning:
            Whether to log warnings.

        Returns
        -------
        dict
            Schema of view.
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if materialization_version is None:
            materialization_version = self.version

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["view_name"] = view_name
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = materialization_version

        url = self._endpoints["view_schema"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self.verify)
        self.raise_for_status(response, log_warning=log_warning)
        return response.json()

    def get_view_schemas(
        self,
        materialization_version: Optional[int] = None,
        datastack_name: str = None,
        log_warning: bool = True,
    ):
        """Get schema for a view

        Parameters
        ----------
        materialization_version:
            Version to query. If None, will use version set by client.
        log_warning:
            Whether to log warnings.

        Returns
        -------
        dict
            Schema of view.
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if materialization_version is None:
            materialization_version = self.version

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = materialization_version

        url = self._endpoints["view_schemas"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self.verify)
        self.raise_for_status(response, log_warning=log_warning)
        return response.json()

    def query_view(
        self,
        view_name: str,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        filter_regex_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        return_df: bool = True,
        split_positions: bool = False,
        materialization_version: Optional[int] = None,
        metadata: bool = True,
        merge_reference: bool = True,
        desired_resolution: Iterable = None,
        get_counts: bool = False,
        random_sample: int = None,
    ):
        """Generic query on a view

        Parameters
        ----------
        view_name : str
            View to query
        filter_in_dict : dict, optional
            Keys are column names, values are allowed entries, by default None
        filter_out_dict : dict, optional
            Keys are column names, values are not allowed entries, by default None
        filter_equal_dict : dict, optional
            Keys are column names, values are specified entry, by default None
        filter_spatial_dict : dict, optional
            Keys are column names, values are bounding boxes expressed in units of the
            voxel_resolution of this dataset. Bounding box is [[min_x, min_y,min_z],[max_x, max_y, max_z]], by default None
        filter_regex_dict : dict, optional
            Keys are column names, values are regex strings, by default None
        select_columns : list of str, optional
            Columns to select, by default None
        offset : int, optional
            Result offset to use, by default None. Will only return top K results.
        limit : int, optional
            Maximum results to return (server will set upper limit,
            see get_server_config), by default None
        datastack_name : str, optional
            Datastack to query, by default None. If None, defaults to one
            specified in client.
        return_df : bool, optional
            Whether to return as a dataframe, by default True. If False, data is
            returned as json (slower).
        split_positions : bool, optional
            Whether to break position columns into x,y,z columns, by default False.
            If False data is returned as one column with [x,y,z] array (slower)
        materialization_version : int, optional
            The version of the datastack to query. If None, will query the client
            `version`, which defaults to the most recent version.
        metadata : bool, optional
            Toggle to return metadata (default True), by default True. If True
            (and return_df is also True), return table and query metadata in the
            df.attr dictionary.
        merge_reference : bool, optional
            Toggle to automatically join reference table, by default True. If True,
            metadata will be queries and if its a reference table it will perform a
            join on the reference table to return the rows of that
        desired_resolution : Iterable[float], optional
            Desired resolution you want all spatial points returned in, by default None.
            If None, defaults to one specified in client, if that is None then points
            are returned as stored in the table and should be in the resolution
            specified in the table metadata
        get_counts : bool, optional
            Whether to get counts of the query, by default False
        random_sample : int, optional
            If given, will do a tablesample of the of the table to return that many
            annotations

        Returns
        -------
        pd.DataFrame
            A pandas dataframe of results of query
        """

        if desired_resolution is None:
            desired_resolution = self.desired_resolution
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

        url, data, query_args, encoding = self._format_query_components(
            datastack_name,
            materialization_version,
            [view_name],
            select_columns,
            None,
            {view_name: filter_in_dict} if filter_in_dict is not None else None,
            {view_name: filter_out_dict} if filter_out_dict is not None else None,
            {view_name: filter_equal_dict} if filter_equal_dict is not None else None,
            {view_name: filter_spatial_dict}
            if filter_spatial_dict is not None
            else None,
            {view_name: filter_regex_dict} if filter_regex_dict is not None else None,
            return_df,
            True,
            offset,
            limit,
            desired_resolution,
            use_view=True,
            random_sample=random_sample,
        )
        if get_counts:
            query_args["count"] = True
        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json", "Accept-Encoding": encoding},
            params=query_args,
            stream=~return_df,
        )
        self.raise_for_status(response)
        if return_df:
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = deserialize_query_response(response)

            if metadata:
                attrs = self._assemble_attributes(
                    [view_name],
                    is_view=True,
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                        "regex": filter_regex_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=False,
                    timestamp=None,
                    materialization_version=materialization_version,
                    desired_resolution=response.headers.get(
                        "dataframe_resolution", desired_resolution
                    ),
                    column_names=response.headers.get("column_names", None),
                )
                df.attrs.update(attrs)
            if split_positions:
                return df
            else:
                return concatenate_position_columns(df, inplace=True)
        else:
            return response.json()

    def get_unique_string_values(
        self, table: str, datastack_name: Optional[str] = None
    ):
        """Get unique string values for a table

        Parameters
        ----------
        table :
            Table to query
        datastack_name :
            Datastack to query. If None, uses the one specified in the client.

        Returns
        -------
        dict[str]
            A dictionary of column names and their unique values
        """
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table

        url = self._endpoints["unique_string_values"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self.verify)
        self.raise_for_status(response)
        return response.json()


# included for historical reasons, there was a typo in the class name
MaterializatonClientV2 = MaterializationClientV2

MaterializatonClientV3 = MaterializationClientV3

client_mapping = {
    2: MaterializationClientV2,
    3: MaterializationClientV3,
    "latest": MaterializationClientV3,
}

MaterializationClientType = Union[MaterializationClientV2, MaterializationClientV3]
