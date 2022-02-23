import logging
from cachetools import cached, TTLCache
from .base import (
    ClientBase,
    BaseEncoder,
    _api_endpoints,
    handle_response,
)
from .auth import AuthClient
from .endpoints import materialization_api_versions, materialization_common
import json
import numpy as np
from datetime import datetime, timezone
import pyarrow as pa
import itertools
from collections.abc import Iterable
from typing import Union
from .timeit import TimeIt
from IPython.display import HTML
import pandas as pd
import pytz
import warnings
import re

SERVER_KEY = "me_server_address"


def convert_position_columns(df, given_resolution, desired_resolution):
    """function to take a dataframe with x,y,z position columns and convert
    them to the desired resolution from the given resolution

    Args:
        df (pd.DataFrame): dataframe to alter
        given_resolution (Iterable(float)): what the given resolution is
        desired_resoultion (Iterable(float)): what the desired resolution is

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
        ts = datetime.utcnow()

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
):
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
    cg_client: caveclient.chunkedgraph.ChunkeGraphClient
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


class MaterializatonClientV2(ClientBase):
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
        super(MaterializatonClientV2, self).__init__(
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
        if version is None:
            version = self.most_recent_version()
        self._version = version
        if cg_client is None:
            self.cg_client = self.fc.chunkedgraph
        else:
            self.cg_client = cg_client
        self.synapse_table = synapse_table
        self.desired_resolution = desired_resolution

    @property
    def datastack_name(self):
        return self._datastack_name

    @property
    def version(self):
        return self._version

    @property
    def homepage(self):
        url = (
            f"{self._server_address}/materialize/views/datastack/{self._datastack_name}"
        )
        return HTML(f'<a href="{url}" target="_blank">Materialization Engine</a>')

    @version.setter
    def version(self, x):
        if int(x) in self.get_versions():
            self._version = int(x)
        else:
            raise ValueError("Version not in materialized database")

    def most_recent_version(self, datastack_name=None):
        """get the most recent version of materialization
        for this datastack name

        Args:
            datastack_name (str, optional): datastack name to find most
            recent materialization of.
            If None, uses the one specified in the client.
        """
        versions = self.get_versions(datastack_name=datastack_name)
        return np.max(np.array(versions))

    def get_versions(self, datastack_name=None):
        """get versions available

        Args:
            datastack_name ([type], optional): [description]. Defaults to None.
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        url = self._endpoints["versions"].format_map(endpoint_mapping)
        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_tables(self, datastack_name=None, version=None):
        """Gets a list of table names for a datastack

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack, by default None.
            If None, uses the one specified in the client.
            Will be set correctly if you are using the framework_client
        version: int or None, optional
            the version to query, else get the tables in the most recent version
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
        # TODO fix up latest version
        url = self._endpoints["tables"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_annotation_count(self, table_name: str, datastack_name=None, version=None):
        """Get number of annotations in a table

        Parameters
        ----------
        table_name (str):
            name of table to mark for deletion
        datastack_name: str or None, optional,
            Name of the datastack_name. If None, uses the one specified in the client.
        version: int or None, optional
            the version to query, else get the tables in the most recent version
        Returns
        -------
        int
            number of annotations
        """
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_count"].format_map(endpoint_mapping)

        response = self.session.get(url)
        self.raise_for_status(response)
        return response.json()

    def get_version_metadata(self, version: int = None, datastack_name: str = None):
        """get metadata about a version

        Args:
            version (int, optional): version number to get metadata about. Defaults to client default version.
            datastack_name (str, optional): datastack to query. Defaults to client default datastack.
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

    def get_timestamp(self, version: int = None, datastack_name: str = None):
        """Get datetime.datetime timestamp for a materialization version.

        Parameters
        ----------
        version : int or None, optional
            Materialization version, by default None. If None, defaults to the value set in the client.
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
    def get_versions_metadata(self, datastack_name=None):
        """get the metadata for all the versions that are presently available and valid

        Args:
            datastack_name (str, optional): datastack to query. If None, defaults to the value set in the client.

        Returns:
            list[dict]: a list of metadata dictionaries
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        url = self._endpoints["versions_metadata"].format_map(endpoint_mapping)
        response = self.session.get(url)
        d = handle_response(response)
        for md in d:
            md["time_stamp"] = convert_timestamp(md["time_stamp"])
            md["expires_on"] = convert_timestamp(md["expires_on"])
        return d

    @cached(cache=TTLCache(maxsize=100, ttl=60 * 60 * 12))
    def get_table_metadata(
        self, table_name: str, datastack_name=None, version: int = None
    ):
        """Get metadata about a table

        Parameters
        ----------
        table_name (str):
            name of table to mark for deletion
        datastack_name: str or None, optional,
            Name of the datastack_name.
            If None, uses the one specified in the client.


        Returns
        -------
        json
            metadata about table
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
        metadata_d = handle_response(response)
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
        suffixes,
        filter_in_dict,
        filter_out_dict,
        filter_equal_dict,
        filter_spatial_dict,
        return_pyarrow,
        split_positions,
        offset,
        limit,
    ):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = version
        data = {}
        query_args = {}
        query_args["return_pyarrow"] = return_pyarrow
        query_args["split_positions"] = split_positions
        if len(tables) == 1:
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
        if select_columns is not None:
            data["select_columns"] = select_columns
        if offset is not None:
            data["offset"] = offset
        if suffixes is not None:
            data["suffixes"] = suffixes
        if limit is not None:
            assert limit > 0
            data["limit"] = limit
        if return_pyarrow:
            encoding = ""
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
            suffixes = ("", "ref")
        else:
            tables = [table]
            suffixes = None
        return tables, suffixes

    def query_table(
        self,
        table: str,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        return_df: bool = True,
        split_positions: bool = False,
        materialization_version: int = None,
        timestamp: datetime = None,
        metadata: bool = True,
        merge_reference: bool = True,
        desired_resolution: Iterable = None,
    ):
        """generic query on materialization tables

        Args:
            table: 'str'

            filter_in_dict (dict , optional):
                keys are column names, values are allowed entries.
                Defaults to None.
            filter_out_dict (dict, optional):
                keys are column names, values are not allowed entries.
                Defaults to None.
            filter_equal_dict (dict, optional):
                inner layer: keys are column names, values are specified entry.
                Defaults to None.
            filter_spatial (dict, optional):
                inner layer: keys are column names, values are bounding boxes
                             as [[min_x, min_y,min_z],[max_x, max_y, max_z]]
                             Expressed in units of the voxel_resolution of this dataset.
            offset (int, optional): offset in query result
            limit (int, optional): maximum results to return (server will set upper limit, see get_server_config)
            select_columns (list of str, optional): columns to select. Defaults to None.
            suffixes: (list[str], optional): suffixes to use on duplicate columns
            offset (int, optional): result offset to use. Defaults to None.
                will only return top K results.
            datastack_name (str, optional): datastack to query.
                If None defaults to one specified in client.
            return_df (bool, optional): whether to return as a dataframe
                default True, if False, data is returned as json (slower)
            split_positions (bool, optional): whether to break position columns into x,y,z columns
                default False, if False data is returned as one column with [x,y,z] array (slower)
            materialization_version (int, optional): version to query.
                If None defaults to one specified in client.
            timestamp (datetime.datetime, optional): timestamp to query
                If passsed will do a live query. Error if also passing a materialization version
            metadata: (bool, optional) : toggle to return metadata (default True)
                If True (and return_df is also True), return table and query metadata in the df.attr dictionary.
            merge_reference: (bool, optional) : toggle to automatically join reference table
                If True, metadata will be queries and if its a reference table it will perform a join
                on the reference table to return the rows of that
            desired_resolution: (Iterable[float], Optional) : desired resolution you want all spatial points returned in
                If None, defaults to one specified in client, if that is None then points are returned
                as stored in the table and should be in the resolution specified in the table metadata
        Returns:
        pd.DataFrame: a pandas dataframe of results of query

        """
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
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    datastack_name=datastack_name,
                    split_positions=split_positions,
                    post_filter=True,
                    metadata=metadata,
                    desired_resolution=desired_resolution,
                )
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

        tables, suffixes = self._resolve_merge_reference(
            merge_reference, table, datastack_name, materialization_version
        )

        url, data, query_args, encoding = self._format_query_components(
            datastack_name,
            materialization_version,
            tables,
            select_columns,
            suffixes,
            {table: filter_in_dict} if filter_in_dict is not None else None,
            {table: filter_out_dict} if filter_out_dict is not None else None,
            {table: filter_equal_dict} if filter_equal_dict is not None else None,
            {table: filter_spatial_dict} if filter_spatial_dict is not None else None,
            return_df,
            True,
            offset,
            limit,
        )

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
                df = pa.deserialize(response.content)
                if desired_resolution is None:
                    desired_resolution = self.desired_resolution
                if desired_resolution is not None:
                    df = df.copy()
                    if len(desired_resolution) != 3:
                        raise ValueError(
                            "desired resolution needs to be of length 3, for xyz"
                        )
                    vox_res = self.get_table_metadata(
                        table, datastack_name, materialization_version
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
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=timestamp is not None,
                    timestamp=string_format_timestamp(timestamp),
                    materialization_version=materialization_version,
                    desired_resolution=desired_resolution,
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
        select_columns=None,
        offset: int = None,
        limit: int = None,
        suffixes: list = None,
        datastack_name: str = None,
        return_df: bool = True,
        split_positions: bool = False,
        materialization_version: int = None,
        metadata: bool = True,
    ):
        """generic query on materialization tables

         Args:
             tables: list of lists with length 2 or 'str'
                 list of two lists: first entries are table names, second
                                    entries are the columns used for the join
             filter_in_dict (dict of dicts, optional):
                 outer layer: keys are table names
                 inner layer: keys are column names, values are allowed entries.
                 Defaults to None.
             filter_out_dict (dict of dicts, optional):
                 outer layer: keys are table names
                 inner layer: keys are column names, values are not allowed entries.
                 Defaults to None.
             filter_equal_dict (dict of dicts, optional):
                 outer layer: keys are table names
                 inner layer: keys are column names, values are specified entry.
                 Defaults to None.
             filter_spatial (dict of dicts, optional):
                 outer layer: keys are table names:
                 inner layer: keys are column names, values are bounding boxes
                              as [[min_x, min_y,min_z],[max_x, max_y, max_z]]
                              Expressed in units of the voxel_resolution of this dataset.
                 Defaults to None
             select_columns (list of str, optional): columns to select. Defaults to None.
             offset (int, optional): result offset to use. Defaults to None.
                 will only return top K results.
             limit (int, optional): maximum results to return (server will set upper limit, see get_server_config)
             suffixes (list[str], optional): suffixes to use for duplicate columns same order as tables
             datastack_name (str, optional): datastack to query.
                 If None defaults to one specified in client.
             return_df (bool, optional): whether to return as a dataframe
                 default True, if False, data is returned as json (slower)
             split_positions (bool, optional): whether to break position columns into x,y,z columns
                 default False, if False data is returned as one column with [x,y,z] array (slower)
             materialization_version (int, optional): version to query.
                 If None defaults to one specified in client.
             metadata: (bool, optional) : toggle to return metadata
                 If True (and return_df is also True), return table and query metadata in the df.attr dictionary.
        Returns:
             pd.DataFrame: a pandas dataframe of results of query

        """
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

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
            return_df,
            True,
            offset,
            limit,
        )

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
                df = pa.deserialize(response.content)

            if metadata:
                attrs = self._assemble_attributes(
                    tables,
                    suffixes=suffixes,
                    filters={
                        "inclusive": filter_in_dict,
                        "exclusive": filter_out_dict,
                        "equal": filter_equal_dict,
                        "spatial": filter_spatial_dict,
                    },
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    live_query=False,
                    timestamp=None,
                    materialization_version=materialization_version,
                )
                df.attrs.update(attrs)
            if split_positions:
                return df
            else:
                return concatenate_position_columns(df, inplace=True)

    def map_filters(self, filters, timestamp, timestamp_past):
        """translate a list of filter dictionaries
           from a point in the future, to a point in the past

        Args:
            filters (list[dict]): filter dictionaries with
            timestamp ([type]): [description]
            timestamp_past ([type]): [description]

        Returns:
            [type]: [description]
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
                f"Timestamp incompatible with IDs: {too_old_str}{too_recent_str}use chunkedgraph client to find valid ID(s)"
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
        with TimeIt("is_latest_roots"):
            all_root_ids = np.empty(0, dtype=np.int64)

            # go through the columns and collect all the root_ids to check
            # to see if they need updating
            for sv_col in sv_columns:
                root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                # use the future map to update rootIDs
                if future_map is not None:
                    df[root_id_col].replace(future_map, inplace=True)
                all_root_ids = np.append(all_root_ids, df[root_id_col].values.copy())

            uniq_root_ids = np.unique(all_root_ids)

            del all_root_ids
            uniq_root_ids = uniq_root_ids[uniq_root_ids != 0]
            logging.info(f"uniq_root_ids {uniq_root_ids}")

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
                with TimeIt(f"find svids {sv_col}"):
                    root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                    svids = df[sv_col].values
                    root_ids = df[root_id_col]
                    is_latest_root = np.isin(root_ids, latest_root_ids)
                    all_is_latest.append(is_latest_root)
                    n_svids = len(svids[~is_latest_root])
                    all_svid_lengths.append(n_svids)
                    logging.info(f"{sv_col} has {n_svids} to update")
                    all_svids = np.append(all_svids, svids[~is_latest_root])
        logging.info(f"num zero svids: {np.sum(all_svids==0)}")
        logging.info(f"all_svids dtype {all_svids.dtype}")
        logging.info(f"all_svid_lengths {all_svid_lengths}")
        with TimeIt("get_roots"):
            # find the up to date root_ids for those supervoxels
            updated_root_ids = self.cg_client.get_roots(all_svids, timestamp=timestamp)
            del all_svids

        # loop through the columns again replacing the root ids with their updated
        # supervoxelids
        k = 0
        for is_latest_root, n_svids, sv_col in zip(
            all_is_latest, all_svid_lengths, sv_columns
        ):
            with TimeIt(f"replace_roots {sv_col}"):
                root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
                root_ids = df[root_id_col].values.copy()

                uroot_id = updated_root_ids[k : k + n_svids]
                k += n_svids
                root_ids[~is_latest_root] = uroot_id
                # ran into an isssue with pyarrow producing read only columns
                df[root_id_col] = None
                df[root_id_col] = root_ids

        return df

    def live_query(
        self,
        table: str,
        timestamp: datetime,
        filter_in_dict=None,
        filter_out_dict=None,
        filter_equal_dict=None,
        filter_spatial_dict=None,
        select_columns=None,
        offset: int = None,
        limit: int = None,
        datastack_name: str = None,
        split_positions: bool = False,
        post_filter: bool = True,
        metadata: bool = True,
        merge_reference: bool = True,
        desired_resolution: Iterable = None,
    ):
        """generic query on materialization tables

        Args:
            table: 'str'
            timestamp (datetime.datetime): time to materialize (in utc)
                pass datetime.datetime.utcnow() for present time
            filter_in_dict (dict , optional):
                keys are column names, values are allowed entries.
                Defaults to None.
            filter_out_dict (dict, optional):
                keys are column names, values are not allowed entries.
                Defaults to None.
            filter_equal_dict (dict, optional):
                inner layer: keys are column names, values are specified entry.
                Defaults to None.
            filter_spatial (dict, optional):
                inner layer: keys are column names, values are bounding boxes
                             as [[min_x, min_y,min_z],[max_x, max_y, max_z]]
                             Expressed in units of the voxel_resolution of this dataset.
                             Defaults to None
            offset (int, optional): offset in query result
            limit (int, optional): maximum results to return (server will set upper limit, see get_server_config)
            select_columns (list of str, optional): columns to select. Defaults to None.
            suffixes: (list[str], optional): suffixes to use on duplicate columns
            offset (int, optional): result offset to use. Defaults to None.
                will only return top K results.
            datastack_name (str, optional): datastack to query.
                If None defaults to one specified in client.
            split_positions (bool, optional): whether to break position columns into x,y,z columns
                default False, if False data is returned as one column with [x,y,z] array (slower)
            post_filter (bool, optional): whether to filter down the result based upon the filters specified
                if false, it will return the query with present root_ids in the root_id columns,
                but the rows will reflect the filters translated into their past IDs.
                So if, for example, a cell had a false merger split off since the last materialization.
                those annotations on that incorrect portion of the cell will be included if this is False,
                but will be filtered down if this is True. (Default=True)
            metadata: (bool, optional) : toggle to return metadata
                If True (and return_df is also True), return table and query metadata in the df.attr dictionary.
            merge_reference: (bool, optional) : toggle to automatically join reference table
                If True, metadata will be queries and if its a reference table it will perform a join
                on the reference table to return the rows of that
            desired_resolution: (Iterable[float], Optional) : desired resolution you want all spatial points returned in
                If None, defaults to one specified in client, if that is None then points are returned
                as stored in the table and should be in the resolution specified in the table metadata

        Returns:
        pd.DataFrame: a pandas dataframe of results of query

        """
        timestamp = convert_timestamp(timestamp)
        return_df = True
        if self.cg_client is None:
            raise ValueError("You must have a cg_client to run live_query")

        if datastack_name is None:
            datastack_name = self.datastack_name

        with TimeIt("find_mat_version"):
            # we want to find the most recent materialization
            # in which the timestamp given is in the future
            mds = self.get_versions_metadata()
            materialization_version = None
            # make sure the materialization's are increasing in ID/time
            for md in sorted(mds, key=lambda x: x["id"]):
                ts = md["time_stamp"]
                if timestamp > ts:
                    materialization_version = md["version"]
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
        with TimeIt("map_filters"):
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

        tables, suffixes = self._resolve_merge_reference(
            merge_reference, table, datastack_name, materialization_version
        )
        with TimeIt("package query"):
            url, data, query_args, encoding = self._format_query_components(
                datastack_name,
                materialization_version,
                tables,
                None,
                suffixes,
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
                True,
                True,
                offset,
                limit,
            )
            logging.debug(f"query_args: {query_args}")
            logging.debug(f"query data: {data}")
        with TimeIt("query materialize"):
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
            self.raise_for_status(response)

        if desired_resolution is None:
            desired_resolution = self.desired_resolution

        with TimeIt("deserialize"):
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                warnings.simplefilter(action="ignore", category=DeprecationWarning)
                df = pa.deserialize(response.content)
                if desired_resolution is not None:
                    df = df.copy()
                    if len(desired_resolution) != 3:
                        raise ValueError(
                            "desired resolution needs to be of length 3, for xyz"
                        )
                    vox_res = self.get_table_metadata(
                        table_name=table,
                        datastack_name=datastack_name,
                        version=materialization_version,
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
            with TimeIt("post_filter"):
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

        return df

    def synapse_query(
        self,
        pre_ids: Union[int, Iterable, np.ndarray] = None,
        post_ids: Union[int, Iterable, np.ndarray] = None,
        bounding_box=None,
        bounding_box_column: str = "post_pt_position",
        timestamp: datetime = None,
        remove_autapses: bool = True,
        include_zeros: bool = True,
        limit: int = None,
        offset: int = None,
        split_positions: bool = False,
        synapse_table: str = None,
        datastack_name: str = None,
        materialization_version: int = None,
        metadata: bool = True,
    ):
        """query synapses

        Args:
            pre_ids (Union[int, Iterable, optional): pre_synaptic cell(s) to query. Defaults to None.
            post_ids (Union[int, Iterable, optional): post synaptic cell(s) to query. Defaults to None.
            timestamp (datetime.datetime, optional): timestamp to query (optional).
                If passed recalculate query at timestamp, do not pass with materialization_verison
            bounding_box: [[min_x, min_y, min_z],[max_x, max_y, max_z]] bounding box to filter
                          synapse locations. Expressed in units of the voxel_resolution of this dataset (optional)
            bounding_box_column (str, optional): which synapse location column to filter by (Default to "post_pt_position")
            remove_autapses (bool, optional): post-hoc filter out synapses. Defaults to True.
            include_zeros (bool, optional): whether to include synapses to/from id=0 (out of segmentation). Defaults to True.
            limit (int, optional): number of synapses to limit, Defaults to None (server side limit applies)
            offset (int, optional): number of synapses to offset query, Defaults to None (no offset).
            split_positions (bool, optional): whether to return positions as seperate x,y,z columns (faster)
                defaults to False
            synapse_table (str, optional): synapse table to query. If None, defaults to self.synapse_table.
            datastack_name: (str, optional): datastack to query
            materialization_version (int, optional): version to query.
                defaults to self.materialization_version if not specified
            metadata: (bool, optional) : toggle to return metadata
                If True (and return_df is also True), return table and query metadata in the df.attr dictionary.

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

    def _assemble_attributes(self, tables, suffixes=None, desired_resolution=None, **kwargs):
        if isinstance(tables, str):
            tables = [tables]

        join_query = len(tables) > 1

        attrs = {
            "datastack_name": self.datastack_name,
        }
        if not join_query:
            attrs["join_query"] = False
            meta = self.get_table_metadata(tables[0])
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
                meta = self.get_table_metadata(tname)
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
                    attrs["dataframe_resolution"] = res[0]
                else:
                    attrs["dataframe_resolution"] = "mixed_resolutions"
            else:
                attrs["dataframe_resolution"] = desired_resolution

        attrs.update(kwargs)
        return attrs


client_mapping = {2: MaterializatonClientV2, "latest": MaterializatonClientV2}
