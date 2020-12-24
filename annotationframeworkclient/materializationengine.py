from .base import ClientBaseWithDataset, ClientBaseWithDatastack, ClientBase, _api_versions, _api_endpoints
from .auth import AuthClient
from .endpoints import materialization_api_versions, materialization_common
from .infoservice import InfoServiceClientV2
import requests
import time
import json
import numpy as np
from datetime import date, datetime
import pyarrow as pa
SERVER_KEY = "me_server_address"


class MEEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def MaterializationClient(server_address,
                          datastack_name=None,
                          auth_client=None,
                          api_version='latest',
                          version=None,
                          verify=True):
    """ Factory for returning AnnotationClient
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
    version : default version to query
        if None will default to latest version
    Returns
    -------
    ClientBaseWithDatastack
        List of datastack names for available datastacks on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(api_version, SERVER_KEY, server_address,
                                            materialization_common, materialization_api_versions, auth_header)

    MatClient = client_mapping[api_version]
    return MatClient(server_address, auth_header, api_version,
                     endpoints, SERVER_KEY, datastack_name,
                     version=version, verify=verify)


class MaterializatonClientV2(ClientBase):
    def __init__(self, server_address, auth_header, api_version,
                 endpoints, server_name, datastack_name, version=None,
                 verify=True):
        super(MaterializatonClientV2, self).__init__(server_address,
                                                     auth_header, api_version, endpoints, server_name)

        self._datastack_name = datastack_name
        self._verify = verify
        if version is None:
            version = self.most_recent_version()
        self._version = version

    @property
    def datastack_name(self):
        return self._datastack_name

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, x):
        if int(x) in self.get_versions():
            self._version = int(x)
        else:
            raise ValueError('Version not in materialized database')

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

    def get_versions(self,  datastack_name=None):
        """get versions available

        Args:
            datastack_name ([type], optional): [description]. Defaults to None.
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        url = self._endpoints["versions"].format_map(endpoint_mapping)
        response = self.session.get(url, verify=self._verify)
        self.raise_for_status(response)
        return response.json()

    def get_tables(self, datastack_name=None, version=None):
        """ Gets a list of table names for a datastack

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

        response = self.session.get(url, verify=self._verify)
        self.raise_for_status(response)
        return response.json()

    def get_annotation_count(self, table_name: str,
                             datastack_name=None,
                             version=None):
        """ Get number of annotations in a table

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

        response = self.session.get(url, verify=self._verify)
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
        response = self.session.get(url, verify=self._verify)
        self.raise_for_status(response)
        return response.json()

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
        meta = self.get_version_metadata(
            version=version, datastack_name=datastack_name)
        return datetime.strptime(meta['time_stamp'], '%Y-%m-%dT%H:%M:%S.%f')

    def get_table_metadata(self, table_name: str, datastack_name=None):
        """ Get metadata about a table

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

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["metadata"].format_map(endpoint_mapping)

        response = self.session.get(url, verify=self._verify)
        self.raise_for_status(response)
        return response.json()

    # def get_annotation(self, table_name, annotation_ids,
    #                    materialization_version=None,
    #                    datastack_name=None):
    #     """ Retrieve an annotation or annotations by id(s) and table name.

    #     Parameters
    #     ----------
    #     table_name : str
    #         Name of the table
    #     annotation_ids : int or iterable
    #         ID or IDS of the annotation to retreive
    #     materialization_version: int or None
    #         materialization version to use
    #         If None, uses the one specified in the client
    #     datastack_name : str or None, optional
    #         Name of the datastack_name.
    #         If None, uses the one specified in the client.
    #     Returns
    #     -------
    #     list
    #         Annotation data
    #     """
    #     if materialization_version is None:
    #         materialization_version = self.version
    #     if datastack_name is None:
    #         datastack_name = self.datastack_name

    #     endpoint_mapping = self.default_url_mapping
    #     endpoint_mapping["datastack_name"] = datastack_name
    #     endpoint_mapping["table_name"] = table_name
    #     endpoint_mapping["version"] = materialization_version
    #     url = self._endpoints["annotations"].format_map(endpoint_mapping)
    #     try:
    #         iter(annotation_ids)
    #     except TypeError:
    #         annotation_ids = [annotation_ids]

    #     params = {
    #         'annotation_ids': ",".join([str(a) for a in annotation_ids])
    #     }
    #     response = self.session.get(url, params=params)
    #     self.raise_for_status(response)
    #     return response.json()

    def query_table(self,
                    table: str,
                    filter_in_dict=None,
                    filter_out_dict=None,
                    filter_equal_dict=None,
                    filter_spatial=None,
                    join_args=None,
                    select_columns=None,
                    offset: int = None,
                    limit: int = None,
                    datastack_name: str = None,
                    materialization_version: int = None):
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
            offset (int, optional): offset in query result
            limit (int, optional): maximum results to return (server will set upper limit, see get_server_config)
            select_columns (list of str, optional): columns to select. Defaults to None.
            suffixes: (list[str], optional): suffixes to use on duplicate columns
            offset (int, optional): result offset to use. Defaults to None.
                will only return top K results. 
            datastack_name (str, optional): datastack to query. 
                If None defaults to one specified in client. 
            materialization_version (int, optional): version to query. 
                If None defaults to one specified in client.
        Returns:
        pd.DataFrame: a pandas dataframe of results of query

        """
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = materialization_version
        data = {}
        query_args = {}
        if type(table) == str:
            tables = [table]
        if len(tables) == 1:
            assert(type(tables[0]) == str)
            endpoint_mapping["table_name"] = tables[0]
            single_table = True
            url = self._endpoints["simple_query"].format_map(endpoint_mapping)
        else:
            single_table = False
            data['tables'] = tables
            url = self._endpoints["join_query"].format_map(endpoint_mapping)

        if filter_in_dict is not None:
            data['filter_in_dict'] = {table: filter_in_dict}
        if filter_out_dict is not None:
            data['filter_notin_dict'] = {table: filter_out_dict}
        if filter_equal_dict is not None:
            data['filter_equal_dict'] = {table: filter_equal_dict}
        if select_columns is not None:
            data['select_columns'] = select_columns
        if offset is not None:
            data['offset'] = offset
        if limit is not None:
            assert(limit > 0)
            data['limit'] = limit
        response = self.session.post(url, data=json.dumps(data, cls=MEEncoder),
                                     headers={
                                         'Content-Type': 'application/json'},
                                     verify=self._verify)
        self.raise_for_status(response)
        return pa.deserialize(response.content)

    def join_query(self,
                   tables,
                   filter_in_dict=None,
                   filter_out_dict=None,
                   filter_equal_dict=None,
                   filter_spatial=None,
                   join_args=None,
                   select_columns=None,
                   offset: int = None,
                   limit: int = None,
                   suffixes: list = None,
                   datastack_name: str = None,
                   materialization_version: int = None):
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
            select_columns (list of str, optional): columns to select. Defaults to None.
            offset (int, optional): result offset to use. Defaults to None.
                will only return top K results. 
            limit (int, optional): maximum results to return (server will set upper limit, see get_server_config)
            suffixes (list[str], optional): suffixes to use for duplicate columns same order as tables 
            datastack_name (str, optional): datastack to query. 
                If None defaults to one specified in client. 
            materialization_version (int, optional): version to query. 
                If None defaults to one specified in client.
        Returns:
        pd.DataFrame: a pandas dataframe of results of query

        """
        if materialization_version is None:
            materialization_version = self.version
        if datastack_name is None:
            datastack_name = self.datastack_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["datastack_name"] = datastack_name
        endpoint_mapping["version"] = materialization_version
        data = {}
        query_args = {}

        data['tables'] = tables
        url = self._endpoints["join_query"].format_map(endpoint_mapping)

        if filter_in_dict is not None:
            data['filter_in_dict'] = filter_in_dict
        if filter_out_dict is not None:
            data['filter_notin_dict'] = filter_out_dict
        if filter_equal_dict is not None:
            data['filter_equal_dict'] = filter_equal_dict
        if select_columns is not None:
            data['select_columns'] = select_columns
        if offset is not None:
            data['offset'] = offset
        if suffixes is not None:
            data['suffixes'] = suffixes
        if limit is not None:
            assert(limit > 0)
            data['limit'] = limit
        response = self.session.post(url, data=json.dumps(data, cls=MEEncoder),
                                     headers={
                                         'Content-Type': 'application/json'},
                                     verify=self._verify)
        self.raise_for_status(response)
        return pa.deserialize(response.content)


client_mapping = {2: MaterializatonClientV2,
                  'latest': MaterializatonClientV2}
