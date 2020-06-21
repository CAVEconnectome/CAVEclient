from .base import ClientBaseWithDataset, _api_verisons, _api_endpoints
from .auth import AuthClient
from .endpoints import annotation_common, annotation_api_versions
import requests
import time

server_key = "ae_server_address"


def AnnotationClient(server_address,
                     dataset_name,
                     auth_client=None,
                     api_version='latest'):
    """ Factory for returning AnnotationClient
    Parameters
    ----------
    server_address : str 
        server_address to use to connect to (i.e. https://minniev1.microns-daf.com)
    dataset_name : str
        Name of the dataset, by default None. If None, uses the one specified in the client.
    auth_client : AuthClient or None, optional
        Authentication client to use to connect to server. If None, do not use authentication.
    api_version : str or int (default: latest)
        What version of the api to use, 0: Legacy client (i.e www.dynamicannotationframework.com) 
        2: new api version, (i.e. minniev1.microns-daf.com)
        'latest': default to the most recent (current 2)

    Returns
    -------
    ClientBaseWithDataset
        List of dataset names for available datasets on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header

    endpoints, api_version = _api_endpoints(api_version, server_key, server_address,
                                            annotation_common, annotation_api_versions)
    AnnoClient = client_mapping[api_version]
    return AnnoClient(server_address, auth_header, api_version, endpoints, server_name, dataset_name)


class AnnotationClientLegacy(ClientBaseWithDataset):
    def __init__(self, server_address, auth_header, api_version, endpoints, server_name, dataset_name):
        super(AnnotationClient, self).__init__(server_address,
                                               auth_header, api_version, endpoints, server_name, dataset_name):

    def get_datasets(self):
        """ Gets a list of datasets

        Returns
        -------
        list
            List of dataset names for available datasets on the annotation engine
        """
        url = self._endpoints["datasets"].format_map(self.default_url_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_tables(self, dataset_name=None):
        """ Gets a list of table names for a dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset, by default None. If None, uses the one specified in the client.

        Returns
        -------
        list
            List of table names
        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        url = self._endpoints["table_names"].format_map(endpoint_mapping)

        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def create_table(self, table_name, schema_name, dataset_name=None):
        """ Creates a new data table based on an existing schema

        Parameters
        ----------
        table_name: str
            Name of the new table. Cannot be the same as an existing table
        schema_name: str
            Name of the schema for the new table.
        dataset_name: str or None, optional,
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        url = self._endpoints["table_names"].format_map(endpoint_mapping)
        data = {"schema_name": schema_name, "table_name": table_name}

        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def get_annotation(self, table_name, annotation_id, dataset_name=None):
        """ Retrieve a single annotation by id and table name.

        Parameters
        ----------
        table_name : str
            Name of the table
        annotation_id : int
            ID number of the annotation to retreive (starting from 1)
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        dict
            Annotation data
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["annotation_id"] = annotation_id

        url = self._endpoints["existing_annotation"].format_map(endpoint_mapping)
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def post_annotation(self, table_name, data, dataset_name=None):
        """ Post one or more new annotations to a table in the AnnotationEngine

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["new_annotation"].format_map(endpoint_mapping)

        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()


class AnnotationClientV2(ClientBaseWithDataset):
    def __init__(self, server_address, auth_header, api_version, endpoints, server_name, dataset_name):
        super(AnnotationClient, self).__init__(server_address,
                                               auth_header, api_version, endpoints, server_name, dataset_name):

    def get_tables(self, dataset_name=None):
        """ Gets a list of table names for a dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset, by default None. If None, uses the one specified in the client.

        Returns
        -------
        list
            List of table names
        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        url = self._endpoints["tables"].format_map(endpoint_mapping)

        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def create_table(self, table_name, schema_name, 
        description, reference_table=None,
        user_id=None,
        dataset_name=None):
        """ Creates a new data table based on an existing schema

        Parameters
        ----------
        table_name: str
            Name of the new table. Cannot be the same as an existing table
        schema_name: str
            Name of the schema for the new table.
        descrption: str
            Human readable description for what is in the table.
            Should include information about who generated the table
            What data it covers, and how it should be interpreted.
            And who should you talk to if you want to use it.
            An Example:
            a manual synapse table to detect chandelier synapses
            on 81 PyC cells with complete AISs 
            [created by Agnes - agnesb@alleninstitute.org, uploaded by Forrest]
        reference_table: str or None
            If the schema you are using is a reference schema
            Meaning it is an annotation of another annotation.
            Then you need to specify what table those annotations are in.
        user_id: int
            If you are uploading this schema on someone else's behalf
            and you want to link this table with their ID, you can specify it here
            Otherwise, the table will be created with your userID in the user_id column.
        dataset_name: str or None, optional,
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name

        url = self._endpoints["tables"].format_map(endpoint_mapping)
        metadata={'description: description'}
        if user_id is not None:
            metadata['user_id']=user_id
        if reference_table is not None:
            metadata['reference_table']=reference_table

        data = {"schema_name": schema_name,
                "table_name": table_name,
                "metadata": metadata}
                  
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def get_annotation(self, table_name, annotation_ids, dataset_name=None):
        """ Retrieve an annotation or annotations by id(s) and table name.

        Parameters
        ----------
        table_name : str
            Name of the table
        annotation_ids : int or iterable
            ID or IDS of the annotation to retreive
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        list
            Annotation data
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        params = {
            'annotations': annotation_ids
        }
        response = self.session.get(url, params=query_d)
        response.raise_for_status()
        return response.json()

    def post_annotation(self, table_name, data, dataset_name=None):
        """ Post one or more new annotations to a table in the AnnotationEngine

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        
        try:
            iter(data)
        except TypeError:
            annotation_ids = [data]

        data = {
            "annotations": data
        }

        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def update_annotation(self, table_name, data, dataset_name=None):
        """Update one or more new annotations to a table in the AnnotationEngine
        Note update is implemented by deleting the old annotation
        and inserting a new annotation, which will receive a new ID.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
            each dict must contain an "id" field which is the ID of the annotation to update
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        
        try:
            iter(data)
        except TypeError:
            annotation_ids = [data]

        data = {
            "annotations": data
        }

        response = self.session.put(url, json=data)
        response.raise_for_status()
        return response.json()

    def delete_annotation(self, table_name, annotation_ids, dataset_name=None):
        """Update one or more new annotations to a table in the AnnotationEngine
        Note update is implemented by deleting the old annotation
        and inserting a new annotation, which will receive a new ID.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
            each dict must contain an "id" field which is the ID of the annotation to update
        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        
        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        data = {
            "annotation_ids": annotation_ids
        }

        response = self.session.delete(url, json=data)
        response.raise_for_status()
        return response.json()
client_mapping = {0: AnnotationClientLegacy,
                  2: AnnotationClientV2,
                  'latest': AnnotationClientV2}
