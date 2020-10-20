from .base import ClientBaseWithDataset, ClientBaseWithDatastack, ClientBase, _api_versions, _api_endpoints, handle_response
from .auth import AuthClient
from .endpoints import annotation_common, annotation_api_versions
from .infoservice import InfoServiceClientV2
import requests
import time
import json
import numpy as np
from datetime import date, datetime

SERVER_KEY = "ae_server_address"


class AEEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def AnnotationClient(server_address,
                     dataset_name=None,
                     aligned_volume_name=None,
                     auth_client=None,
                     api_version='latest'):
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

    Returns
    -------
    ClientBaseWithDatastack
        List of datastack names for available datastacks on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(api_version, SERVER_KEY, server_address,
                                            annotation_common, annotation_api_versions, auth_header)

    AnnoClient = client_mapping[api_version]
    if api_version > 1:
        return AnnoClient(server_address, auth_header, api_version,
                          endpoints, SERVER_KEY, aligned_volume_name)
    else:
        return AnnoClient(server_address, auth_header, api_version,
                          endpoints, SERVER_KEY, dataset_name)


class AnnotationClientLegacy(ClientBaseWithDataset):
    def __init__(self, server_address, auth_header, api_version, endpoints, server_name, dataset_name):
        super(AnnotationClientLegacy, self).__init__(server_address,
                                                     auth_header, api_version, endpoints,
                                                     server_name, dataset_name)

    def get_datasets(self):
        """ Gets a list of datasets

        Returns
        -------
        list
            List of dataset names for available datasets on the annotation engine
        """
        url = self._endpoints["datasets"].format_map(self.default_url_mapping)
        response = self.session.get(url)
        return handle_response(response)

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
        return handle_response(response)

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
        return handle_response(response)

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

        url = self._endpoints["existing_annotation"].format_map(
            endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

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

        response = self.session.post(url, data=json.dumps(data, cls=AEEncoder),
                                     headers={'Content-Type': 'application/json'})
        return handle_response(response)


class AnnotationClientV2(ClientBase):
    def __init__(self, server_address, auth_header, api_version,
                 endpoints, server_name, aligned_volume_name):
        super(AnnotationClientV2, self).__init__(server_address,
                                                 auth_header, api_version, endpoints, server_name)

        self._aligned_volume_name = aligned_volume_name

    @property
    def aligned_volume_name(self):
        return self._aligned_volume_name

    def get_tables(self, aligned_volume_name=None):
        """ Gets a list of table names for a aligned_volume_name

        Parameters
        ----------
        aligned_volume_name : str or None, optional
            Name of the aligned_volume, by default None.
            If None, uses the one specified in the client.
            Will be set correctly if you are using the framework_client

        Returns
        -------
        list
            List of table names
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        url = self._endpoints["tables"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def get_annotation_count(self, table_name: str, aligned_volume_name=None):
        """ Get number of annotations in a table

        Parameters
        ----------
        table_name (str): 
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        int
            number of annotations
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_count"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def get_table_metadata(self, table_name: str, aligned_volume_name=None):
        """ Get metadata about a table

        Parameters
        ----------
        table_name (str): 
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        json
            metadata about table
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_info"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def delete_table(self, table_name: str, aligned_volume_name=None):
        """ Marks a table for deletion
        requires super admin priviledges

        Parameters
        ----------
        table_name (str): 
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        json
            Response JSON
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_info"].format_map(endpoint_mapping)

        response = self.session.delete(url)
        return handle_response(response)

    def create_table(self, table_name, schema_name,
                     description, reference_table=None,
                     flat_segmentation_source=None,
                     user_id=None,
                     aligned_volume_name=None):
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
        flat_segmentation_source: str or None
            the source to a flat segmentation that corresponds to this table
            i.e. precomputed:\\gs:\\mybucket\this_tables_annotation
        user_id: int
            If you are uploading this schema on someone else's behalf
            and you want to link this table with their ID, you can specify it here
            Otherwise, the table will be created with your userID in the user_id column.
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name

        url = self._endpoints["tables"].format_map(endpoint_mapping)
        metadata = {'description': description}
        if user_id is not None:
            metadata['user_id'] = user_id
        if reference_table is not None:
            metadata['reference_table'] = reference_table
        if flat_segmentation_source is not None:
            metadata['flat_segmentation_source'] = flat_segmentation_source
        data = {"schema_type": schema_name,
                "table_name": table_name,
                "metadata": metadata}

        response = self.session.post(url, json=data)
        return handle_response(response)

    def get_annotation(self, table_name, annotation_ids, aligned_volume_name=None):
        """ Retrieve an annotation or annotations by id(s) and table name.

        Parameters
        ----------
        table_name : str
            Name of the table
        annotation_ids : int or iterable
            ID or IDS of the annotation to retreive
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        list
            Annotation data
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        params = {
            'annotation_ids': ",".join([str(a) for a in annotation_ids])
        }
        response = self.session.get(url, params=params)
        return handle_response(response)

    def post_annotation(self, table_name, data, aligned_volume_name=None):
        """ Post one or more new annotations to a table in the AnnotationEngine

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)

        try:
            iter(data)
        except TypeError:
            annotation_ids = [data]

        data = {
            "annotations": data
        }

        response = self.session.post(url, data=json.dumps(data, cls=AEEncoder),
                                     headers={'Content-Type': 'application/json'})
        return handle_response(response)

    def update_annotation(self, table_name, data, aligned_volume_name=None):
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
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
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
        return handle_response(response)

    def delete_annotation(self, table_name, annotation_ids, aligned_volume_name=None):
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
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)

        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        data = {
            "annotation_ids": annotation_ids
        }

        response = self.session.delete(url, data=json.dumps(data, cls=AEEncoder),
                                       headers={'Content-Type': 'application/json'})
        return handle_response(response)


client_mapping = {0: AnnotationClientLegacy,
                  2: AnnotationClientV2,
                  'latest': AnnotationClientV2}
