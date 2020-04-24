from .base import ClientBase, _api_verisons, _api_endpoints
from .auth import AuthClient
from .endpoints import annotation_common, annotation_api_versions
import requests
import time

server_key = "ae_server_address"


def AnnotationClient(server_address=None,
                     dataset_name=None,
                     auth_client=None,
                     api_version='latest'):
    "Client factory for annotation engine"

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header

    endpoints = _api_endpoints(api_version, server_key, server_address,
                               annotation_common, annotation_api_versions)
    AnnoClient = client_mapping[api_version]
    return AnnoClient(server_address, auth_header, api_version, endpoints, server_name, dataset_name)


class AnnotationClientLegacy(ClientBase):
    def __init__(self, server_address, auth_header, api_version, endpoints, server_name, dataset_name):
        super(AnnotationClient, self).__init__(server_address,
                                               auth_header, api_version, endpoints, server_name):
        self._dataset_name = dataset_name

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def default_url_mapping(self):
        return self._default_url_mapping

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


client_mapping = {0: AnnotationClientLegacy}
