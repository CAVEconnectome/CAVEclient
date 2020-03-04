import requests
import numpy as np
import time

from emannotationschemas.utils import get_flattened_bsp_keys_from_schema
from emannotationschemas import get_schema

from annotationframeworkclient.endpoints import annotationengine_endpoints as ae
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg
from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice
from .auth import AuthClient

from multiwrapper import multiprocessing_utils as mu


class AnnotationClient(object):
    """Client for interacting with the annotation database

    Parameters
    ----------
    server_address : str or None, optional
        Base URL for the annotation framework service. If None is specified, uses a default server address.

    dataset_name : str or None, optional
        Name of the dataset on the annotation service. If not specified here, required for any individual transaction.

    cg_server_address : str or None, optional
        Chunkedgraph server address, if different from the main one. If None, uses the default server address.

    auth_client : auth.AuthClient or None, optional
        An AuthClient instance with a loaded token for the dataset. If None, does not use a token when interacting with the endpoint, which will not work on services behind any login authorization.
    """

    def __init__(
        self,
        server_address=None,
        dataset_name=None,
        cg_server_address=None,
        auth_client=None,
    ):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address
        self._dataset_name = dataset_name

        self._cg_server_address = cg_server_address

        if auth_client is None:
            auth_client = AuthClient()

        self.session = requests.Session()
        self.session.headers.update(auth_client.request_header)

        self._default_url_mapping = {
            "ae_server_address": self._server_address,
            "cg_server_address": self._cg_server_address,
        }

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @server_address.setter
    def server_address(self, value):
        self._server_address = value
        self._default_url_mapping["ae_server_address"] = value

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @property
    def cg_server_address(self):
        return self._server_address

    @cg_server_address.setter
    def cg_server_address(self, value):
        self._cg_server_address = value
        self._default_url_mapping["cg_server_address"] = value

    def get_datasets(self):
        """ Gets a list of datasets

        Returns
        -------
        list
            List of dataset names for available datasets on the annotation engine
        """
        url = ae["datasets"].format_map(self.default_url_mapping)
        response = self.session.get(url)
        assert response.status_code == 200
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
        url = ae["table_names"].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert response.status_code == 200
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
        url = ae["table_names"].format_map(endpoint_mapping)
        data = {"schema_name": schema_name, "table_name": table_name}

        response = requests.post(url, json=data)
        assert response.status_code == 200
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

        url = ae["existing_annotation"].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert response.status_code == 200
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

        url = ae["new_annotation"].format_map(endpoint_mapping)

        response = self.session.post(url, json=data)
        assert response.status_code == 200
        return response.json()

    def _bulk_import_df_thread(self, args):
        """ bulk_import_df helper """

        data_df_cs, url, n_retries = args

        print("Number of blocks: %d" % (len(data_df_cs)))

        time_start = time.time()
        responses = []
        for i_block, data_df in enumerate(data_df_cs):
            if i_block > 0:
                dt = time.time() - time_start
                eta = dt / i_block * len(data_df_cs) - dt
                eta /= 3600
                print(
                    "%d / %d - dt = %.2fs - eta = %.2fh"
                    % (i_block, len(data_df_cs), dt, eta)
                )

            data_df_json = data_df.to_json()

            response = 0

            for i_try in range(n_retries):
                response = self.session.post(url, json=data_df_json, timeout=300)

                print(i_try, response.status_code)

                if response.status_code == 200:
                    break

            responses.append(response.json)

        return responses

    def bulk_import_df(
        self,
        table_name,
        data_df,
        min_block_size=40,
        dataset_name=None,
        n_threads=16,
        n_retries=100,
    ):
        """ Imports all annotations from a single dataframe in one go, trying to organize points in the same chunk together for faster lookup.

        Parameters
        ----------
        table_name : str
            Target table name for the data

        data_df : pandas.DataFrame
            DataFrame with column names matching the desired schema

        min_block_size : int, optional
            Minimum block size, by default, 40.

        dataset_name : str or None, optional
            Name of the dataset. If None, uses the one specified in the client.

        n_threads : int, optional
            Number of threads to use, by default 16.

        n_retries : int, optional
            Number of retry attempts, by default 100
        Returns
        -------
        json
            Response JSON
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        if endpoint_mapping["cg_server_address"] is None:
            self.cg_server_address = self.infoserviceclient.pychunkgraph_endpoint(
                dataset_name=dataset_name
            )

        if endpoint_mapping["table_id"] is None:
            pcg_seg_endpoint = self.infoserviceclient.pychunkedgraph_viewer_source(
                dataset_name=dataset_name
            )
            pcg_table = pcg_seg_endpoint.split("/")[-1]
            endpoint_mapping["table_id"] = pcg_table

        url = cg["info"].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert response.status_code == 200
        info = response.json()

        chunk_size = np.array(info["scales"][0]["chunk_sizes"][0]) * np.array([1, 1, 4])
        offset = np.array(info["scales"][0]["voxel_offset"])

        Schema = get_schema(table_name)
        schema = Schema()

        rel_column_keys = get_flattened_bsp_keys_from_schema(schema)

        data_df = data_df.reset_index(drop=True)

        bspf_coords = []
        for rel_column_key in rel_column_keys:
            bspf_coords.append(
                np.array(data_df[rel_column_key].values.tolist())[:, None, :]
            )

        bspf_coords = np.concatenate(bspf_coords, axis=1)
        bspf_coords -= offset
        bspf_coords = (bspf_coords / chunk_size).astype(np.int)

        if len(rel_column_keys) > 1:
            f_shape = np.array(list(bspf_coords.shape))[[0, 2]]
            bspf_coords_f = np.zeros(f_shape, dtype=np.int)

            selector = np.argmin(bspf_coords, axis=1)[:, 2]

            for i_k in range(len(rel_column_keys)):
                m = selector == i_k
                bspf_coords_f[m] = bspf_coords[m][:, i_k]
        else:
            bspf_coords_f = bspf_coords[:, 0]

        bspf_coords = None

        ind = np.lexsort(
            (bspf_coords_f[:, 0], bspf_coords_f[:, 1], bspf_coords_f[:, 2])
        )

        # bspf_coords = None

        data_df = data_df.reindex(ind)
        bspf_coords_f = bspf_coords_f[ind]

        data_df_chunks = []
        range_start = 0
        range_end = 1

        for i_row in range(1, len(data_df)):
            if np.sum(bspf_coords_f[i_row] - bspf_coords_f[i_row - 1]) == 0:
                range_end += 1
            elif (range_end - range_start) < min_block_size:
                range_end += 1
            else:
                data_df_chunks.append(data_df[range_start:range_end])
                range_start = i_row
                range_end = i_row + 1

        data_df_chunks.append(data_df[range_start:range_end])

        url = "{}/annotation/dataset/{}/{}?bulk=true".format(
            self.server_address, dataset_name, table_name
        )

        print(url)

        multi_args = []
        arg = []
        for data_df_chunk in data_df_chunks:
            arg.append(data_df_chunk)

            if len(arg) > 1000:
                multi_args.append([arg, url, n_retries])
                arg = []

        if len(arg) > 0:
            multi_args.append([arg, url, n_retries])

        print(len(multi_args))

        results = mu.multithread_func(
            self._bulk_import_df_thread, multi_args, n_threads=n_threads
        )

        responses = []
        for result in results:
            responses.extend(result)

        return responses
