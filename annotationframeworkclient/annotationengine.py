import requests
import numpy as np
import time

from emannotationschemas.utils import get_flattened_bsp_keys_from_schema
from emannotationschemas import get_schema

from annotationframeworkclient.endpoints import annotationengine_endpoints as ae
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg
from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice

from multiwrapper import multiprocessing_utils as mu


class AnnotationClient(object):
    def __init__(self, server_address=None, dataset_name=None, cg_server_address=None):
        """
        :param server_address: str or None
            server url
        :param dataset_name: str or None
            dataset name
        """

        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address
        self._dataset_name = dataset_name

        self._infoserviceclient = None
        self._cg_server_address = cg_server_address 

        self.session = requests.Session()
        self._default_url_mapping = {"ae_server_address": self._server_address,
                                     'cg_server_address': self._cg_server_address}
        
    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @server_address.setter
    def server_address(self, value):
        self._server_address = value
        self._default_url_mapping['ae_server_address'] = value


    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()


    @property
    def cg_server_address(self):        
        return self._server_address

    @cg_server_address.setter
    def cg_server_address(self, value):
        self._cg_server_address = value
        self._default_url_mapping['cg_server_address'] = value

    def infoserviceclient(self):
        if self._infoserviceclient is None:
            self._infoserviceclient = infoservice.InfoServiceClient(server_address=self.server_address, dataset_name=self.dataset_name)

        return self._infoserviceclient

    def get_datasets(self):
        """ Returns existing datasets

        :return: list
        """
        url = ae["datasets"].format_map(self.default_url_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def get_tables(self, dataset_name=None):
        """ Reads table infos

        :param dataset_name: str
        :return: dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        url = ae["table_names"].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code==200)
        return response.json()

    def create_table(self, table_name, schema_name, dataset_name=None):
        """ Creates a table

        :param table_name: str
        :param schema_name: str
        :param dataset_name: str
        :return: response
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        url = ae["table_names"].format_map(endpoint_mapping)
        data = {'schema_name': schema_name,
                'table_name': table_name}

        response = requests.post(url, json=data)
        assert(response.status_code==200)
        return response.json()

    def get_dataset_info(self, dataset_name=None):
        """ Returns information about a dataset

        Calls get_dataset_info from informationserviceclient

        :param dataset_name: str
        :return: dict
        """

        return self.infoserviceclient.get_dataset_info(dataset_name=dataset_name)

    def get_annotation(self, table_name, annotation_id, dataset_name=None):
        """ Returns information about one specific annotation

        :param dataset_name: str
        :param table_name: str
        :param annotation_id: np.uint64
        :return dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def post_annotation(self, table_name, data, dataset_name=None):
        """ Post an annotation to the AnnotationEngine

        :param dataset_name: str
        :param table_name: str
        :param data: dict
        :return dict
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
        assert(response.status_code == 200)
        return response.json()

    def update_annotation(self, table_name, annotation_id, data,
                          dataset_name=None):
        """ Updates an existing annotation

        :param table_name: str
        :param annotation_id: np.uint64
        :param data: dict
        :param dataset_name: str
        :return: dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)

        response = self.session.put(url, json=data)
        assert(response.status_code == 200)
        return response.json()

    def delete_annotation(self, table_name, annotation_id,
                          dataset_name=None):
        """ Delete an existing annotation

        :param dataset_name: str
        :param table_name: str
        :param annotation_id: int
        :return dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["table_name"] = table_name
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)

        response = self.session.delete(url)
        assert(response.status_code == 200)
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
                print("%d / %d - dt = %.2fs - eta = %.2fh" %
                      (i_block, len(data_df_cs), dt, eta))

            data_df_json = data_df.to_json()

            response = 0

            for i_try in range(n_retries):
                response = self.session.post(url, json=data_df_json, timeout=300)

                print(i_try, response.status_code)

                if response.status_code == 200:
                    break

            responses.append(response.json)

        return responses

    def bulk_import_df(self, table_name, data_df,
                       min_block_size=40, dataset_name=None,
                       n_threads=16, n_retries=100):
        """ Imports all annotations from a single dataframe in one go

        :param dataset_name: str
        :param table_name: str
        :param data_df: pandas DataFrame
        :return:
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        dataset_info = self.get_dataset_info(dataset_name)

        endpoint_mapping = self.default_url_mapping
        if endpoint_mapping['cg_server_address'] is None:
            self.cg_server_address = self.infoserviceclient.pychunkgraph_endpoint(dataset_name=dataset_name)
        
        if endpoint_mapping['table_id'] is None:
            pcg_seg_endpoint = self.infoserviceclient.pychunkedgraph_viewer_source(dataset_name=dataset_name)
            pcg_table = pcg_seg_endpoint.split('/')[-1]
            endpoint_mapping['table_id'] = pcg_table

        url = cg['info'].format_map(endpoint_mapping)
        
        response = self.session.get(url)
        assert(response.status_code == 200)
        info = response.json()

        chunk_size = np.array(info["scales"][0]["chunk_sizes"][0]) * np.array([1, 1, 4])
        offset = np.array(info['scales'][0]['voxel_offset'])

        Schema = get_schema(table_name)
        schema = Schema()

        rel_column_keys = get_flattened_bsp_keys_from_schema(schema)

        data_df = data_df.reset_index(drop=True)

        bspf_coords = []
        for rel_column_key in rel_column_keys:
            bspf_coords.append(
                np.array(data_df[rel_column_key].values.tolist())[:, None, :])

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

        ind = np.lexsort((bspf_coords_f[:, 0], bspf_coords_f[:, 1], bspf_coords_f[:, 2]))

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
                data_df_chunks.append(data_df[range_start: range_end])
                range_start = i_row
                range_end = i_row + 1

        data_df_chunks.append(data_df[range_start: range_end])

        url = "{}/annotation/dataset/{}/{}?bulk=true".format(self.server_address,
                                                             dataset_name,
                                                             table_name)

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

        results = mu.multithread_func(self._bulk_import_df_thread, multi_args,
                                      n_threads=n_threads)

        responses = []
        for result in results:
            responses.extend(result)


        return responses

        # return data_df_chunks

    def get_supervoxel(self, xyz, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name
        
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        endpoint_mapping['x'] = int(xyz[0])
        endpoint_mapping['y'] = int(xyz[1])
        endpoint_mapping['z'] = int(xyz[2])

        url = ae['supervoxel'].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def get_root_id(self, supervoxel_id, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        if endpoint_mapping['cg_server_address'] is None:
            self.cg_server_address = self.infoserviceclient.pychunkgraph_endpoint(dataset_name=dataset_name)

        url = cg['handle_root'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[supervoxel_id])
        assert(response.status_code == 200)
        return np.squeeze(np.frombuffer(response.content, dtype=np.uint64)).tolist()

    def get_root_id_under_point(self, xyz, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name

        svid = self.get_supervoxel(xyz, dataset_name=dataset_name)
        return self.get_root_id(svid)