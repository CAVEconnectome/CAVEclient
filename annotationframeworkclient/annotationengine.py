import requests
import os
import numpy as np
import cloudvolume
import json
import time
#
from emannotationschemas.utils import get_flattened_bsp_keys_from_schema
from emannotationschemas import get_schema

from annotationframeworkclient.endpoints import annotationengine_endpoints as ae
from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice


class AnnotationClient(object):
    def __init__(self, server_address=None, dataset_name=None):
        """

        :param server_address: str or None
            server url
        :param dataset_name: str or None
            dataset name
        """
        if server_address is None:
            server_address = os.environ.get('ANNOTATION_ENGINE_ENDPOINT', None)

        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address
        self._dataset_name = dataset_name
        self.session = requests.Session()

        self._default_url_mapping = {"server_address": self._server_address}
        self._infoserviceclient = None

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    @property
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

    def get_annotation_types(self, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        url = ae["annotation_types"].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code==200)
        return response.json()

    def get_dataset_info(self, dataset_name=None):
        """ Returns information about a dataset

        Calls get_dataset_info from informationserviceclient

        :param dataset_name: str
        :return: dict
        """

        return self.infoserviceclient.get_dataset_info(dataset_name=dataset_name)

    def get_annotation(self, annotation_type, annotation_id, dataset_name=None):
        """ Returns information about one specific annotation

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: np.uint64
        :return dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["annotation_type"] = annotation_type
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def post_annotation(self, annotation_type, data, dataset_name=None):
        """ Post an annotation to the AnnotationEngine

        :param dataset_name: str
        :param annotation_type: str
        :param data: dict
        :return dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["annotation_type"] = annotation_type

        url = ae["new_annotation"].format_map(endpoint_mapping)

        response = self.session.post(url, json=data)
        assert(response.status_code == 200)
        return response.json()

    def update_annotation(self, annotation_type, annotation_id, data,
                          dataset_name=None):
        """ Updates an existing annotation

        :param annotation_type: str
        :param annotation_id: np.uint64
        :param data: dict
        :param dataset_name: str
        :return: dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["annotation_type"] = annotation_type
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)

        response = self.session.put(url, json=data)
        assert(response.status_code == 200)
        return response.json()

    def delete_annotation(self, annotation_type, annotation_id,
                          dataset_name=None):
        """ Delete an existing annotation

        :param dataset_name: str
        :param annotation_type: str
        :param annotation_id: int
        :return dict
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["dataset_name"] = dataset_name
        endpoint_mapping["annotation_type"] = annotation_type
        endpoint_mapping["annotation_id"] = annotation_id

        url = ae["existing_annotation"].format_map(endpoint_mapping)

        response = self.session.delete(url)
        assert(response.status_code == 200)
        return response.json()

    def bulk_import_df(self, annotation_type, data_df,
                       block_size=10000, dataset_name=None):
        """ Imports all annotations from a single dataframe in one go

        :param dataset_name: str
        :param annotation_type: str
        :param data_df: pandas DataFrame
        :return:
        """
        if dataset_name is None:
            dataset_name = self.dataset_name

        dataset_info = self.get_dataset_info(dataset_name)
        cv = cloudvolume.CloudVolume(dataset_info["pychunkgraph_segmentation_source"])
        chunk_size = np.array(cv.info["scales"][0]["chunk_sizes"][0]) * 8
        bounds = np.array(cv.bounds.to_list()).reshape(2, 3)

        Schema = get_schema(annotation_type)
        schema = Schema()

        rel_column_keys = get_flattened_bsp_keys_from_schema(schema)

        data_df = data_df.reset_index(drop=True)

        bspf_coords = []
        for rel_column_key in rel_column_keys:
            bspf_coords.append(
                np.array(data_df[rel_column_key].values.tolist())[:, None, :])

        bspf_coords = np.concatenate(bspf_coords, axis=1)
        bspf_coords -= bounds[0]
        bspf_coords = (bspf_coords / chunk_size).astype(np.int)

        bspf_coords = bspf_coords[:, 0]
        ind = np.lexsort(
            (bspf_coords[:, 0], bspf_coords[:, 1], bspf_coords[:, 2]))

        data_df = data_df.reindex(ind)

        url = "{}/annotation/dataset/{}/{}?bulk=true".format(self.server_address,
                                                             dataset_name,
                                                             annotation_type)
        n_blocks = int(np.ceil(len(data_df) / block_size))

        print("Number of blocks: %d" % n_blocks)
        time_start = time.time()

        raise()

        responses = []
        for i_block in range(0, len(data_df), block_size):
            if i_block > 0:
                dt = time.time() - time_start
                eta = dt / i_block * len(data_df) - dt
                print("%d / %d - dt = %.2fs - eta = %.2fs" %
                      (i_block, len(data_df), dt, eta))

            data_block = data_df[i_block: i_block + block_size].to_json()
            response = self.session.post(url, json=data_block, verify=False)
            assert(response.status_code == 200)
            responses.append(response.json)

        return responses


    def lookup_supervoxel(self, xyz, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name
        
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        endpoint_mapping['x'] = int(xyz[0])
        endpoint_mapping['y'] = int(xyz[1])
        endpoint_mapping['z'] = int(xyz[2])

        url = ae['lookup_supervoxel'].format_map(endpoint_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()


    # def get_annotations_of_root_id(self, annotation_type, root_id, dataset_name=None):
    #     if dataset_name == None:
    #         dataset_name = self.dataset_name

    #     endpoint_mapping = self.default_url_mapping
    #     endpoint_mapping['dataset_name'] = dataset_name
    #     endpoint_mapping['annotation_type'] = annotation_type
    #     endpoint_mapping['root_id'] = root_id

    #     url = ae['existing_segment_annotation'].format_map(endpoint_mapping)
    #     response = self.session.get(url)
    #     assert(response.status_code == 200)
    #     return response.json()
        
