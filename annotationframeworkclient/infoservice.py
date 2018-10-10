import requests
from .endpoints import infoservice_endpoints as ie

class InfoServiceClient(object):
    def __init__(self, server_address, dataset_name=None):
        self._server_address = server_address
        self._dataset_name = dataset_name
        self.session = requests.Session()
        self.info_cache = dict()

        self._default_url_mapping = {"server_address": self._server_address}

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @property
    def default_url_mapping(self):
        return self._default_url_mapping

    def get_datasets(self):
        endpoint_mapping = self.default_url_mapping
        url = ie['datasets'].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def get_dataset_info(self, dataset_name=None, use_stored=True):
        if dataset_name is None:
            dataset_name = self.dataset_name
        assert(dataset_name is not None)
        
        if (not use_stored) or (dataset not in self.cached_data):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping['dataset_name'] = dataset_name
            url = ie['dataset_info'].format_map(endpoint_mapping)
        
            response = self.session.get(url)
            assert(response.status_code == 200)
        
            self.info_cache[dataset_name] = response.json()
        
        return self.info_cache.get(dataset_name, None)

    def get_property(self, info_property, dataset_name=None, use_stored=True):
        if dataset_name is None:
            dataset_name=self.dataset_name
        assert(dataset_name is not None)

        self.get_dataset_info(dataset_name=dataset_name, use_stored=use_stored)
        return self.info_cache[dataset_name].get(info_property, None)

    def annotation_endpoint(self, dataset_name=None, use_stored=True):
        return self.get_property('annotation_engine_endpoint',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def annotation_dataset_name(self, dataset_name=None, use_stored=True):
        return self.get_property('annotation_dataset_name',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def flat_segmentation_source(self, dataset_name=None, use_stored=True):
        return self.get_property('flat_segmentation_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def image_source(self, dataset_name=None, use_stored=True):
        return self.get_property('image_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def pychunkgraph_endpoint(self, dataset_name=None, use_stored=True):
        return self.get_property('pychunkgraph_endpoint',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def pychunkgraph_segmentation_source(self, dataset_name=None, use_stored=True):
        return self.get_property('pychunkgraph_segmentation_source',
                                 dataset_name=dataset,
                                 use_stored=use_stored)

    def refresh_stored_data(self):
        for ds in self.info_cache.keys():
            self.get_dataset_info(dataset=ds, use_stored=False)
