import requests
from urllib.parse import urlparse
from warnings import warn
from annotationframeworkclient.endpoints import infoservice_endpoints as ie
from annotationframeworkclient import endpoints

def format_precomputed_neuroglancer(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = f'precomputed://{objurl}'
    elif qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'precomputed://gs://{qry.path[1:]}'
    else:
        objurl_out = None
    return objurl_out

def format_precomputed_https(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = f'precomputed://https://storage.googleapis.com/{qry.path[1:]}'
    elif qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'precomputed://{objurl}'
    else:
        objurl_out = None
    return objurl_out

def format_graphene(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = f'graphene://{objurl}'
    elif qry.scheme == 'graphene':
        objurl_out = objurl
    else:
        objurl_out = None
    return objurl_out

def format_cloudvolume(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'graphene':
        return format_graphene(objurl)
    elif qry.scheme == 'gs' or qry.scheme == 'http' or qry.scheme == 'https':
        return format_precomputed_https(objurl)
    else:
        return None

def format_raw(objurl):
    return objurl

# No reformatting
output_map_raw = {}

# Use precomputed://gs:// links for neuroglancer, but use precomputed://https://storage.googleapis.com links in cloudvolume
output_map_precomputed = {'raw': format_raw,
                   'cloudvolume': format_precomputed_https,
                   'neuroglancer': format_precomputed_neuroglancer}

# Use graphene://https:// links for both neuroglancer and cloudvolume
output_map_graphene = {'raw': format_raw,
                       'cloudvolume': format_graphene,
                       'neuroglancer': format_graphene}

class InfoServiceClient(object):
    def __init__(self, server_address=None, dataset_name=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address

        self._dataset_name = dataset_name
        self.session = requests.Session()
        self.info_cache = dict()

        self._default_url_mapping = {"i_server_address": self._server_address}

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @server_address.setter
    def server_address(self, value):
        self._server_address = value
        self._default_url_mapping['i_server_address'] = value

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def get_datasets(self):
        """Query which datasets are available at the info service
        
        Returns:
            list: List of dataset names
        """
        endpoint_mapping = self.default_url_mapping
        url = ie['datasets'].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def get_dataset_info(self, dataset_name=None, use_stored=True):
        if dataset_name is None:
            dataset_name = self.dataset_name
        assert(dataset_name is not None)
        
        if (not use_stored) or (dataset_name not in self.info_cache):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping['dataset_name'] = dataset_name
            url = ie['dataset_info'].format_map(endpoint_mapping)
        
            response = self.session.get(url)
            assert(response.status_code == 200)
        
            self.info_cache[dataset_name] = response.json()
        
        return self.info_cache.get(dataset_name, None)

    def _get_property(self, info_property, dataset_name=None, use_stored=True, format_for='raw', output_map=output_map_raw):
        if dataset_name is None:
            dataset_name = self.dataset_name
        assert(dataset_name is not None)

        self.get_dataset_info(dataset_name=dataset_name, use_stored=use_stored)
        return output_map.get(format_for, format_raw)(self.info_cache[dataset_name].get(info_property, None))

    def annotation_endpoint(self, dataset_name=None, use_stored=True):
        return self._get_property('annotation_engine_endpoint',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  output_map=output_map_raw)

    def flat_segmentation_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        return self._get_property('flat_segmentation_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for,
                                 output_map=output_map_precomputed)

    def image_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        return self._get_property('image_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for,
                                 output_map=output_map_precomputed)

    def synapse_segmentation_source(self, dataset_name=None,
                                        use_stored=True, format_for='raw'):
        return self._get_property('synapse_segmentation_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_precomputed)

    def supervoxel_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        return self._get_property('pychunkedgraph_supervoxel_source',
                                  dataset_name=dataset_name, use_stored=use_stored,
                                  format_for=format_for, output_map=output_map_precomputed)

    def pychunkedgraph_endpoint(self, dataset_name=None, use_stored=True):
        return self._get_property('pychunkgraph_endpoint',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  output_map=output_map_raw)

    def pychunkgraph_endpoint(self, **kwargs):
        warn('Please use ''pychunkedgraph_endpoint''', DeprecationWarning)
        return self.pychunkedgraph_endpoint(**kwargs)

    def pychunkedgraph_segmentation_source(self, dataset_name=None,
                                        use_stored=True, format_for='raw'):
        return self._get_property('pychunkgraph_segmentation_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_graphene)

    def pychunkgraph_segmentation_source(self, **kwargs):
        warn('Please use ''pychunkedgraph_segmentation_source'' in the future.', DeprecationWarning)
        return self.pychunkedgraph_segmentation_source(**kwargs)

    def pychunkedgraph_viewer_source(self, **kwargs):
        warn('Use ''graphene_source'' instead', DeprecationWarning)
        return self.graphene_source(**kwargs)

    def graphene_source(self, dataset_name=None,
                        use_stored=True, format_for='raw'):
        return self._get_property('graphene_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for,
                                 output_map=output_map_graphene)

    def refresh_stored_data(self):
        for ds in self.info_cache.keys():
            self.get_dataset_info(dataset_name=ds, use_stored=False)

