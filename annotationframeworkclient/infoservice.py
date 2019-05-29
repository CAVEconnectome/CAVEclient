import requests
from urllib.parse import urlparse
from annotationframeworkclient.endpoints import infoservice_endpoints as ie
from annotationframeworkclient import endpoints

def format_neuroglancer_precomputed(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = 'precomputed://{}'.format(objurl)
    elif qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = 'precomputed://gs://{}'.format(qry.path[1:])
    else:
        objurl_out = None
    return objurl_out


def format_neuroglancer_graphene(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'http' or qry.scheme == 'https':
        objurl_out = 'graphene://{}'.format(objurl)
    elif qry.scheme == 'graphene':
        objurl_out = objurl_out
    else:
        objurl_out = None
    return objurl_out


def format_cloudvolume(objurl):
    qry = urlparse(objurl)
    if qry.scheme == 'gs':
        objurl_out = 'https://storage.googleapis.com/{}{}'.format(qry.netloc,
                                                                  qry.path)
    elif qry.netloc == 'storage.googleapis.com':
        objurl_out = objurl
    else:
        objurl_out
    return objurl_out


def format_raw(objurl):
    return objurl


output_map = {'raw': format_raw,
              'cloudvolume': format_cloudvolume,
              'neuroglancer_flat': format_neuroglancer_precomputed,
              'neuroglancer_pcg': format_neuroglancer_graphene,
              }


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

    def get_property(self, info_property, dataset_name=None, use_stored=True, format_for='raw'):
        if dataset_name is None:
            dataset_name = self.dataset_name
        assert(dataset_name is not None)

        self.get_dataset_info(dataset_name=dataset_name, use_stored=use_stored)
        return output_map.get(format_for, format_raw)(self.info_cache[dataset_name].get(info_property, None))

    def annotation_endpoint(self, dataset_name=None, use_stored=True):
        return self.get_property('annotation_engine_endpoint',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    # def annotation_dataset_name(self, dataset_name=None, use_stored=True):
    #     return self.get_property('annotation_dataset_name',
    #                              dataset_name=dataset_name,
    #                              use_stored=use_stored)

    def pychunkedgraph_viewer_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        if format_for == 'neuroglancer':
            format_for = 'neuroglancer_pcg'
        return self.get_property('pychunkedgraph_viewer_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for)


    def flat_segmentation_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        if format_for == 'neuroglancer':
            format_for = 'neuroglancer_flat'
        return self.get_property('flat_segmentation_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for)

    def image_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        if format_for == 'neuroglancer':
            format_for = 'neuroglancer_flat'
        return self.get_property('image_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for)

    def pychunkgraph_endpoint(self, dataset_name=None, use_stored=True):
        return self.get_property('pychunkgraph_endpoint',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored)

    def pychunkgraph_segmentation_source(self, dataset_name=None,
                                         use_stored=True, format_for='raw'):
        return self.get_property('pychunkgraph_segmentation_source',
                                 dataset_name=dataset_name,
                                 use_stored=use_stored,
                                 format_for=format_for)

    def refresh_stored_data(self):
        for ds in self.info_cache.keys():
            self.get_dataset_info(dataset=ds, use_stored=False)
