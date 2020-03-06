import requests
from urllib.parse import urlparse
from warnings import warn
from annotationframeworkclient.endpoints import infoservice_endpoints as ie
from annotationframeworkclient import endpoints
from .auth import AuthClient


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
    """Client for interacting with the Info Service

    Parameters
    ----------
    server_address : str or None, optional
        Address of the Info Service. If None, defaults to www.dynamicannotationframework.com
    dataset_name : str or None,
        Name of the dataset to query. If None, the dataset must be specified for every query.
    auth_client : auth.AuthClient or None, optional
        Instance of an AuthClient with token to handle authorization. If None, does not specify a token.    """

    def __init__(self, server_address=None, dataset_name=None, auth_client=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address

        self._dataset_name = dataset_name

        if auth_client is None:
            auth_client = AuthClient()

        self.session = requests.Session()
        self.session.headers.update(auth_client.request_header)

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

        Returns
        -------
        list
            List of dataset names
        """
        endpoint_mapping = self.default_url_mapping
        url = ie['datasets'].format_map(endpoint_mapping)

        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_dataset_info(self, dataset_name=None, use_stored=True):
        """Gets the info record for a dataset

        Parameters
        ----------
        dataset_name : str, optional
            Dataset to look up. If None, uses the one specified by the client. By default None
        use_stored : bool, optional
            If True and the information has already been queried for that dataset, then uses the cached version. If False, re-queries the infromation. By default True

        Returns
        -------
        dict or None
            The complete info record for the dataset
        """
        if dataset_name is None:
            dataset_name = self.dataset_name
        if dataset_name is None:
            raise ValueError('No Dataset set')

        if (not use_stored) or (dataset_name not in self.info_cache):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping['dataset_name'] = dataset_name
            url = ie['dataset_info'].format_map(endpoint_mapping)

            response = self.session.get(url)
            response.raise_for_status()

            self.info_cache[dataset_name] = response.json()

        return self.info_cache.get(dataset_name, None)

    def _get_property(self, info_property, dataset_name=None, use_stored=True, format_for='raw', output_map=output_map_raw):
        if dataset_name is None:
            dataset_name = self.dataset_name
        if dataset_name is None:
            raise ValueError('No Dataset set')

        self.get_dataset_info(dataset_name=dataset_name, use_stored=use_stored)
        return output_map.get(format_for, format_raw)(self.info_cache[dataset_name].get(info_property, None))

    def annotation_endpoint(self, dataset_name=None, use_stored=True):
        """AnnotationEngine endpoint for a dataset.

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.

        Returns
        -------
        str
            Location of the AnnotationEngine
        """
        return self._get_property('annotation_engine_endpoint',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  output_map=output_map_raw)

    def flat_segmentation_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        """Cloud path to the flat segmentation for the dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.
        format_for : 'raw', 'cloudvolume', or 'neuroglancer', optional
            Formats the path for different uses.
            If 'raw' (default), the path in the InfoService is passed along.
            If 'cloudvolume', a "precomputed://gs://" type path is converted to a full https URL.
            If 'neuroglancer', a full https URL is converted to a "precomputed://gs://" type path.

        Returns
        -------
        str
            Formatted cloud path to the flat segmentation
        """
        return self._get_property('flat_segmentation_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_precomputed)

    def image_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        """Cloud path to the imagery for the dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.
        format_for : 'raw', 'cloudvolume', or 'neuroglancer', optional
            Formats the path for different uses.
            If 'raw' (default), the path in the InfoService is passed along.
            If 'cloudvolume', a "precomputed://gs://" type path is converted to a full https URL.
            If 'neuroglancer', a full https URL is converted to a "precomputed://gs://" type path.

        Returns
        -------
        str
            Formatted cloud path to the flat segmentation
        """
        return self._get_property('image_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_precomputed)

    def synapse_segmentation_source(self, dataset_name=None,
                                    use_stored=True, format_for='raw'):
        """Cloud path to the synapse segmentation for a dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.
        format_for : 'raw', 'cloudvolume', or 'neuroglancer', optional
            Formats the path for different uses.
            If 'raw' (default), the path in the InfoService is passed along.
            If 'cloudvolume', a "precomputed://gs://" type path is converted to a full https URL.
            If 'neuroglancer', a full https URL is converted to a "precomputed://gs://" type path.

        Returns
        -------
        str
            Formatted cloud path to the synapse segmentation
        """
        return self._get_property('synapse_segmentation_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_precomputed)

    def supervoxel_source(self, dataset_name=None, use_stored=True, format_for='raw'):
        """Cloud path to the supervoxel segmentation for a dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.
        format_for : 'raw', 'cloudvolume', or 'neuroglancer', optional
            Formats the path for different uses.
            If 'raw' (default), the path in the InfoService is passed along.
            If 'cloudvolume', a "precomputed://gs://" type path is converted to a full https URL.
            If 'neuroglancer', a full https URL is converted to a "precomputed://gs://" type path.

        Returns
        -------
        str
            Formatted cloud path to the supervoxel segmentation
        """
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
        """Cloud path to the chunkgraph-backed Graphene segmentation for a dataset

        Parameters
        ----------
        dataset_name : str or None, optional
            Name of the dataset to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.
        format_for : 'raw', 'cloudvolume', or 'neuroglancer', optional
            Formats the path for different uses.
            If 'raw' (default), the path in the InfoService is passed along.
            If 'cloudvolume', a "graphene://https://" type path is used
            If 'neuroglancer', a "graphene://https://" type path is used, as needed by Neuroglancer.

        Returns
        -------
        str
            Formatted cloud path to the Graphene segmentation
        """
        return self._get_property('graphene_source',
                                  dataset_name=dataset_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_graphene)

    def refresh_stored_data(self):
        """Reload the stored info values from the server.
        """
        for ds in self.info_cache.keys():
            self.get_dataset_info(dataset_name=ds, use_stored=False)
