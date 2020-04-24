from .base import ClientBaseWithDataset, _api_versions, _api_endpoints
from .auth import AuthClient
from .endpoints import infoservice_common, infoservice_api_versions, default_global_server_address
from .format_utils import output_map_raw, output_map_precomputed, output_map_graphene
import requests
from warnings import warn

server_key = "i_server_address"


def InfoServiceClient(server_address=None,
                      dataset_name=None,
                      auth_client=None,
                      api_version='latest',
                      ):
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(api_version, server_key, server_address,
                                            infoservice_common, infoservice_api_versions, auth_header)

    InfoClient = client_mapping[api_version]
    return InfoClient(server_address=server_address,
                      auth_header=auth_header,
                      api_version=api_version,
                      endpoints=endpoints,
                      server_name=server_key,
                      dataset_name=dataset_name,
                      )


class InfoServiceClientLegacy(ClientBaseWithDataset):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name,
                 dataset_name):
        super(InfoServiceClientLegacy, self).__init__(server_address,
                                                      auth_header,
                                                      api_version,
                                                      endpoints,
                                                      server_name,
                                                      dataset_name)
        self.info_cache = dict()

    def get_datasets(self):
        """Query which datasets are available at the info service

        Returns
        -------
        list
            List of dataset names
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['datasets'].format_map(endpoint_mapping)

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
            url = self._endpoints['dataset_info'].format_map(endpoint_mapping)

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


client_mapping = {0: InfoServiceClientLegacy}
