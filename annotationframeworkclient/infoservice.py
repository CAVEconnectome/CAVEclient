from .base import ClientBaseWithDataset, ClientBaseWithDatastack, _api_versions, _api_endpoints, handle_response
from .auth import AuthClient
from .endpoints import infoservice_common, infoservice_api_versions, default_global_server_address
from .format_utils import output_map_raw, output_map_precomputed, output_map_graphene, format_raw
import requests
from warnings import warn

SERVER_KEY = "i_server_address"


def InfoServiceClient(server_address=None,
                      datastack_name=None,
                      auth_client=None,
                      api_version='latest',
                      ):
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(api_version, SERVER_KEY, server_address,
                                            infoservice_common, infoservice_api_versions, auth_header)

    InfoClient = client_mapping[api_version]
    return InfoClient(server_address,
                      auth_header,
                      api_version,
                      endpoints,
                      SERVER_KEY,
                      datastack_name,
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
        return handle_response(response)

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
            self.info_cache[dataset_name] = handle_response(response)

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
        warn('Please use ''pychunkedgraph_segmentation_source'' in the future.',
             DeprecationWarning)
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


class InfoServiceClientV2(ClientBaseWithDatastack):
    def __init__(self,
                 server_address,
                 auth_header,
                 api_version,
                 endpoints,
                 server_name,
                 datastack_name):
        super(InfoServiceClientV2, self).__init__(server_address,
                                                  auth_header,
                                                  api_version,
                                                  endpoints,
                                                  server_name,
                                                  datastack_name)
        self.info_cache = dict()
        if datastack_name is not None:
            ds_info = self.get_datastack_info(datastack_name=datastack_name)
            self._aligned_volume_name = ds_info['aligned_volume']['id']
            self._aligned_volume_id = ds_info['aligned_volume']['name']
        else:
            self._aligned_volume_name = None
            self._aligned_volume_id = None

    @property
    def aligned_volume_name(self):
        return self._aligned_volume_name

    @property
    def aligned_volume_id(self):
        return self._aligned_volume_id

    def get_datastacks(self):
        """Query which datastacks are available at the info service

        Returns
        -------
        list
            List of datastack names
        """
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['datastacks'].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def get_datastack_info(self, datastack_name=None, use_stored=True):
        """Gets the info record for a datastack

        Parameters
        ----------
        datastack_name : str, optional
            datastack to look up. If None, uses the one specified by the client. By default None
        use_stored : bool, optional
            If True and the information has already been queried for that datastack, then uses the cached version. If False, re-queries the infromation. By default True

        Returns
        -------
        dict or None
            The complete info record for the datastack
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if datastack_name is None:
            raise ValueError('No Dataset set')

        if (not use_stored) or (datastack_name not in self.info_cache):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping['datastack_name'] = datastack_name
            url = self._endpoints['datastack_info'].format_map(
                endpoint_mapping)

            response = self.session.get(url)
            self.raise_for_status(response)

            self.info_cache[datastack_name] = handle_response(response)

        return self.info_cache.get(datastack_name, None)

    def _get_property(self, info_property, datastack_name=None, use_stored=True, format_for='raw', output_map=output_map_raw):
        if datastack_name is None:
            datastack_name = self.datastack_name
        if datastack_name is None:
            raise ValueError('No Dataset set')

        self.get_datastack_info(
            datastack_name=datastack_name, use_stored=use_stored)
        value = self.info_cache[datastack_name].get(info_property, None)
        return output_map.get(format_for, format_raw)(value)

    def get_aligned_volumes(self):
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints['aligned_volumes'].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    def get_aligned_volume_info(self, datastack_name: str = None, use_stored=True):
        """Gets the info record for a aligned_volume

        Parameters
        ----------
        datastack_name : str, optional
            datastack_name to look up. If None, uses the one specified by the client. By default None
        use_stored : bool, optional
            If True and the information has already been queried for that dataset, then uses the cached version. If False, re-queries the infromation. By default True

        Returns
        -------
        dict or None
            The complete info record for the aligned_volume
        """
        return self._get_property('aligned_volume',
                                  datastack_name=datastack_name,
                                  use_stored=use_stored)

    def get_aligned_volume_info_by_id(self, aligned_volume_id: int = None, use_stored=True):
        if aligned_volume_id is None:
            aligned_volume_id = self._aligned_volume_id
        if aligned_volume_id is None:
            raise ValueError(
                "Must specify aligned_volume_id or provide datastack_name in init")

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['aligned_volume_id'] = aligned_volume_id
        url = self._endpoints['aligned_volume_by_id'].format_map(
            endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def local_server(self, datastack_name=None, use_stored=True):
        return self._get_property('local_server',
                                  datastack_name=datastack_name,
                                  use_stored=use_stored,
                                  output_map=output_map_raw)

    def annotation_endpoint(self, datastack_name=None, use_stored=True):
        """AnnotationEngine endpoint for a dataset.

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.

        Returns
        -------
        str
            Location of the AnnotationEngine
        """
        local_server = self.local_server(
            datastack_name=datastack_name, use_stored=use_stored)

        return local_server + "/annotation"

    def image_source(self, datastack_name=None, use_stored=True, format_for='raw'):
        """Cloud path to the imagery for the dataset

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.
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

        av_info = self.get_aligned_volume_info(datastack_name=datastack_name,
                                               use_stored=use_stored)
        return av_info['image_source']

    def synapse_segmentation_source(self, datastack_name=None,
                                    use_stored=True, format_for='raw'):
        """Cloud path to the synapse segmentation for a dataset

        Parameters
        ----------
        datastack_name : str or None, optional
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
                                  datastack_name=datastack_name,
                                  use_stored=use_stored,
                                  format_for=format_for,
                                  output_map=output_map_precomputed)

    def segmentation_source(self, datastack_name=None, format_for='raw', use_stored=True):
        """Cloud path to the chunkgraph-backed Graphene segmentation for a dataset

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.
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
        return self._get_property('segmentation_source',
                                  datastack_name=datastack_name,
                                  use_stored=use_stored,
                                  output_map=output_map_raw)

    def refresh_stored_data(self):
        """Reload the stored info values from the server.
        """
        for ds in self.info_cache.keys():
            self.get_datastack_info(datastack_name=ds, use_stored=False)

    def viewer_site(self, datastack_name=None, use_stored=True):
        """Get the base Neuroglancer URL for the dataset
        """
        return self._get_property('viewer_site',
                                  datastack_name=datastack_name,
                                  use_stored=use_stored,
                                  format_for='raw')


client_mapping = {0: InfoServiceClientLegacy,
                  2: InfoServiceClientV2,
                  'latest': InfoServiceClientV2}
