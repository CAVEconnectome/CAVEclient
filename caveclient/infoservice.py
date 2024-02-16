import re

import numpy as np

from .auth import AuthClient
from .base import (
    ClientBaseWithDatastack,
    _api_endpoints,
    handle_response,
)
from .endpoints import (
    default_global_server_address,
    infoservice_api_versions,
    infoservice_common,
)
from .format_utils import (
    format_raw,
    output_map,
)

SERVER_KEY = "i_server_address"


def InfoServiceClient(
    server_address=None,
    datastack_name=None,
    auth_client=None,
    api_version="latest",
    verify=True,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
    info_cache=None,
) -> "InfoServiceClientV2":
    if server_address is None:
        server_address = default_global_server_address

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(
        api_version,
        SERVER_KEY,
        server_address,
        infoservice_common,
        infoservice_api_versions,
        auth_header,
        verify=verify,
    )

    InfoClient = client_mapping[api_version]
    return InfoClient(
        server_address,
        auth_header,
        api_version,
        endpoints,
        SERVER_KEY,
        datastack_name,
        verify=verify,
        max_retries=max_retries,
        pool_maxsize=pool_maxsize,
        pool_block=pool_block,
        over_client=over_client,
        info_cache=info_cache,
    )


class InfoServiceClientV2(ClientBaseWithDatastack):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        datastack_name,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
        info_cache=None,
    ):
        super(InfoServiceClientV2, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            datastack_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )
        if not info_cache:
            self.info_cache = dict()
        else:
            self.info_cache = info_cache

        if datastack_name is not None:
            ds_info = self.get_datastack_info(datastack_name=datastack_name)
            self._aligned_volume_name = ds_info["aligned_volume"]["name"]
            self._aligned_volume_id = ds_info["aligned_volume"]["id"]
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
        url = self._endpoints["datastacks"].format_map(endpoint_mapping)

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
            raise ValueError("No Dataset set")

        if (not use_stored) or (datastack_name not in self.info_cache):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping["datastack_name"] = datastack_name
            url = self._endpoints["datastack_info"].format_map(endpoint_mapping)

            response = self.session.get(url)
            self.raise_for_status(response)

            self.info_cache[datastack_name] = handle_response(response)

        return self.info_cache.get(datastack_name, None)

    def _get_property(
        self,
        info_property,
        datastack_name=None,
        use_stored=True,
        format_for="raw",
        output_map=output_map,
    ):
        if datastack_name is None:
            datastack_name = self.datastack_name
        if datastack_name is None:
            raise ValueError("No Dataset set")

        self.get_datastack_info(datastack_name=datastack_name, use_stored=use_stored)
        value = self.info_cache[datastack_name].get(info_property, None)
        return output_map.get(format_for, format_raw)(value)

    def get_aligned_volumes(self):
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["aligned_volumes"].format_map(endpoint_mapping)
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
        return self._get_property(
            "aligned_volume", datastack_name=datastack_name, use_stored=use_stored
        )

    def get_datastacks_by_aligned_volume(self, aligned_volume: str = None):
        """Lookup what datastacks are associated with this aligned volume

        Args:
            aligned_volume (str, optional): aligned volume to lookup. Defaults to None.

        Raises:
            ValueError: if no aligned volume is specified

        Returns:
            list: a list of datastack string
        """

        if aligned_volume is None:
            aligned_volume = self._aligned_volume_name
        if aligned_volume is None:
            raise ValueError(
                "Must specify aligned_volume_id or provide datastack_name in init"
            )
        print(aligned_volume)
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume
        url = self._endpoints["datastacks_from_aligned_volume"].format_map(
            endpoint_mapping
        )

        response = self.session.get(url)
        return handle_response(response)

    def get_aligned_volume_info_by_id(
        self, aligned_volume_id: int = None, use_stored=True
    ):
        if aligned_volume_id is None:
            aligned_volume_id = self._aligned_volume_id
        if aligned_volume_id is None:
            raise ValueError(
                "Must specify aligned_volume_id or provide datastack_name in init"
            )

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_id"] = aligned_volume_id
        url = self._endpoints["aligned_volume_by_id"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def local_server(self, datastack_name=None, use_stored=True):
        return self._get_property(
            "local_server",
            datastack_name=datastack_name,
            use_stored=use_stored,
            output_map=output_map,
        )

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
            datastack_name=datastack_name, use_stored=use_stored
        )

        return local_server + "/annotation"

    def image_source(self, datastack_name=None, use_stored=True, format_for="raw"):
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

        av_info = self.get_aligned_volume_info(
            datastack_name=datastack_name, use_stored=use_stored
        )
        return av_info["image_source"]

    def synapse_segmentation_source(
        self, datastack_name=None, use_stored=True, format_for="raw"
    ):
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
        return self._get_property(
            "synapse_segmentation_source",
            datastack_name=datastack_name,
            use_stored=use_stored,
            format_for=format_for,
            output_map=output_map,
        )

    def segmentation_source(
        self, datastack_name=None, format_for="raw", use_stored=True
    ):
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
        return self._get_property(
            "segmentation_source",
            datastack_name=datastack_name,
            use_stored=use_stored,
            output_map=output_map,
            format_for=format_for,
        )

    def refresh_stored_data(self):
        """Reload the stored info values from the server."""
        for ds in self.info_cache.keys():
            self.get_datastack_info(datastack_name=ds, use_stored=False)

    def viewer_resolution(self, datastack_name=None, use_stored=True) -> np.array:
        """Get the viewer resolution metadata for this datastack

        Parameters
        ----------
        datastack_name (_type_, optional): _description_. Defaults to None.
            If None use the default one configured in the client
        use_stored (bool, optional): _description_. Defaults to True.
            Use the cached value, if False go get a new value from server

        Returns
        -------
        :
            Voxel resolution as a len(3) np.array
        """
        vx = self._get_property(
            "viewer_resolution_x",
            datastack_name=datastack_name,
            use_stored=use_stored,
        )
        vy = self._get_property(
            "viewer_resolution_y",
            datastack_name=datastack_name,
            use_stored=use_stored,
        )
        vz = self._get_property(
            "viewer_resolution_z",
            datastack_name=datastack_name,
            use_stored=use_stored,
        )
        return np.array([vx, vy, vz])

    def viewer_site(self, datastack_name=None, use_stored=True) -> str:
        """Get the base Neuroglancer URL for the dataset

        Parameters
        ----------
        datastack_name : str or None, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.

        Returns
        -------
        :
            Base URL for the Neuroglancer viewer
        """
        return self._get_property(
            "viewer_site",
            datastack_name=datastack_name,
            use_stored=use_stored,
        )

    def image_cloudvolume(self, **kwargs):
        """Generate a cloudvolume instance based on the image source, using authentication if needed and
        sensible default values for reading CAVE resources. By default, fill_missing is True and bounded
        is False. All keyword arguments are passed onto the CloudVolume initialization function, and defaults
        can be overridden.

        Requires cloudvolume to be installed, which is not included by default.
        """
        return self._make_cloudvolume(
            self.image_source(format_for="cloudvolume"), **kwargs
        )

    def segmentation_cloudvolume(self, use_client_secret=True, **kwargs):
        """Generate a cloudvolume instance based on the segmentation source, using authentication if needed and
        sensible default values for reading CAVE resources. By default, fill_missing is True and bounded
        is False. All keyword arguments are passed onto the CloudVolume initialization function, and defaults
        can be overridden.

        Requires cloudvolume to be installed, which is not included by default.
        """
        return self._make_cloudvolume(
            self.segmentation_source(format_for="cloudvolume"),
            use_client_secret=use_client_secret,
            **kwargs,
        )

    def _make_cloudvolume(self, cloudpath, use_client_secret=True, **kwargs):
        try:
            import cloudvolume
        except ImportError:
            raise ImportError(
                "Could not import cloudvolume. Make sure it is installed. See https://pypi.org/project/cloud-volume for more info."
            )

        use_https = kwargs.pop("use_https", True)
        bounded = kwargs.pop("bounded", False)
        fill_missing = kwargs.pop("fill_missing", True)

        if re.search("^graphene", cloudpath) and use_client_secret:
            # Authentication header is "Authorization {token}"
            secrets = {"token": self.session.headers.get("Authorization").split(" ")[1]}
        else:
            secrets = None

        cv = cloudvolume.CloudVolume(
            cloudpath,
            use_https=use_https,
            fill_missing=fill_missing,
            bounded=bounded,
            secrets=secrets,
            **kwargs,
        )
        return cv


client_mapping = {
    2: InfoServiceClientV2,
    "latest": InfoServiceClientV2,
}
