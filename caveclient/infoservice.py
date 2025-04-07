import re
from typing import Literal, Optional

import numpy as np

from .auth import AuthClient
from .base import (
    ClientBaseWithDatastack,
    _api_endpoints,
    _check_version_compatibility,
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


class InfoServiceClient(ClientBaseWithDatastack):
    """Client for interacting with the info service."""

    def __init__(
        self,
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
    ):
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
        super(InfoServiceClient, self).__init__(
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

    def get_datastacks(self) -> list:
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

    @_check_version_compatibility(kwarg_use_constraints={"image_mirror": ">=4.3.1"})
    def get_datastack_info(
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
        image_mirror: Optional[str] = None,
    ) -> dict:
        """Gets the info record for a datastack

        Parameters
        ----------
        datastack_name : str, optional
            datastack to look up. If None, uses the one specified by the client. By default None
        use_stored : bool, optional
            If True and the information has already been queried for that datastack, then uses the cached version. If False, re-queries the infromation. By default True
        image_mirror : str, optional
            If not None, will use this image mirror to get the datastack info. By default None. Requires info service app version >= 4.3.1.
            Note that getting the datastack info with a specific image mirror will overwrite the cached info.
            Use `refresh_stored_data` to reload the datastack info with the default values.

        Returns
        -------
        dict or None
            The complete info record for the datastack
        """
        if datastack_name is None:
            datastack_name = self.datastack_name
        if datastack_name is None:
            raise ValueError("No Dataset set")

        if (
            (not use_stored)
            or (datastack_name not in self.info_cache)
            or image_mirror is not None
        ):
            endpoint_mapping = self.default_url_mapping
            endpoint_mapping["datastack_name"] = datastack_name
            url = self._endpoints["datastack_info"].format_map(endpoint_mapping)
            if image_mirror is not None:
                params = {"image_source_name": image_mirror}
            else:
                params = None
            response = self.session.get(url, params=params)
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
    ) -> str:
        if datastack_name is None:
            datastack_name = self.datastack_name
        if datastack_name is None:
            raise ValueError("No Dataset set")

        self.get_datastack_info(datastack_name=datastack_name, use_stored=use_stored)
        value = self.info_cache[datastack_name].get(info_property, None)
        return output_map.get(format_for, format_raw)(value)

    def get_aligned_volumes(self) -> list:
        endpoint_mapping = self.default_url_mapping
        url = self._endpoints["aligned_volumes"].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    @_check_version_compatibility(kwarg_use_constraints={"image_mirror": ">=4.3.1"})
    def get_aligned_volume_info(
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
        image_mirror: Optional[str] = None,
    ) -> dict:
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
        if image_mirror is not None:
            av_info = self.get_image_mirrors(datastack_name=datastack_name)
            for av in av_info:
                if av["name"] == image_mirror:
                    return av
            else:
                raise ValueError(
                    f"Image source {image_mirror} not found in aligned volumes"
                )
        else:
            return self._get_property(
                "aligned_volume",
                datastack_name=datastack_name,
                use_stored=use_stored,
            )

    def get_datastacks_by_aligned_volume(
        self,
        aligned_volume: Optional[str] = None,
    ) -> list:
        """Lookup what datastacks are associated with this aligned volume

        Parameters
        ----------
        aligned_volume : str, optional
            aligned volume to lookup. If None, uses the one specified by the client. By default None

        Returns
        -------
        list
            List of datastack names
        """

        if aligned_volume is None:
            aligned_volume = self._aligned_volume_name
        if aligned_volume is None:
            raise ValueError(
                "Must specify aligned_volume_id or provide datastack_name in init"
            )
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume
        url = self._endpoints["datastacks_from_aligned_volume"].format_map(
            endpoint_mapping
        )

        response = self.session.get(url)
        return handle_response(response)

    def get_aligned_volume_info_by_id(
        self,
        aligned_volume_id: int = None,
        use_stored: bool = True,
    ) -> dict:
        """Gets the info record for a aligned_volume from its id instead of its name

        Parameters
        ----------
        aligned_volume_id : int, optional
            aligned volume id to look up. If None, uses the one specified by the client. By default None
        use_stored : bool, optional
            If True and the information has already been queried for that dataset, then uses the cached version. If False, re-queries the infromation. By default True

        Returns
        -------
        dict
            The complete info record for the aligned_volume
        """
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

    def local_server(
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
    ) -> str:
        """Get the local server address for the datastack.

        Parameters
        ----------
        datastack_name : str, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.
        use_stored : bool, optional
            If True, uses the cached value if available. If False, re-queries the InfoService. Default is True.

        Returns
        -------
        str
            Local server url for the datastack
        """

        return self._get_property(
            "local_server",
            datastack_name=datastack_name,
            use_stored=use_stored,
            output_map=output_map,
        )

    def annotation_endpoint(
        self, datastack_name: Optional[str] = None, use_stored: bool = True
    ) -> str:
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

    def image_source(
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
        format_for: Literal[
            "raw", "cloudvolume", "neuroglancer", "cave_explorer", "cave-explorer"
        ] = "raw",
        image_mirror: Optional[str] = None,
    ) -> str:
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
            If 'cave_explorer', 'cave-explorer' or "spelunker', a full https URL is converted to a modern neuroglancer path.
        image_mirror : str, optional
            If not None, will use this image mirror to get the datastack info. By default None.

        Returns
        -------
        str
            Formatted cloud path to the imagery
        """

        av_info = self.get_aligned_volume_info(
            datastack_name=datastack_name,
            use_stored=use_stored,
            image_mirror=image_mirror,
        )
        return output_map.get(format_for)(av_info["image_source"])

    @_check_version_compatibility(method_constraint=">=4.3.1")
    def get_image_mirrors(self, datastack_name: Optional[str] = None) -> list:
        """Get all image sources for a given aligned volume

        Parameters
        ----------
        datastack_name : str, optional
            Name of the datastack to look up. If None, uses the value specified by the client. Default is None.

        Returns
        -------
        list
            List of image mirror info files for the aligned volume
        """
        endpoint_mapping = self.default_url_mapping

        if datastack_name is not None:
            endpoint_mapping["datastack_name"] = datastack_name

        url = self._endpoints["image_sources"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    @_check_version_compatibility(method_constraint=">=4.3.1")
    def get_image_mirror_names(
        self,
        datastack_name: Optional[str] = None,
    ) -> list:
        """Get all image mirror names for a given aligned volume.

        Parameters
        ----------
        datastack_name: str, optional
            Name of the aligned volume to look up. If None, uses the value specified by the client. Default is None.

        Returns
        -------
        list
            List of image mirror names for the aligned volume
        """
        return [
            x["name"] for x in self.get_image_mirrors(datastack_name=datastack_name)
        ]

    def synapse_segmentation_source(
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
        format_for: Literal[
            "raw", "cloudvolume", "neuroglancer", "cave_explorer", "cave-explorer"
        ] = "raw",
    ) -> str:
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
        self,
        datastack_name: Optional[str] = None,
        use_stored: bool = True,
        format_for: Literal[
            "raw", "cloudvolume", "neuroglancer", "cave_explorer", "cave-explorer"
        ] = "raw",
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
            If 'cave_explorer', 'cave-explorer' or "spelunker', a full https URL is converted to a modern neuroglancer path.

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

    def viewer_resolution(
        self, datastack_name: Optional[str] = None, use_stored: bool = True
    ) -> np.array:
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

    def viewer_site(
        self, datastack_name: Optional[str] = None, use_stored: bool = True
    ) -> str:
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

    @_check_version_compatibility(kwarg_use_constraints={"image_mirror": ">=4.3.1"})
    def image_cloudvolume(
        self, image_mirror: Optional[str] = None, **kwargs
    ) -> "cloudvolume.CloudVolume":  # noqa: F821
        """Generate a cloudvolume instance based on the image source, using authentication if needed and
        sensible default values for reading CAVE resources. By default, fill_missing is True and bounded
        is False. All keyword arguments are passed onto the CloudVolume initialization function, and defaults
        can be overridden.

        Requires cloudvolume to be installed, which is not included by default.
        """
        return self._make_cloudvolume(
            self.image_source(format_for="cloudvolume", image_mirror=image_mirror),
            **kwargs,
        )

    #
    def segmentation_cloudvolume(
        self, use_client_secret=True, **kwargs
    ) -> "cloudvolume.CloudVolume":  # noqa: F821
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

        if re.search("^graphene", cloudpath) is not None and use_client_secret:
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
