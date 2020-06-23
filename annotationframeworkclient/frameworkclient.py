from .annotationengine import AnnotationClient
from .auth import AuthClient, default_token_file
from .chunkedgraph import ChunkedGraphClient
from .emannotationschemas import SchemaClient
from .infoservice import InfoServiceClient
from .jsonservice import JSONService
from .imagery import ImageryClient
from .lookup import LookupClient
from annotationframeworkclient.endpoints import default_server_address


class FrameworkClient(object):
    """A manager for all clients sharing common datastack and authentication information.

    This client wraps all the other clients and keeps track of the things that need to be consistent across them.
    To instantiate a client:
    client = FrameworkClient(datastack_name='my_datastack', server_address='www.myserver.com',
                             auth_token_file='~/.mysecrets/secrets.json')

    Then
    * client.info is an InfoService client (see infoservice.InfoServiceClient)
    * client.state is a neuroglancer state client (see jsonservice.JSONService)
    * client.schema is an EM Annotation Schemas client (see emannotationschemas.SchemaClient)
    * client.chunkedgraph is a Chunkedgraph client (see chunkedgraph.ChunkedGraphClient)
    * client.annotation is an Annotation DB client (see annotationengine.AnnotationClient)
    * client.imagery_client(...) will generate an imagery client.

    All subclients are loaded lazily and share the same datastack name, server address, and auth tokens where used.

    Parameters
    ----------
    datastack_name : str, optional
        Datastack name for the services. Almost all services need this and will not work if it is not passed.
    server_address : str or None
        URL of the framework server. If None, chooses the default server www.dynamicannotationframework.com.
        Optional, defaults to None.
    auth_token_file : str or None
        Path to a json file containing the auth token. If None, uses the default location. See Auth client documentation.
        Optional, defaults to None.
    auth_token_key : str
        Dictionary key for the token in the the JSON file.
        Optional, default is 'token'.
    auth_token : str or None
        Direct entry of an auth token. If None, uses the file arguments to find the token.
        Optional, default is None.
    """

    def __init__(
        self,
        datastack_name=None,
        server_address=None,
        auth_token_file=default_token_file,
        auth_token_key="token",
        auth_token=None,
    ):
        self._datastack_name = datastack_name
        if server_address is None:
            server_address = default_server_address
        self._server_address = server_address
        self._auth_config = (
            auth_token_file,
            auth_token_key,
            auth_token,
            server_address,
        )

        self._auth = None
        self._info = None
        self._state = None
        self._schema = None
        self._chunkedgraph = None
        self._annotation = None
        self._lookup = None
        self.local_server = self.info.local_server
        av_info = self.info.get_aligned_volume_info()
        self._aligned_volume_name = av_info['name']


    def change_auth(self, auth_token_file=None, auth_token_key=None, auth_token=None):
        """Change the authentication token and reset services.

        Parameters
        ----------
        auth_token_file : str, optional
            New auth token json file path, by default None, which defaults to the existing state.
        auth_token_key : str, optional
            New dictionary key under which the token is stored in the json file, by default None,
            which defaults to the existing state.
        auth_token : str, optional
            Direct entry of a new token, by default None.
        """
        if auth_token_file is None:
            auth_token_file = self._auth_config[0]
        if auth_token_key is None:
            auth_token_key = self._auth_config[1]

        self._auth_config = (
            auth_token_file,
            auth_token_key,
            auth_token,
            self._server_address,
        )
        self._reset_services()

    def _reset_services(self):
        self._auth = None
        self._info = None
        self._state = None
        self._schema = None
        self._chunkedgraph = None
        self._annotation = None
        self._imagery = None
        self._lookup = None

    @property
    def datastack_name(self):
        return self._datastack_name

    @property
    def server_address(self):
        return self._server_address

    @property
    def auth(self):
        if self._auth is None:
            self._auth = AuthClient(*self._auth_config)
        return self._auth

    @property
    def info(self)->InfoServiceClient:
        if self._info is None:
            self._info = InfoServiceClient(
                server_address=self.server_address,
                datastack_name=self.datastack_name,
                auth_client=self.auth,
            )
        return self._info

    @property
    def state(self):
        if self._state is None:
            self._state = JSONService(
                server_address=self.server_address, auth_client=self.auth
            )
        return self._state

    @property
    def schema(self):
        if self._schema is None:
            self._schema = SchemaClient(
                server_address=self.server_address, auth_client=self.auth
            )
        return self._schema

    @property
    def chunkedgraph(self):
        if self._chunkedgraph is None:
            self._chunkedgraph = ChunkedGraphClient(
                server_address=self.local_server,
                datastack_name=self.datastack_name,
                auth_client=self.auth,
            )
        return self._chunkedgraph

    @property
    def annotation(self):
        if self._annotation is None:
            self._annotation = AnnotationClient(
                server_address=self.local_server,
                aligned_volume_name=self._aligned_volume_name,
                auth_client=self.auth,
            )
        return self._annotation

    def make_lookup_client(self,
                           timestamp=None,
                           voxel_resolution=[4, 4, 40],
                           use_graphene=True):
        """Generate a lookup client based on the client configuration

        Parameters
        ----------
        timestamp : datetime.datetime, optional
            Time stamp to use for lookups, by default None
        voxel_resolution : list, optional
            Resolution of voxels in nm, by default [4, 4, 40]
        use_graphene : bool, optional
            Selection to use the graphene_segmentation by default. If False, reverts to the flat segmentation source. By default True

        Returns
        -------
        lookuptool.LookupClient
        """
        if use_graphene and self.info.graphene_source() is not None:
            return LookupClient(datastack_name=self.datastack_name,
                                segmentation_path=self.info.segmentation_source(
                                    format_for='cloudvolume'),
                                server_address=self.local_server,
                                auth_client=self.auth,
                                timestamp=timestamp,
                                voxel_resolution=voxel_resolution,
                                )
        else:
            # Default to the flat segmentation
            return LookupClient(datastack_name=self.datastack_name,
                                segmentation_path=self.info.segmentation_source(
                                    format_for='cloudvolume'),
                                server_address=self.local_server,
                                auth_client=self.auth,
                                voxel_resolution=voxel_resolution,
                                )

    def make_imagery_client(self,
                            base_resolution=[4, 4, 40],
                            graphene_segmentation=True,
                            image_mip=0,
                            segmentation_mip=0,
                            segmentation=True,
                            imagery=True):
        """Generates an imagery client based on the current framework client.

        Parameters
        ----------
        base_resolution : list, optional
            Sets the voxel resolution that bounds will be entered in, by default [4, 4, 40]
        graphene_segmentation : bool, optional
            If True, use the graphene segmentation. If false, use the flat segmentation. By default True.
        image_mip : int, optional
            Default mip level to use for imagery lookups, by default 0. Note that the same mip
            level for imagery and segmentation can correspond to different voxel resolutions.
        segmentation_mip : int, optional
            Default mip level to use for segmentation lookups, by default 0.
        segmentation : bool, optional
            If False, no segmentation cloudvolume is initialized. By default True
        imagery : bool, optional
            If False, no imagery cloudvolume is initialized. By default True

        Returns
        -------
        imagery.ImageryClient

        """
        return ImageryClient(datastack_name=self.datastack_name,
                             auth_client=self.auth,
                             pcg_client=self.chunkedgraph,
                             image_mip=image_mip,
                             segmentation_mip=segmentation_mip,
                             segmentation=segmentation,
                             imagery=imagery,
                             )