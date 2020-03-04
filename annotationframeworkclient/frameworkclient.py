from .annotationengine import AnnotationClient
from .auth import AuthClient, default_token_file
from .chunkedgraph import ChunkedGraphClient
from .emannotationschemas import SchemaClient
from .infoservice import InfoServiceClient
from .jsonservice import JSONService
from .imagery import ImageryClient
from annotationframeworkclient.endpoints import default_server_address


class FrameworkClient(object):
    """A manager for all clients sharing common dataset and authentication information.
    This basically wraps the other clients and keeps track of the things that need to be consistent across them.
    To instantiate a client:
    client = FrameworkClient(dataset_name='my_dataset',
                             server_address='www.myserver.com',
                             auth_token_file='~/.mysecrets/secrets.json')

    Then 
        client.info is an InfoService client (see infoservice.InfoServiceClient)
        client.state is a neuroglancer state client (see jsonservice.JSONService)
        client.schema is an EM Annotation Schemas client (see emannotationschemas.SchemaClient)
        client.chunkedgraph is a Chunkedgraph client (see chunkedgraph.ChunkedGraphClient)
        client.annotation is an Annotation DB client (see annotationengine.AnnotationClient)

    All subclients are loaded lazily, and share the same dataset name, server address, and auth tokens where used.

    Parameters
    ----------
    dataset_name : str
        Dataset name for the services
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

    Returns
    -------
    FrameworkClient
        Client for programmatic handling of the framework endpoint API.
    """

    def __init__(
        self,
        dataset_name,
        server_address=None,
        auth_token_file=default_token_file,
        auth_token_key="token",
        auth_token=None,
    ):
        self._dataset_name = dataset_name
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
        self._imagery = None

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
        self.reset_services()

    def reset_services(self):
        """Reinitializes all subclients
        """
        self._auth = None
        self._info = None
        self._state = None
        self._schema = None
        self._chunkedgraph = None
        self._annotation = None
        self._imagery = None

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def server_address(self):
        return self._server_address

    @property
    def auth(self):
        if self._auth is None:
            self._auth = AuthClient(*self._auth_config)
        return self._auth

    @property
    def info(self):
        if self._info is None:
            self._info = InfoServiceClient(
                server_address=self.server_address,
                dataset_name=self.dataset_name,
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
                server_address=self.server_address,
                dataset_name=self.dataset_name,
                auth_client=self.auth,
            )
        return self._chunkedgraph

    @property
    def annotation(self):
        if self._annotation is None:
            self._annotation = AnnotationClient(
                server_address=self.server_address,
                dataset_name=self.dataset_name,
                auth_client=self.auth,
            )
        return self._annotation

    def imagery_client(self,
                       base_resolution=[4, 4, 40],
                       graphene_segmentation=True,
                       image_mip=0,
                       segmentation_mip=0,
                       segmentation=True,
                       imagery=True):
        return ImageryClient(dataset_name=self.dataset_name,
                             auth_client=self.auth,
                             pcg_client=self.chunkedgraph,
                             image_mip=image_mip,
                             segmentation_mip=segmention_mip,
                             segmentation=segmentation,
                             imagery=imagery,
                             )
