from .annotationengine import AnnotationClient
from .auth import AuthClient, default_token_file
from .chunkedgraph import ChunkedGraphClient
from .emannotationschemas import SchemaClient
from .infoservice import InfoServiceClient
from .jsonservice import JSONService
from annotationframeworkclient.endpoints import default_server_address

class FrameworkClient(object):
    def __init__(self, dataset_name, server_address=None,
                        auth_token_file=default_token_file, auth_token_key="token", auth_token=None):
        self._dataset_name = dataset_name
        if server_address is None:
            server_address = default_server_address
        self._server_address = server_address
        self._auth_config = (auth_token_file, auth_token_key, auth_token, server_address)

        self._auth = None
        self._info = None
        self._state = None
        self._schema = None
        self._chunkedgraph = None
        self._annotation = None
    
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
            self._info = InfoServiceClient(server_address=self.server_address,
                                           dataset_name=self.dataset_name,
                                           auth_client=self.auth)
        return self._info

    @property
    def state(self):
        if self._state is None:
            self._state = JSONService(server_address=self.server_address,
                                      auth_client=self.auth)
        return self._state

    @property
    def schema(self):
        if self._schema is None:
            self._schema = SchemaClient(server_address=self.server_address,
                                        auth_client=self.auth)
        return self._schema
    
    @property
    def chunkedgraph(self):
        if self._chunkedgraph is None:
            self._chunkedgraph = ChunkedGraphClient(server_address=self.server_address,
                                                    dataset_name=self.dataset_name,
                                                    auth_client=self.auth)
        return self._chunkedgraph

    @property
    def annotation(self):
        if self._annotation is None:
            self._annotation = AnnotationClient(server_address=self.server_address,
                                                dataset_name=self.dataset_name,
                                                auth_client=self.auth)
        return self._annotation