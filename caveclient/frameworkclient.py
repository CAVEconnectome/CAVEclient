from datetime import datetime
from typing import Optional

from .annotationengine import AnnotationClient, AnnotationClientV2
from .auth import AuthClient, default_token_file
from .chunkedgraph import ChunkedGraphClient, ChunkedGraphClientV1
from .datastack_lookup import handle_server_address
from .emannotationschemas import SchemaClient, SchemaClientLegacy
from .endpoints import default_global_server_address
from .infoservice import InfoServiceClient, InfoServiceClientV2
from .jsonservice import JSONService, JSONServiceV1
from .l2cache import L2CacheClient, L2CacheClientLegacy
from .materializationengine import MaterializationClient, MaterializationClientType
from .skeletonservice import SkeletonClient


class GlobalClientError(Exception):
    pass


class CAVEclient(object):
    def __new__(
        cls,
        datastack_name=None,
        server_address=None,
        auth_token_file=None,
        auth_token_key=None,
        auth_token=None,
        global_only=False,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        desired_resolution=None,
        info_cache=None,
        write_server_cache=True,
        version: Optional[int] = None,
    ):
        """A manager for all clients sharing common datastack and authentication information.

        This client wraps all the other clients and keeps track of the things that need to be consistent across them.
        To instantiate a client:

            from caveclient import CAVEclient

            client = CAVEclient(datastack_name='my_datastack',
                                server_address='www.myserver.com',
                                auth_token_file='~/.mysecrets/secrets.json')

        Then:

        - `client.annotation` is an `AnnotationClient` (see [client.annotation](../client_api/annotation.md))
        - `client.auth` is an `AuthClient` (see [client.auth](../client_api/auth.md))
        - `client.chunkedgraph` is a `ChunkedGraphClient` (see [client.chunkedgraph](../client_api/chunkedgraph.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../client_api/info.md))
        - `client.l2cache` is an `L2CacheClient` (see [client.l2cache](../client_api/l2cache.md))
        - `client.materialize` is a `MaterializationClient` (see [client.materialize](../client_api/materialize.md))
        - `client.skeleton` is a `SkeletonClient` (see [client.skeleton](../client_api/skeleton.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../client_api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../client_api/state.md))


        All subclients are loaded lazily and share the same datastack name, server address, and auth tokens where used.
        If creating a client without a datastack name, the client will only have access to the global services.

        Parameters
        ----------
        datastack_name : str, optional
            Datastack name for the services. Almost all services need this and will not work if it is not passed.
        server_address : str or None
            URL of the framework server. If None, chooses the default server global.daf-apis.com.
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
        max_retries : int or None, optional
            Sets the default number of retries on failed requests.
            If None, uses the value set in the session defaults.
        pool_maxsize : int or None, optional
            Sets the max number of threads in a requests pool, although this value will
            be exceeded if pool_block is set to False. If None, uses the value set in
            the session defaults.
        pool_block: bool or None, optional
            If True, prevents the number of threads in a requests pool from exceeding
            the max size. If None, uses the value set in the session defaults.
        desired_resolution : Iterable[float]or None, optional
            If given, should be a list or array of the desired resolution you want queries returned in
            useful for materialization queries.
        info_cache: dict or None, optional
            Pre-computed info cache, bypassing the lookup of datastack info from the info service. Should only be used in cases where this information is cached and thus repetitive lookups can be avoided.
        write_server_cache: bool, optional
            If True, write the map between datastack and server address to a local cache file that is used to look up server addresses if not provided. Optional, defaults to True.
        version:
            The default materialization version of the datastack to use. If None, the
            latest version is used. Optional, defaults to None.
        """
        server_address = handle_server_address(
            datastack_name, server_address, write=write_server_cache
        )

        if global_only or datastack_name is None:
            return CAVEclientGlobal(
                server_address=server_address,
                auth_token_file=auth_token_file,
                auth_token_key=auth_token_key,
                auth_token=auth_token,
                max_retries=max_retries,
                pool_maxsize=pool_maxsize,
                pool_block=pool_block,
                info_cache=info_cache,
            )
        else:
            return CAVEclientFull(
                datastack_name=datastack_name,
                server_address=server_address,
                auth_token_file=auth_token_file,
                auth_token_key=auth_token_key,
                auth_token=auth_token,
                max_retries=max_retries,
                pool_maxsize=pool_maxsize,
                pool_block=pool_block,
                desired_resolution=desired_resolution,
                info_cache=info_cache,
                version=version,
            )


class CAVEclientGlobal(object):
    def __init__(
        self,
        server_address=None,
        auth_token_file=None,
        auth_token_key=None,
        auth_token=None,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        info_cache=None,
    ):
        """A manager for all clients sharing common datastack and authentication information.

        This client wraps all the other clients and keeps track of the things that need to be consistent across them.
        To instantiate a client:

            from caveclient import CAVEclient

            client = CAVEclient(datastack_name='my_datastack',
                                server_address='www.myserver.com',
                                auth_token_file='~/.mysecrets/secrets.json')

        Then:

        - `client.auth` is an `AuthClient` (see [client.auth](../client_api/auth.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../client_api/info.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../client_api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../client_api/state.md))

        All subclients are loaded lazily and share the same datastack name, server address, and auth tokens (where used).

        Parameters
        ----------
        server_address : str or None
            URL of the framework server. If None, chooses the default server global.daf-apis.com.
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
        max_retries : int or None, optional
            Sets the default number of retries on failed requests. Optional, by default 2.
        pool_maxsize : int or None, optional
            Sets the max number of threads in a requests pool, although this value will be exceeded if pool_block is set to False. Optional, uses requests defaults if None.
        pool_block: bool or None, optional
            If True, prevents the number of threads in a requests pool from exceeding the max size. Optional, uses requests defaults (False) if None.
        info_cache: dict or None, optional
            Pre-computed info cache, bypassing the lookup of datastack info from the info service. Should only be used in cases where this information is cached and thus repetitive lookups can be avoided.

        See Also
        --------

        [set_session_defaults](../extended_api/session_config.md/#caveclient.session_config.set_session_defaults)

        [get_session_defaults](../extended_api/session_config.md/#caveclient.session_config.get_session_defaults)
        """
        if server_address is None:
            server_address = default_global_server_address
        self._server_address = server_address
        self._auth_config = {}
        self.change_auth(
            auth_token_file=auth_token_file,
            auth_token_key=auth_token_key,
            auth_token=auth_token,
        )
        self._max_retries = max_retries
        self._pool_maxsize = pool_maxsize
        self._pool_block = pool_block
        self._info_cache = info_cache

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
            auth_token_file = self._auth_config.get("auth_token_file", None)
        if auth_token_key is None:
            auth_token_key = self._auth_config.get("auth_token_key", None)

        self._auth_config = {
            "token_file": auth_token_file,
            "token_key": auth_token_key,
            "token": auth_token,
            "server_address": self._server_address,
        }
        self._reset_services()

    def _reset_services(self):
        self._auth = None
        self._info = None
        self._state = None
        self._schema = None

    @property
    def server_address(self):
        """The server address for the client."""
        return self._server_address

    @property
    def auth(self) -> AuthClient:
        """
        A client for the auth service. See [client.auth](../client_api/auth.md) for more information.
        """
        if self._auth is None:
            self._auth = AuthClient(**self._auth_config)
        return self._auth

    @property
    def info(self) -> InfoServiceClientV2:
        """
        A client for the info service. See [client.info](../client_api/info.md) for more information.
        """
        if self._info is None:
            self._info = InfoServiceClient(
                server_address=self.server_address,
                datastack_name=self.datastack_name,
                auth_client=self.auth,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
                info_cache=self._info_cache,
            )
        return self._info

    @property
    def state(self) -> JSONServiceV1:
        """
        A client for the neuroglancer state service. See [client.state](../client_api/state.md)
        for more information.
        """
        if self._state is None:
            self._state = JSONService(
                server_address=self.server_address,
                auth_client=self.auth,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._state

    @property
    def schema(self) -> SchemaClientLegacy:
        """
        A client for the EM Annotation Schemas service. See [client.schema](../client_api/schema.md)
        for more information.
        """
        if self._schema is None:
            self._schema = SchemaClient(
                server_address=self.server_address,
                auth_client=self.auth,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._schema

    def _no_local_functionality(self):
        raise GlobalClientError(
            "Client in global-only mode because no datastack was set."
        )

    @property
    def annotation(self) -> None:
        self._no_local_functionality()

    @property
    def chunkedgraph(self) -> None:
        self._no_local_functionality()

    @property
    def datastack_name(self) -> None:
        return None

    def __repr__(self):
        return f"CAVEclient<datastack=None, server_address={self.server_address}>"


class CAVEclientFull(CAVEclientGlobal):
    def __init__(
        self,
        datastack_name=None,
        server_address=None,
        auth_token_file=default_token_file,
        auth_token_key="token",
        auth_token=None,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        desired_resolution=None,
        info_cache=None,
        version: Optional[int] = None,
    ):
        """A manager for all clients sharing common datastack and authentication information.

        This client wraps all the other clients and keeps track of the things that need to be consistent across them.
        To instantiate a client:

            from caveclient import CAVEclient

            client = CAVEclient(datastack_name='my_datastack',
                                server_address='www.myserver.com',
                                auth_token_file='~/.mysecrets/secrets.json')

        Then

        - `client.annotation` is an `AnnotationClient` (see [client.annotation](../client_api/annotation.md))
        - `client.auth` is an `AuthClient` (see [client.auth](../client_api/auth.md))
        - `client.chunkedgraph` is a `ChunkedGraphClient` (see [client.chunkedgraph](../client_api/chunkedgraph.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../client_api/info.md))
        - `client.l2cache` is an `L2CacheClient` (see [client.l2cache](../client_api/l2cache.md))
        - `client.materialize` is a `MaterializationClient` (see [client.materialize](../client_api/materialize.md))
        - `client.skeleton` is a `SkeletonClient` (see [client.skeleton](../client_api/skeleton.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../client_api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../client_api/state.md))

        All subclients are loaded lazily and share the same datastack name, server address, and auth tokens where used.

        Parameters
        ----------
        datastack_name : str, optional
            Datastack name for the services. Almost all services need this and will not work if it is not passed.
        server_address : str or None
            URL of the framework server. If None, chooses the default server global.daf-apis.com.
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
        max_retries : int or None, optional
            Sets the default number of retries on failed requests. Optional, by default 2.
        pool_maxsize : int or None, optional
            Sets the max number of threads in a requests pool, although this value will be exceeded if pool_block is set to False. Optional, uses requests defaults if None.
        pool_block: bool or None, optional
            If True, prevents the number of threads in a requests pool from exceeding the max size. Optional, uses requests defaults (False) if None.
        desired_resolution : Iterable[float]or None, optional
            If given, should be a list or array of the desired resolution you want queries returned in
            useful for materialization queries.
        info_cache: dict or None, optional
            Pre-computed info cache, bypassing the lookup of datastack info from the info service. Should only be used in cases where this information is cached and thus repetitive lookups can be avoided.
        version:
            The default materialization version of the datastack to use. If None, the
            latest version is used. Optional, defaults to None.

        See Also
        --------

        [set_session_defaults](../extended_api/session_config.md/#caveclient.session_config.set_session_defaults)

        [get_session_defaults](../extended_api/session_config.md/#caveclient.session_config.get_session_defaults)
        """
        super(CAVEclientFull, self).__init__(
            server_address=server_address,
            auth_token_file=auth_token_file,
            auth_token_key=auth_token_key,
            auth_token=auth_token,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            info_cache=info_cache,
        )

        self._datastack_name = datastack_name

        self._chunkedgraph = None
        self._annotation = None
        self._materialize = None
        self._skeleton = None
        self._l2cache = None
        self.desired_resolution = desired_resolution
        self.local_server = self.info.local_server()
        self.auth.local_server = self.local_server

        av_info = self.info.get_aligned_volume_info()
        self._aligned_volume_name = av_info["name"]

        # this uses the setter, and also sets the timestamp
        self.version = version

    @property
    def version(self) -> Optional[int]:
        """The default materialization version of the datastack to use for queries which
        expect a version. Also sets the timestamp to the corresponding timestamp of the
        version for queries which rely on a timestamp."""
        return self._version

    @version.setter
    def version(self, version: Optional[int]):
        if version is None:
            self._version = None
            self._timestamp = None
        elif isinstance(version, int):
            if version in self.materialize.get_versions(expired=True):
                self._version = version
                self._timestamp = self.materialize.get_timestamp(version)
            else:
                raise ValueError(
                    f"Version {version} is not available for this datastack."
                )
        else:
            raise TypeError("Version must be an integer or None.")

    @property
    def timestamp(self) -> Optional[datetime]:
        """The default timestamp to use for queries which rely on a timestamp."""
        return self._timestamp

    def _reset_services(self):
        self._auth = None
        self._info = None
        self._state = None
        self._schema = None
        self._chunkedgraph = None
        self._annotation = None
        self._materialize = None
        self._skeleton = None
        self._l2cache = None

    @property
    def datastack_name(self) -> str:
        """The name of the datastack for the client."""
        return self._datastack_name

    @property
    def chunkedgraph(self) -> ChunkedGraphClientV1:
        """
        A client for the chunkedgraph service. See [client.chunkedgraph](../client_api/chunkedgraph.md)
        for more information.
        """
        if self._chunkedgraph is None:
            seg_source = self.info.segmentation_source()
            table_name = seg_source.split("/")[-1]

            self._chunkedgraph = ChunkedGraphClient(
                table_name=table_name,
                server_address=self.local_server,
                auth_client=self.auth,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._chunkedgraph

    @property
    def annotation(self) -> AnnotationClientV2:
        """
        A client for the annotation service. See [client.annotation](../client_api/annotation.md)
        for more information.
        """
        if self._annotation is None:
            self._annotation = AnnotationClient(
                server_address=self.local_server,
                aligned_volume_name=self._aligned_volume_name,
                auth_client=self.auth,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._annotation

    @property
    def materialize(self) -> MaterializationClientType:
        """
        A client for the materialization service. See [client.materialize](../client_api/materialize.md)
        for more information.
        """
        if self._materialize is None:
            self._materialize = MaterializationClient(
                server_address=self.local_server,
                auth_client=self.auth,
                datastack_name=self._datastack_name,
                synapse_table=self.info.get_datastack_info().get("synapse_table", None),
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
                desired_resolution=self.desired_resolution,
            )
        return self._materialize

    @property
    def skeleton(self) -> SkeletonClient:
        """
        A client for the skeleton service. See [client.skeleton](../client_api/skeleton.md)
        for more information.
        """
        if self._skeleton is None:
            self._skeleton = SkeletonClient(
                server_address=self.local_server,
                auth_client=self.auth,
                datastack_name=self._datastack_name,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._skeleton

    @property
    def state(self) -> JSONServiceV1:
        """
        A client for the neuroglancer state service. See [client.state](../client_api/state.md)
        for more information.
        """
        if self._state is None:
            self._state = JSONService(
                server_address=self.server_address,
                auth_client=self.auth,
                ngl_url=self.info.viewer_site(),
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._state

    @property
    def l2cache(self) -> L2CacheClientLegacy:
        """
        A client for the L2 cache service. See [client.l2cache](../client_api/l2cache.md)
        for more information.
        """
        if self._l2cache is None:
            seg_source = self.info.segmentation_source()
            table_name = seg_source.split("/")[-1]

            self._l2cache = L2CacheClient(
                server_address=self.local_server,
                auth_client=self.auth,
                table_name=table_name,
                max_retries=self._max_retries,
                pool_maxsize=self._pool_maxsize,
                pool_block=self._pool_block,
                over_client=self,
            )
        return self._l2cache

    def __repr__(self):
        return f"CAVEclient<datastack_name={self.datastack_name}, server_address={self.server_address}>"
