import getpass
import re
from datetime import datetime
from typing import Optional

from requests.exceptions import HTTPError

from .annotationengine import AnnotationClient
from .auth import AuthClient, default_token_file
from .chunkedgraph import ChunkedGraphClient
from .datastack_lookup import handle_server_address
from .emannotationschemas import SchemaClient
from .endpoints import default_global_server_address
from .infoservice import InfoServiceClient
from .jsonservice import JSONService
from .l2cache import L2CacheClient
from .materializationengine import MaterializationClient
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

        - `client.annotation` is an `AnnotationClient` (see [client.annotation](../api/annotation.md))
        - `client.auth` is an `AuthClient` (see [client.auth](../api/auth.md))
        - `client.chunkedgraph` is a `ChunkedGraphClient` (see [client.chunkedgraph](../api/chunkedgraph.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../api/info.md))
        - `client.l2cache` is an `L2CacheClient` (see [client.l2cache](../api/l2cache.md))
        - `client.materialize` is a `MaterializationClient` (see [client.materialize](../api/materialize.md))
        - `client.skeleton` is a `SkeletonClient` (see [client.skeleton](../api/skeleton.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../api/state.md))


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
            server_address = handle_server_address(
                datastack_name, server_address, write=write_server_cache
            )

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

    @staticmethod
    def setup_token(
        server_address: str,
        overwrite: bool = True,
        open: bool = True,
    ):
        """Set up a new or existing user token for a CAVE server.

        Parameters
        ----------
        server_address : str
            The server address to set up the token for. This will be provided by your documentation or server administrator.
        overwrite : bool, optional
            If True, overwrite the existing token if it exists. If False, do not overwrite existing data.
            Optional, defaults to True.
        open : bool, optional
            If True, open the token page in a web browser to create or copy an existing token.
            Optional, defaults to True.
        """

        global_client = CAVEclientGlobal(server_address=server_address)
        page_url = global_client.auth.get_token_page(open=open)
        print(f"Visit {page_url} and copy an existing token or create a new one.")
        while True:
            new_token = getpass.getpass(
                "Paste your auth token (input will be hidden): "
            )
            new_token = new_token.strip().lower()
            if re.match(r"^[a-f0-9]{32}$", new_token):
                try:
                    new_client = CAVEclientGlobal(
                        server_address=server_address,
                        auth_token=new_token.strip(),
                    )
                    new_client.info.get_datastacks()
                    break
                except HTTPError:
                    print(
                        "Token did not work for login — check the token and your internet connectivity and try again!"
                    )
            else:
                print(
                    "Invalid token format. Please ensure it is a 32-character hexadecimal (0-9, a-f) string."
                )
        global_client.auth.save_token(token=new_token.strip(), overwrite=overwrite)
        complete_message = "You will not need to specify a server address when initializing a client for configured datastacks in the future.\nSetup complete!"

        client = CAVEclientGlobal(server_address=server_address)
        datastack_names = sorted(client.info.get_datastacks())
        datastack_name_list = [f"\t{ds}\n" for ds in datastack_names]
        if len(datastack_names) == 0:
            print("No datastacks found. Setup complete!")
            return
        else:
            while True:
                setup_all = input(
                    "Set all of your datastacks to use this token and server ('Y', recommended), specify individual datastacks ('n'), or finish now ('exit'). (Y/n/exit) "
                )
                if setup_all.lower() in ["y", "yes", ""]:
                    for ds in datastack_names:
                        _set_up_token(
                            client,
                            token=new_token,
                            datastack_name=ds,
                            overwrite=overwrite,
                        )
                    print("All datastacks are configured to use this server address.")
                    print(complete_message)
                    return
                elif setup_all.lower() in ["n", "no"]:
                    print(f"Found datastacks:\n{''.join(datastack_name_list)}")
                    while True:
                        datastack_name = input(
                            "Enter the name of a datastack to use this server automatically or 'exit' to finish: "
                        )
                        if datastack_name.lower() == "exit":
                            break
                        else:
                            ds = datastack_name.strip()
                            if ds not in datastack_names:
                                print(f"Datastack '{ds}' not found.")
                                continue
                            else:
                                _set_up_token(
                                    client,
                                    token=new_token,
                                    datastack_name=ds,
                                    overwrite=overwrite,
                                )
                                print(f"Token set up for datastack: {datastack_name}.")
                    print(complete_message)
                    return
                elif setup_all.lower() in ["exit"]:
                    print(
                        f"Finished setting up token for {server_address} with no datasets configured."
                    )
                    return
                else:
                    print("Invalid input. Please try again.")


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

        - `client.auth` is an `AuthClient` (see [client.auth](../api/auth.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../api/info.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../api/state.md))

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

    def change_auth(
        self,
        auth_token_file=None,
        auth_token_key=None,
        auth_token=None,
    ):
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
        A client for the auth service. See [client.auth](../api/auth.md) for more information.
        """
        if self._auth is None:
            self._auth = AuthClient(**self._auth_config)
        return self._auth

    @property
    def info(self) -> InfoServiceClient:
        """
        A client for the info service. See [client.info](../api/info.md) for more information.
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
    def state(self) -> JSONService:
        """
        A client for the neuroglancer state service. See [client.state](../api/state.md)
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
    def schema(self) -> SchemaClient:
        """
        A client for the EM Annotation Schemas service. See [client.schema](../api/schema.md)
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

        - `client.annotation` is an `AnnotationClient` (see [client.annotation](../api/annotation.md))
        - `client.auth` is an `AuthClient` (see [client.auth](../api/auth.md))
        - `client.chunkedgraph` is a `ChunkedGraphClient` (see [client.chunkedgraph](../api/chunkedgraph.md))
        - `client.info` is an `InfoServiceClient` (see [client.info](../api/info.md))
        - `client.l2cache` is an `L2CacheClient` (see [client.l2cache](../api/l2cache.md))
        - `client.materialize` is a `MaterializationClient` (see [client.materialize](../api/materialize.md))
        - `client.skeleton` is a `SkeletonClient` (see [client.skeleton](../api/skeleton.md))
        - `client.schema` is a `SchemaClient` (see [client.schema](../api/schema.md))
        - `client.state` is a neuroglancer `JSONService` (see [client.state](../api/state.md))

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
    def chunkedgraph(self) -> ChunkedGraphClient:
        """
        A client for the chunkedgraph service. See [client.chunkedgraph](../api/chunkedgraph.md)
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
    def annotation(self) -> AnnotationClient:
        """
        A client for the annotation service. See [client.annotation](../api/annotation.md)
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
    def materialize(self) -> MaterializationClient:
        """
        A client for the materialization service. See [client.materialize](../api/materialize.md)
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
        A client for the skeleton service. See [client.skeleton](../api/skeleton.md)
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
    def state(self) -> JSONService:
        """
        A client for the neuroglancer state service. See [client.state](../api/state.md)
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
    def l2cache(self) -> L2CacheClient:
        """
        A client for the L2 cache service. See [client.l2cache](../api/l2cache.md)
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


def _set_up_token(
    client: CAVEclientGlobal,
    token: str,
    datastack_name: str,
    overwrite: bool,
):
    local_server = client.info.get_datastack_info(datastack_name).get("local_server")
    handle_server_address(
        datastack=datastack_name,
        server_address=client.server_address,
        write=True,
        do_log=False,
    )
    if local_server:
        client._auth._local_server = local_server
        try:
            client.auth.save_token(
                token=token,
                overwrite=overwrite,
                local_server=True,
                ignore_readonly=True,
            )
        except ValueError as e:
            print(f"Could not save token for {datastack_name}: {e}")
    pass
