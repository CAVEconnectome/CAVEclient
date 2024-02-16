import json
from typing import Dict, Iterable, List, Mapping, Optional, Union

import numpy as np
import pandas as pd

from .auth import AuthClient
from .base import BaseEncoder, ClientBase, _api_endpoints, handle_response
from .endpoints import annotation_api_versions, annotation_common
from .tools import stage

SERVER_KEY = "ae_server_address"


def AnnotationClient(
    server_address,
    dataset_name=None,
    aligned_volume_name=None,
    auth_client=None,
    api_version="latest",
    verify=True,
    max_retries=None,
    pool_maxsize=None,
    pool_block=None,
    over_client=None,
) -> "AnnotationClientV2":
    """Factory for returning AnnotationClient

    Parameters
    ----------
    server_address : str
        server_address to use to connect to (i.e. https://minniev1.microns-daf.com)
    dataset_name : str
        Name of the datastack.
    auth_client : AuthClient or None, optional
        Authentication client to use to connect to server. If None, do not use authentication.
    api_version : str or int (default: latest)
        What version of the api to use, 0: Legacy client (i.e www.dynamicannotationframework.com)
        2: new api version, (i.e. minniev1.microns-daf.com)
        'latest': default to the most recent (current 2)
    verify : str (default : True)
        whether to verify https
    max_retries : Int or None, optional
        Set the number of retries per request, by default None. If None, defaults to requests package default.
    pool_block : Bool or None, optional
        If True, restricts pool of threads to max size, by default None. If None, defaults to requests package default.
    pool_maxsize : Int or None, optional
        Sets the max number of threads in the pool, by default None. If None, defaults to requests package default.
    over_client:
        client to overwrite configuration with

    Returns
    -------
    ClientBaseWithDatastack
        List of datastack names for available datastacks on the annotation engine
    """

    if auth_client is None:
        auth_client = AuthClient()

    auth_header = auth_client.request_header
    endpoints, api_version = _api_endpoints(
        api_version,
        SERVER_KEY,
        server_address,
        annotation_common,
        annotation_api_versions,
        auth_header,
        verify=verify,
    )

    AnnoClient = client_mapping[api_version]
    if api_version > 1:
        return AnnoClient(
            server_address,
            auth_header,
            api_version,
            endpoints,
            SERVER_KEY,
            aligned_volume_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )
    else:
        return AnnoClient(
            server_address,
            auth_header,
            api_version,
            endpoints,
            SERVER_KEY,
            dataset_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )


class AnnotationClientV2(ClientBase):
    def __init__(
        self,
        server_address,
        auth_header,
        api_version,
        endpoints,
        server_name,
        aligned_volume_name,
        verify=True,
        max_retries=None,
        pool_maxsize=None,
        pool_block=None,
        over_client=None,
        schema_client=None,
    ):
        super(AnnotationClientV2, self).__init__(
            server_address,
            auth_header,
            api_version,
            endpoints,
            server_name,
            verify=verify,
            max_retries=max_retries,
            pool_maxsize=pool_maxsize,
            pool_block=pool_block,
            over_client=over_client,
        )

        self._aligned_volume_name = aligned_volume_name
        if schema_client is None:
            self._schema_client = self.fc.schema
        else:
            self._schema_client = schema_client

    @property
    def aligned_volume_name(self):
        return self._aligned_volume_name

    def get_tables(self, aligned_volume_name: str = None):
        """Gets a list of table names for a aligned_volume_name

        Parameters
        ----------
        aligned_volume_name : str or None, optional
            Name of the aligned_volume, by default None.
            If None, uses the one specified in the client.
            Will be set correctly if you are using the framework_client

        Returns
        -------
        list
            List of table names
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        url = self._endpoints["tables"].format_map(endpoint_mapping)
        response = self.session.get(url)
        return handle_response(response)

    def get_annotation_count(self, table_name: str, aligned_volume_name: str = None):
        """Get number of annotations in a table

        Parameters
        ----------
        table_name (str):
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        int
            number of annotations
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_count"].format_map(endpoint_mapping)

        response = self.session.get(url)
        return handle_response(response)

    def get_table_metadata(self, table_name: str, aligned_volume_name: str = None):
        """Get metadata about a table

        Parameters
        ----------
        table_name (str):
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        json
            metadata about table
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_info"].format_map(endpoint_mapping)
        response = self.session.get(url)
        metadata_d = handle_response(response)
        vx = metadata_d.pop("voxel_resolution_x")
        vy = metadata_d.pop("voxel_resolution_y")
        vz = metadata_d.pop("voxel_resolution_z")
        metadata_d["voxel_resolution"] = [vx, vy, vz]
        return metadata_d

    def delete_table(self, table_name: str, aligned_volume_name: str = None):
        """Marks a table for deletion requires super admin privileges

        Parameters
        ----------
        table_name (str):
            name of table to mark for deletion
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.


        Returns
        -------
        json
            Response JSON
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name

        url = self._endpoints["table_info"].format_map(endpoint_mapping)

        response = self.session.delete(url)
        return handle_response(response)

    def create_table(
        self,
        table_name: str,
        schema_name: str,
        description: str,
        voxel_resolution: List[float],
        reference_table: str = None,
        track_target_id_updates: bool = None,
        flat_segmentation_source: str = None,
        user_id: int = None,
        aligned_volume_name: str = None,
        write_permission: str = "PRIVATE",
        read_permission: str = "PUBLIC",
        notice_text: str = None,
    ):
        """Creates a new data table based on an existing schema

        Parameters
        ----------
        table_name: str
            Name of the new table. Cannot be the same as an existing table
        schema_name: str
            Name of the schema for the new table.
        description: str
            Human readable description for what is in the table.
            Should include information about who generated the table
            What data it covers, and how it should be interpreted.
            And who should you talk to if you want to use it.
            An Example:
            a manual synapse table to detect chandelier synapses
            on 81 PyC cells with complete AISs
            [created by Agnes - agnesb@alleninstitute.org, uploaded by Forrest]
        voxel_resolution: list[float]
            voxel resolution points will be uploaded in, typically nm, i.e [1,1,1] means nanometers
            [4,4,40] would be 4nm, 4nm, 40nm voxels
        reference_table: str or None
            If the schema you are using is a reference schema
            Meaning it is an annotation of another annotation.
            Then you need to specify what the target table
            those annotations are in.
        track_target_id_updates: bool or None
            Indicates whether to automatically update reference table's foreign key
            if target annotation table row is updated.
        flat_segmentation_source: str or None
            the source to a flat segmentation that corresponds to this table
            i.e. precomputed:\\gs:\\mybucket\this_tables_annotation
        user_id: int
            If you are uploading this schema on someone else's behalf
            and you want to link this table with their ID, you can specify it here
            Otherwise, the table will be created with your userID in the user_id column.
        aligned_volume_name: str or None, optional,
            Name of the aligned_volume. If None, uses the one specified in the client.
        write_permission: str, optional
            What permissions to give the table for writing.  One of
            PRIVATE: only you can write to this table (DEFAULT)
            GROUP: only members that share a group with you can write (excluding some groups)
            PUBLIC: Anyone can write to this table. Note all data is logged, and deletes are done
            by marking rows as deleted, so all data is always recoverable
        read_permission: str, optional
            What permissions to give the table for reading. One of
            PRIVATE: only you can read this table. Intended to be used for sorting out bugs.
            GROUP: only members that share a group with you can read (intended for within group vetting)
            PUBLIC: anyone with permissions to read this datastack can read this data (DEFAULT)
        notice_text: str, optional
            Text the user will see when querying this table. Can be used to warn users of flaws,
            and uncertainty in the data, or to advertise citations that should be used with this table.
            Defaults to None, no text. If you want to remove text, send empty string.

        Returns
        -------
        json
            Response JSON

        Examples
        --------
        Basic annotation table:

            description = "Some description about the table"
            voxel_res = [4,4,40]
            client.create_table("some_synapse_table", "synapse", description, voxel_res)
        """
        if read_permission not in ["PRIVATE", "GROUP", "PUBLIC"]:
            raise ValueError("read_permission must be one of PRIVATE GROUP or PUBLIC")
        if write_permission not in ["PRIVATE", "GROUP", "PUBLIC"]:
            raise ValueError("write_permission must be one of PRIVATE GROUP or PUBLIC")

        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name

        url = self._endpoints["tables"].format_map(endpoint_mapping)

        metadata = {
            "description": description,
            "voxel_resolution_x": float(voxel_resolution[0]),
            "voxel_resolution_y": float(voxel_resolution[1]),
            "voxel_resolution_z": float(voxel_resolution[2]),
            "read_permission": read_permission,
            "write_permission": write_permission,
        }
        if user_id is not None:
            metadata["user_id"] = user_id
        if notice_text is not None:
            metadata["notice_text"] = notice_text
        if reference_table is not None:
            metadata["table_metadata"] = {
                "reference_table": reference_table,
                "track_target_id_updates": track_target_id_updates,
            }
        if flat_segmentation_source is not None:
            metadata["flat_segmentation_source"] = flat_segmentation_source

        data = {
            "schema_type": schema_name,
            "table_name": table_name,
            "metadata": metadata,
        }

        response = self.session.post(url, json=data)
        return handle_response(response, as_json=False)

    def update_metadata(
        self,
        table_name: str,
        description: str = None,
        flat_segmentation_source: str = None,
        read_permission: str = None,
        write_permission: str = None,
        user_id: int = None,
        notice_text: str = None,
        aligned_volume_name: str = None,
    ):
        """Update the metadata on an existing table

        Parameters
        ----------
        table_name (str): name of table to update
        description (str, optional): text description of the the table.
            Defaults to None (will not update).
        flat_segmentation_source (str, optional): cloudpath to a flat segmentation associated with this table.
            Defaults to None (will not update).
        read_permission: str, optional
            What permissions to give the table for reading. One of
            PRIVATE: only you can read this table. Intended to be used for sorting out bugs.
            GROUP: only members that share a group with you can read (intended for within group vetting)
            PUBLIC: anyone with permissions to read this datastack can read this data
            Defaults to None (will not update).
        write_permission: str, optional
            What permissions to give the table for writing.  One of
            PRIVATE: only you can write to this table
            GROUP: only members that share a group with you can write (excluding some groups)
            PUBLIC: Anyone can write to this table. Note all data is logged, and deletes are done
            by marking rows as deleted, so all data is always recoverable
            Defaults to None (will not update).
        user_id (int, optional): change ownership of this table to this user_id.
            Note, if you use this you will not be able to update the metadata on this table any longer
            and depending on permissions may not be able to read or write to it
            Defaults to None. (will not update)
        notice_text: str, optional
            Text the user will see when querying this table. Can be used to warn users of flaws,
            and uncertainty in the data, or to advertise citations that should be used with this table.
            Defaults to None. (will not update)
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.
        """
        if read_permission is not None:
            if read_permission not in ["PRIVATE", "GROUP", "PUBLIC"]:
                raise ValueError(
                    "read_permission must be one of PRIVATE GROUP or PUBLIC"
                )
        if write_permission is not None:
            if write_permission not in ["PRIVATE", "GROUP", "PUBLIC"]:
                raise ValueError(
                    "write_permission must be one of PRIVATE GROUP or PUBLIC"
                )

        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name

        url = self._endpoints["tables"].format_map(endpoint_mapping)

        metadata = {}
        if description is not None:
            metadata["description"] = description
        if read_permission is not None:
            metadata["read_permission"] = read_permission
        if write_permission is not None:
            metadata["write_permission"] = write_permission
        if flat_segmentation_source is not None:
            metadata["flat_segmentation_source"] = flat_segmentation_source
        if user_id is not None:
            metadata["user_id"] = user_id
        if notice_text is not None:
            metadata["notice_text"] = notice_text

        data = {"table_name": table_name, "metadata": metadata}
        response = self.session.put(url, json=data)
        return handle_response(response, as_json=True)

    def get_annotation(
        self,
        table_name: str,
        annotation_ids: (int or Iterable),
        aligned_volume_name: str = None,
    ):
        """Retrieve an annotation or annotations by id(s) and table name.

        Parameters
        ----------
        table_name : str
            Name of the table
        annotation_ids : int or iterable
            ID or IDS of the annotation to retreive
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        list
            Annotation data
        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)
        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        params = {"annotation_ids": ",".join([str(a) for a in annotation_ids])}
        response = self.session.get(url, params=params)
        return handle_response(response)

    def post_annotation(
        self, table_name: str, data: (dict or List), aligned_volume_name: str = None
    ):
        """Post one or more new annotations to a table in the AnnotationEngine.
        All inserted annotations will be marked as 'valid'. To invalidate
        annotations refer to 'update_annotation', 'update_annotation_df'
        and 'delete_annotation' methods.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)

        try:
            iter(data)
        except TypeError:
            data = [data]

        data = {"annotations": data}

        response = self.session.post(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json"},
        )
        return handle_response(response)

    @staticmethod
    def process_position_columns(
        df: pd.DataFrame,
        position_columns: Optional[Union[Iterable[str], Mapping[str, str]]],
    ):
        """Process a dataframe into a list of dictionaries

        Parameters
        ----------
        df :
            Dataframe to process
        position_columns :
            See `.post_annotation_df`

        Returns
        -------
        dict
            Annotations ready for posting
        """
        if position_columns is None:
            position_columns = [c for c in df.columns if c.endswith("_position")]
        if isinstance(position_columns, (list, np.ndarray, pd.Index)):
            position_columns = {c: c.rsplit("_", 1)[0] for c in position_columns}
        if not isinstance(position_columns, dict):
            raise ValueError("position_columns must be a list, dict or None")

        data = df.to_dict(orient="records")
        for d in data:
            for k, v in position_columns.items():
                pos = d.pop(k)
                d[v] = {"position": pos}
        return data

    def post_annotation_df(
        self,
        table_name: str,
        df: pd.DataFrame,
        position_columns: Optional[Union[Iterable[str], Mapping[str, str]]],
        aligned_volume_name=None,
    ):
        """Post one or more new annotations to a table in the AnnotationEngine.
        All inserted annotations will be marked as 'valid'. To invalidate
        annotations see 'update_annotation', 'update_annotation_df'
        and 'delete_annotation' methods.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        df : pd.DataFrame
            A pandas dataframe containing the annotations. Columns should be fields in schema,
            position columns need to be called out in position_columns argument.
        position_columns: dict or (list or np.array or pd.Index) or None
            if None, will look for all columns with 'X_position' in the name and assume they go
            in fields called "X".
            if Iterable assumes each column given ends in _position.
            (i.e. ['pt_position'] if 'pt' is the name of the position field in schema)
            if Mapping, keys are names of columns in dataframe, values are the names of the fields
            (i.e. {'pt_column': 'pt'} would be correct if you had one column named 'pt_column'
            which needed to go into a schema with a position column called 'pt')

        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        data = self.process_position_columns(df, position_columns)

        return self.post_annotation(
            table_name, data, aligned_volume_name=aligned_volume_name
        )

    def update_annotation(
        self, table_name: str, data: Union[Dict, List], aligned_volume_name: str = None
    ):
        """Update one or more new annotations to a table in the AnnotationEngine.
        Updating is implemented by invalidating the old annotation
        and inserting a new annotation row, which will receive a new primary key ID.

        Notes
        -----
        If annotations ids were user provided upon insertion the database will
        autoincrement from the current max id in the table.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        data : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
            each dict must contain an "id" field which is the ID of the annotation to update
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name
        if isinstance(data, dict):
            data = [data]

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)

        try:
            iter(data)
        except TypeError:
            data = [data]

        data = {"annotations": data}

        response = self.session.put(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json"},
        )
        return handle_response(response)

    def update_annotation_df(
        self,
        table_name: str,
        df: pd.DataFrame,
        position_columns: (Iterable[str] or Mapping[str, str] or None),
        aligned_volume_name=None,
    ):
        """Update one or more annotations to a table in the AnnotationEngine using a
        dataframe as format. Updating is implemented by invalidating the old annotation
        and inserting a new annotation row, which will receive a new primary key ID.

        Notes
        -----
        If annotations ids were user provided upon insertion the database will
        autoincrement from the current max id in the table.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        df : pd.DataFrame
            A pandas dataframe containing the annotations. Columns should be fields in schema,
            position columns need to be called out in position_columns argument.
        position_columns: dict or (list or np.array or pd.Index) or None
            if None, will look for all columns with 'X_position' in the name and assume they go
            in fields called "X".
            if Iterable assumes each column given ends in _position.
            (i.e. ['pt_position'] if 'pt' is the name of the position field in schema)
            if Mapping, keys are names of columns in dataframe, values are the names of the fields
            (i.e. {'pt_column': 'pt'} would be correct if you had one column named 'pt_column'
            which needed to go into a schema with a position column called 'pt')

        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        data = self.process_position_columns(df, position_columns)

        return self.update_annotation(
            table_name, data, aligned_volume_name=aligned_volume_name
        )

    def delete_annotation(
        self,
        table_name: str,
        annotation_ids: Union[dict, List],
        aligned_volume_name: str = None,
    ):
        """Delete one or more annotations in a table. Annotations that are
        deleted are recorded as 'non-valid' but are not physically removed from the table.

        Parameters
        ----------
        table_name : str
            Name of the table where annotations will be added
        annotation_ids : dict or list,
            A list of (or a single) dict of schematized annotation data matching the target table.
            each dict must contain an "id" field which is the ID of the annotation to update
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        json
            Response JSON: a list of new annotation IDs.

        """
        if aligned_volume_name is None:
            aligned_volume_name = self.aligned_volume_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping["aligned_volume_name"] = aligned_volume_name
        endpoint_mapping["table_name"] = table_name
        url = self._endpoints["annotations"].format_map(endpoint_mapping)

        try:
            iter(annotation_ids)
        except TypeError:
            annotation_ids = [annotation_ids]

        data = {"annotation_ids": annotation_ids}

        response = self.session.delete(
            url,
            data=json.dumps(data, cls=BaseEncoder),
            headers={"Content-Type": "application/json"},
        )
        return handle_response(response)

    def stage_annotations(
        self,
        table_name=None,
        schema_name=None,
        update=False,
        id_field=False,
        table_resolution=None,
        annotation_resolution=None,
    ):
        """
        Get a StagedAnnotations object to help produce correctly formatted annotations for a given table or schema.
        StagedAnnotation objects can be uploaded directly with `upload_staged_annotations`.

        Parameters
        ----------
        table_name : str, optional
            Table name to stage annotations for, by default None.
        schema_name : str, optional
            Schema name to use to make annotations. Only needed if the table_name is not set, by default None
        update : bool, optional
            Set to True if individual annotations are going to be updated, by default False.
        id_field : bool, optional
            Set to True if id fields are to be specified. Not needed if update is True, which always needs id fields. Optional, by default False
        table_resolution : list-like or None, optional
            Voxel resolution of spatial points in the table in nanometers. This is found automatically from the info service if a table name is provided, by default None.
            If annotation_resolution is also set, this allows points to be scaled correctly for the table.
        annotation_resolution : list-like, optional
            Voxel resolution of spatial points provided by the user when creating annotations. If the table resolution is also available (manually or from the info service),
            annotations are correctly rescaled for the volume. By default, None.
        """

        if table_name is not None:
            obj_name = table_name
            table_meta = self.get_table_metadata(table_name)
            schema_name = table_meta["schema_type"]
            table_resolution = table_meta["voxel_resolution"]
        else:
            if schema_name is None:
                raise ValueError("Must specify either table name or schema name")
            obj_name = schema_name

        schema = self._schema_client.schema_definition(schema_name)
        return stage.StagedAnnotations(
            schema,
            name=obj_name,
            update=update,
            id_field=id_field,
            annotation_resolution=annotation_resolution,
            table_resolution=table_resolution,
            table_name=table_name,
        )

    def upload_staged_annotations(
        self,
        staged_annos: stage.StagedAnnotations,
        aligned_volume_name: str = None,
    ):
        """
        Upload annotations directly from an Annotation Guide object.
        This method uses the options specified in the object, including table name and if the annotation is an update or not.

        Parameters
        ----------
        staged_annos : guide.AnnotationGuide
            AnnotationGuide object with a specified table name and a collection of annotations already filled in.
        aligned_volume_name : str or None, optional
            Name of the aligned_volume. If None, uses the one specified in the client.

        Returns
        -------
        List or dict
            If new annotations are posted, a list of ids.
            If annotations are being updated, a dictionary with the mapping from old ids to new ids.
        """
        if staged_annos.table_name is None:
            raise ValueError(
                "Only annotation guide objects with a specified table name can be used here"
            )
        if staged_annos.is_update:
            return self.update_annotation(
                staged_annos.table_name,
                staged_annos.annotation_list,
                aligned_volume_name=aligned_volume_name,
            )
        else:
            return self.post_annotation(
                staged_annos.table_name,
                staged_annos.annotation_list,
                aligned_volume_name=aligned_volume_name,
            )


client_mapping = {
    2: AnnotationClientV2,
    "latest": AnnotationClientV2,
}
