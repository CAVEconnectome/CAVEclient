import tqdm
import pandas as pd
import numpy as np
import datetime
import re
try:
    import cloudvolume as cv
except ImportError:
    logging.warning('Warning: Need to install cloudvolume to use Lookup client')
from .chunkedgraph import ChunkedGraphClient
from .endpoints import default_global_server_address
from .auth import AuthClient


def _is_graphene(segmentation_path):
    r = re.match("^graphene:", segmentation_path)
    if r is None:
        return False
    else:
        return True


class LookupClient(object):
    """Helper to look up supervoxels and root ids of points

    Parameters
    ----------
    datastack_name : str
        Name of the datastack
    segmentation_path : str or None, optional
        Cloud path to a graphene or precomputed segmentation. If None, requires a specified ChunkedGraph client.
    segmentation_mip : int, optional
        Mip level of the segmentation to use for lookups. Default is 0 (highest resolution).
    timestamp : datetime.datetime or None, optional
        Sets the timestamp for the state of the ChunkedGraph to query. If None, uses the current time.
    voxel_resolution : list-like, optional
        3 element list with the voxel resolution in nm. By default, [4,4,40]
    server_address : str or None, optional
        Host for the chunkedgraph server. If None, defaults to the default server value. Not used if a a ChunkedGraph client is specified.
    auth_client : auth.AuthClient or None, optional
        Initialized AuthClient. If None, produces an Auth client with default parameters.
    """

    def __init__(self,
                 segmentation_path=None,
                 segmentation_mip=0,
                 timestamp=None,
                 voxel_resolution=[4, 4, 40],
                 server_address=None,
                 auth_client=None,
                 ):
        self._segmentation_path = segmentation_path
        self._voxel_resolution = voxel_resolution
        self._timestamp = timestamp

        self._mip = segmentation_mip

        if server_address is None:
            server_address = default_global_server_address
        self._server_address = server_address

        if auth_client is None:
            self._auth_client = AuthClient()
        else:
            self._auth_client = auth_client

        if _is_graphene(self._segmentation_path):
            table_name = self._segmentation_path.split('/')[-1]
            self._chunkedgraph_client = ChunkedGraphClient(server_address=self._server_address,
                                                           table_name=table_name,
                                                           timestamp=self._timestamp,
                                                           auth_client=self._auth_client)
        else:
            self._chunkedgraph_client = None

        self._cv = None

    @property
    def cv(self):
        if self._cv is None:
            self._cv = CloudVolume(self._segmentation_path, use_https=True,
                                   bounded=False, progress=False, mip=self._mip)
        return self._cv

    @property
    def voxel_resolution(self):
        return self._voxel_resolution

    def lookup_supervoxels(self, xyzs, voxel_resolution=None, mip=None):
        """Lookup supervoxels from a list of points.

        Parameters
        ----------
        xyzs : list-like
            N-length list-like or Nx3 array of 3D points in space. Units are voxels.
        voxel_resolution : list-like or None, optional
            3 element resolution of voxels in units of nm. If None, defaults to the value set at class initialization.

        Returns
        -------
        list
            N-length list of supervoxel ids.
        """

        if voxel_resolution is None:
            voxel_resolution = self.voxel_resolution
        if mip is None:
            mip = self._mip
        sv_ids = []
        for xyz in tqdm.tqdm(xyzs):
            sv_ids.append(int(self.cv.download_point(
                xyz, size=1, coord_resolution=voxel_resolution, mip=mip).squeeze()))
        return sv_ids

    def lookup_root_ids(self, supervoxel_ids, timestamp=None):
        """ Get root ids for a collection of supervoxel_ids

        Parameters
        ----------
        supervoxel_ids : list-like
            List of supervoxel ids. Supervoxels with id 0 are passed through as null. 
        timestamp : datetime.datetime, optional
            Sets the timestamp for the state of the ChunkedGraph to query. If None, uses the value set at class initialization.

        Returns
        -------
        list
            List of root ids that the 
        """
        if timestamp is None:
            timestamp = self._timestamp

        if self._chunkedgraph_client is not None:
            root_ids = []
            for svid in tqdm.tqdm(supervoxel_ids):
                if svid == 0:
                    root_ids.append(0)
                else:
                    root_ids.append(
                        int(self._chunkedgraph_client.get_root_id(svid, timestamp=timestamp)))
        else:
            root_ids = supervoxel_ids   # No chunkedgraph means that it's a flat segmentation.
        return root_ids

    def lookup_points(self, xyzs, voxel_resolution=None, timestamp=None, mip=None):
        """[summary]

        Parameters
        ----------
        xyzs : Iterable
            Nx3 numpy.array or N-length list-like of N 3d points to look up. Each point is in voxel units.
        voxel_resolution : list-like or None, optional
            The resolution in nm of the voxel coordinates. If None, defaults to the value set for the client. By default, None.

        Returns
        -------
        list
            Root id for each point in xyzs
        list
            Supervoxel id for each point in xyzs. If a flat segmentation, set to None.
        """
        if voxel_resolution is None:
            voxel_resolution = self.voxel_resolution
        if timestamp is None:
            timestamp = self._timestamp
        if mip is None:
            mip = self._mip

        sv_ids = self.lookup_supervoxels(xyzs, voxel_resolution=voxel_resolution, mip=mip)
        root_ids = self.lookup_root_ids(sv_ids, timestamp=timestamp)
        if self._chunkedgraph_client is None:
            sv_ids = None                           # ignore supervoxels for flat segmentations
        return root_ids, sv_ids

    def lookup_dataframe(self,
                         point_column,
                         data,
                         position_suffix='_position',
                         supervoxel_suffix='_supervoxel_id',
                         root_id_suffix='_root_id',
                         voxel_resolution=None,
                         timestamp=None,
                         mip=None):
        """Expand a dataframe with supervoxel and root ids for position columns.

        Every column with spatial points is expanded into three columns (two if a flat segmentation).
        For example, if the original point column is named 'pt', it is replaced by:

        * 'pt_position' : The original spatial point data

        * 'pt_supervoxel_id' : The id of the supervoxel holding that point, or 0 if null.

        * 'pt_root_id' : The root id for the point (at a given timestamp, if specified)

        For a flat segmentation, the supervoxels are the root ids, and the supervoxel_id column is omitted.

        Parameters
        ----------
        point_column : str or list-like
            Column name or list of column names that have point data, with points in voxels.
        data : pd.DataFrame
            DataFrame with columns containing points and/or other data
        position_suffix : str, optional
            Suffix applied to the position column, by default '_position'
        supervoxel_suffix : str, optional
            Suffix applied to the supervoxel id column, by default '_supervoxel_id'
        root_id_suffix : str, optional
            Suffix applied to the root id column, by default '_root_id'
        voxel_resolution : list-like or None, optional
            The resolution in nm of the voxel coordinates. If None, defaults to the value set for the client. By default, None.
        timestamp : datetime.datetime, optional
            Sets the timestamp for the state of the ChunkedGraph to query. If None, uses the value set at class initialization.

        Returns
        -------
        pd.DataFrame
            A copy of the original DataFrame with each of the point columns replaced by lookups.
        """

        if voxel_resolution is None:
            voxel_resolution = self.voxel_resolution
        if isinstance(point_column, str):
            point_column = [point_column]
        if timestamp is None:
            timestamp = self._timestamp
        if mip is None:
            mip = self._mip

        data = data.copy()
        new_col_order = []
        for col in data.columns:
            if col in point_column:
                rids, sids = self.lookup_points(
                    data[col], voxel_resolution=voxel_resolution, timestamp=timestamp, mip=mip)

                new_position_column = f"{col}{position_suffix}"
                data.rename(columns={col: new_position_column}, inplace=True)
                new_col_order.append(new_position_column)

                new_root_id_column = f"{col}{root_id_suffix}"
                if self._chunkedgraph_client is None:
                    data[new_root_id_column] = sids
                    new_col_order.append(new_root_id_column)
                else:
                    new_supervoxel_column = f"{col}{supervoxel_suffix}"
                    data[new_supervoxel_column] = sids
                    data[new_root_id_column] = rids
                    new_col_order.append(new_supervoxel_column)
                    new_col_order.append(new_root_id_column)
            else:
                new_col_order.append(col)
        return data[new_col_order]
