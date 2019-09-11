import cloudvolume as cv
import numpy as np
from annotationframeworkclient.chunkedgraph import ChunkedGraphClient
from annotationframeworkclient import infoservice
from annotationframeworkclient.endpoints import default_server_address

class ImageryClient(object):
    def __init__(self, image_source=None, segmentation_source=None,
                 server_address=None, dataset_name=None, base_resolution=[4,4,40],
                 chunked_graph_server_address=None, chunked_segmentation=True,
                 table_name=None, image_mip=0, segmentation_mip=0,
                 segmentation=True, imagery=True):

        self._info = None
        if server_address is None:
            self._server_address = default_server_address
        self._dataset_name = dataset_name
        self._table_name = table_name
        self._chunked_segmentation = chunked_segmentation
        self._base_resolution = np.array(base_resolution)
        
        self._base_imagery_mip = image_mip
        self._base_segmentation_mip = segmentation_mip
        self._image_source = image_source
        self._segmentation_source = segmentation_source

        self._use_segmentation = segmentation
        self._use_imagery = imagery
        self._img_cv = None
        self._seg_cv = None
        self._pcg_client = None
    
    @property
    def base_resolution(self):
        return self._base_resolution

    @property
    def info(self):
        if self._info is None:
            self._info = infoservice.InfoServiceClient(self._server_address,
                                                       self._dataset_name)
        return self._info

    @property
    def image_source(self):
        if self._image_source is None:
            self._image_source = self.info.image_source(format_for='cloudvolume')
        return self._image_source

    @property
    def image_cv(self):
        if self._use_imagery is False:
            return None
        
        if self._img_cv is None:
            self._img_cv = cv.CloudVolume(self.image_source,
                                          mip=self._base_imagery_mip,
                                          bounded=False)
        return self._img_cv

    @property
    def segmentation_source(self):
        if self._use_segmentation is False:
            return None
        elif self._segmentation_source is None:
            if self._chunked_segmentation:
                self._segmentation_source = self.info.pychunkgraph_segmentation_source(
                    format_for='neuroglancer_pcg')
            else:
                self._segmentation_source = self.info.flat_segmentation_source(format_for='cloudvolume')
        return self._segmentation_source
        
    @property
    def segmentation_cv(self):
        if self._use_segmentation is False:
            return None
        elif self._seg_cv is None:
            self._seg_cv = cv.CloudVolume(self.segmentation_source,
                                          mip=self._base_segmentation_mip,
                                          use_https=True,
                                          bounded=False)
        return self._seg_cv
    
    @property
    def pcg_client(self):
        if self._use_segmentation is False:
            return None
        elif self._pcg_client is None:
            self._pcg_client = ChunkedGraphClient(server_address=self._server_address,
                                                  dataset_name=self._dataset_name,
                                                  table_name=self._table_name)
        return self._pcg_client

        
    def _rescale_for_mip(self, bounds, mip, use_cv):
        if use_cv == 'image':
            cv = self.image_cv
        elif use_cv == 'segmentation':
            cv = self.segmentation_cv
        scaling = self._base_resolution / np.array(cv.mip_resolution(mip))
        return np.floor(bounds * scaling).astype(int).tolist()

    def _process_bounds(self, bounds, center, width, height, depth):
        if bounds is None:
            if width is None:
                width = 0
            if height is None:
                height = 0
            if depth is None:
                depth = 0

            lbound = np.array(center) - np.floor(np.array([width, height, depth])/2)
            ubound = lbound + np.array(width, height, depth).astype(int)
            bounds = [lbound, ubound]
        return bounds
 
    def _bounds_to_slices(self, bounds):
        lbounds = np.min(bounds, axis=0)
        ubounds = np.max(bounds, axis=0)+1
        xslice, yslice, zslice = (slice(lbounds[ii], ubounds[ii]) if ubounds[ii] > lbounds[ii]+1 \
                                    else lbounds[ii] for ii in range(3))
        return xslice, yslice, zslice

    def image_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, mip=None):
        if mip is None:
            mip = self._base_imagery_mip
        bounds = self._process_bounds(bounds, center, width, height, depth)
        bounds = self._rescale_for_mip(bounds, mip, use_cv='image')
        slices = self._bounds_to_slices(bounds)
        return self.image_cv.download(slices, mip=mip)

    def segmentation_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, root_ids=None, mip=None):
        if mip is None:
            mip = self._base_segmentation_mip

        bounds = self._process_bounds(bounds, center, width, height, depth)
        bounds = self._rescale_for_mip(bounds, mip, use_cv='segmentation')
        slices = self._bounds_to_slices(bounds)
        return self.segmentation_cv.download(slices, segids=root_ids, mip=mip)


    def _get_root_ids_for_cutout(self, seg_cutout):
        supervoxel_ids = np.unique(seg_cutout)
        self.pcg_client.get_root_id(supervoxel_ids)