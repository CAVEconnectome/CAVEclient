import cloudvolume as cv
import numpy as np
import tqdm
import fastremap
from scipy import ndimage
from annotationframeworkclient.chunkedgraph import ChunkedGraphClient
from annotationframeworkclient import infoservice
from annotationframeworkclient.endpoints import default_server_address


class ImageryClient(object):
    def __init__(self, image_source=None, segmentation_source=None,
                 server_address=None, dataset_name=None, base_resolution=[4, 4, 40],
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
                self._segmentation_source = self.info.flat_segmentation_source(
                    format_for='cloudvolume')
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

    def _scale_nm_to_voxel(self, xyz_nm, mip, use_cv):
        if use_cv == 'image':
            cv = self.image_cv
        elif use_cv == 'segmentation':
            cv = self.segmentation_cv
        voxel_per_nm = np.array(1/cv.mip_resolution(mip))
        return (xyz_nm * voxel_per_nm).astype(int).tolist()

    def _scale_voxel_to_nm(self, xyz_voxel, nm_per_voxel):
        return np.array(xyz_voxel) * nm_per_voxel

    def _rescale_for_mip(self, bounds, mip, use_cv):
        bounds_nm = self._scale_voxel_to_nm(bounds, self.base_resolution)
        bounds_mip_voxel = self._scale_nm_to_voxel(bounds_nm, mip, use_cv)
        return bounds_mip_voxel

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
        xslice, yslice, zslice = (slice(lbounds[ii], ubounds[ii]) if ubounds[ii] > lbounds[ii]+1
                                  else lbounds[ii] for ii in range(3))
        return xslice, yslice, zslice

    def image_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, mip=None):
        if mip is None:
            mip = self._base_imagery_mip
        bounds = self._process_bounds(bounds, center, width, height, depth)
        bounds_vx = self._rescale_for_mip(bounds, mip, use_cv='image')
        slices = self._bounds_to_slices(bounds_vx)
        return self.image_cv.download(slices, mip=mip)

    def split_segmentation_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, root_ids='all', mip=None, split_by_root_id=False):
        seg_img = self.segmentation_cutout(
            bounds=bounds, center=center, width=width, height=height, depth=depth, root_ids=root_ids, mip=mip)
        split_segmentation = {}
        for root_id in np.unique(seg_img):
            split_segmentation[root_id] = (seg_img == root_id).astype(int)
        return split_segmentation

    def segmentation_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, root_ids='all', mip=None):
        if mip is None:
            mip = self._base_segmentation_mip

        bounds = self._process_bounds(bounds, center, width, height, depth)
        pcg_bounds = self._rescale_for_mip(bounds, 0, use_cv='segmentation')

        bounds_vx = self._rescale_for_mip(bounds, mip, use_cv='segmentation')
        slices = self._bounds_to_slices(bounds_vx)
        if root_ids == 'all':
            seg_cutout = self.segmentation_cv.download(slices, segids=None, mip=mip)
            root_id_map = self._all_root_id_map_for_cutout(
                seg_cutout, pcg_bounds=pcg_bounds, mip=mip)
            return fastremap.remap(seg_cutout, root_id_map, preserve_missing_labels=True)
        else:
            return self.segmentation_cv.download(slices, segids=root_ids, mip=mip)

    def _all_root_id_map_for_cutout(self, seg_cutout, pcg_bounds=None, mip=None):
        sv_to_root_id = {}

        supervoxel_ids = np.unique(seg_cutout)
        pbar = tqdm.tqdm(desc='Finding root ids', total=len(supervoxel_ids), unit='supervoxel')

        inds_to_update = supervoxel_ids == 0
        pbar.update(sum(inds_to_update))

        while np.any(supervoxel_ids > 0):
            # Get the first remaining supervoxel id, find its root id and peer supervoxel ids
            sv_id_base = supervoxel_ids[supervoxel_ids > 0][0]
            root_id = int(self.pcg_client.get_root_id(int(sv_id_base)))
            sv_ids_for_root = self.pcg_client.get_leaves(
                root_id, bounds=np.array(pcg_bounds).T.tolist())
            inds_to_update = np.isin(supervoxel_ids, sv_ids_for_root)
            for sv_id in supervoxel_ids[inds_to_update]:
                sv_to_root_id[sv_id] = root_id
            supervoxel_ids[inds_to_update] = 0
            pbar.update(sum(inds_to_update))
        return sv_to_root_id

    def image_and_segmentation_cutout(self, bounds=None, center=None, width=None, height=None, depth=None, image_mip=None, segmentation_mip=None, root_ids='all', resize=True, split_segmentations=False):

        bounds = self._process_bounds(bounds, center, width, height, depth)

        if image_mip is None:
            image_mip = self._base_imagery_mip
        if segmentation_mip is None:
            segmentation_mip = self._base_segmentation_mip

        img_resolution = self.image_cv.mip_resolution(image_mip)
        seg_resolution = self.segmentation_cv.mip_resolution(segmentation_mip)
        if np.all(img_resolution == seg_resolution):
            zoom_to = None
        if np.all(img_resolution >= seg_resolution):
            zoom_to = 'segmentation'
        else:
            zoom_to = 'image'

        print('Downloading images')
        img = self.image_cutout(bounds=bounds,
                                mip=image_mip)
        img_shape = img.shape

        print('Downloading segmentation')
        if split_segmentations is False:
            seg = self.segmentation_cutout(bounds,
                                           root_ids=root_ids,
                                           mip=segmentation_mip)
            seg_shape = seg.shape
        else:
            seg = self.split_segmentation_cutout(bounds,
                                                 root_ids=root_ids,
                                                 mip=segmentation_mip)
            if len(seg) > 0:
                seg_shape = seg[list(seg.keys())[0]].shape
            else:
                seg_shape = 1

        if zoom_to is None:
            pass
        elif zoom_to == 'segmentation':
            zoom_scale = np.array(seg_shape) / np.array(img_shape)
            img = ndimage.zoom(img, zoom_scale, mode='nearest', order=0)
        elif zoom_to == 'image':
            zoom_scale = np.array(img_shape) / np.array(seg_shape)
            if split_segmentations is False:
                seg = ndimage.zoom(seg, zoom_scale, mode='nearest', order=0)
            else:
                for root_id, seg_cutout in seg.items():
                    seg[root_id] = ndimage.zoom(seg_cutout, zoom_scale, mode='nearest', order=0)

        return img, seg

    def image_resolution_to_mip(self, resolution):
        resolution = tuple(resolution)
        image_dict, _ = self.mip_resolutions()
        return image_dict[resolution]

    def segmentation_resolution_to_mip(self, resolution):
        resolution = tuple(resolution)
        _, seg_dict = self.mip_resolutions()
        return seg_dict[resolution]

    def mip_resolutions(self):
        image_resolution = {}
        for mip in self.image_cv.available_mips:
            image_resolution[tuple(self.image_cv.mip_resolution(mip))] = mip

        segmentation_resolution = {}
        for mip in self.segmentation_cv.available_mips:
            segmentation_resolution[tuple(self.segmentation_cv.mip_resolution(mip))] = mip
        return image_resolution, segmentation_resolution
