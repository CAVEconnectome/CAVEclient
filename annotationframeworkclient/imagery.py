import fastremap
import numpy as np
import tqdm
import imageio
from scipy import ndimage
import cloudvolume as cv
from annotationframeworkclient import infoservice
import cloudvolume as cv
from annotationframeworkclient.chunkedgraph import ChunkedGraphClient
from annotationframeworkclient.endpoints import default_server_address

class DimensionException(Exception):
    pass

class ImageryClient(object):
    """Class to help download imagery and segmentation data. Can either take
       explicit cloudvolume paths for imagery and segmentation or use the Info Service
       to look up the right paths.

        Parameters
        ----------
        image_source : str, optional
            CloudVolume path to an imagery source, by default None
        segmentation_source : str, optional
            CloudVolume path to a segmentation source, by default None
        server_address : str, optional
            Address of an Info Service host, by default None. If none, defaults to
            https://www.dynamicannotationframework.com
        dataset_name : str, optional
            Dataset name to lookup information for in the Info Service, by default None
        base_resolution : list, optional
            Sets the voxel resolution that locations will be entered in, by default [4, 4, 40]
        chunked_graph_server_address : str, optional
            Location of a pychunkgraph server, by default None
        chunked_segmentation : bool, optional
            If true, use the chunkedgraph segmentation. If false, use the flat segmentation. By default True.
        table_name : str, optional
            Name of the chunkedgraph table (if used), by default None
        image_mip : int, optional
            Default mip level to use for imagery lookups, by default 0. Note that the same mip
            level for imagery and segmentation can correspond to different voxel resolutions.
        segmentation_mip : int, optional
            Default mip level to use for segmentation lookups, by default 0.
        segmentation : bool, optional
            If False, no segmentation cloudvolume is initialized. By default True
        imagery : bool, optional
            If False, no imagery cloudvolume is initialized. By default True
        """
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
        """The voxel resolution assumed when locations are used for the client.
        
        Returns
        -------
        list
            X, y, and z voxel resolution.
        """
        return self._base_resolution

    @property
    def info(self):
        """InfoClient for the imagery dataset (if set)
        """
        if self._server_address is None or self._dataset_name is None:
            return None
        
        if self._info is None:
            self._info = infoservice.InfoServiceClient(self._server_address,
                                                       self._dataset_name)
        return self._info

    @property
    def image_source(self):
        """Image Cloudvolume path
        """
        if self._image_source is None and self.info is not None:
            self._image_source = self.info.image_source(format_for='cloudvolume')
        return self._image_source

    @property
    def image_cv(self):
        """Imagery CloudVolume
        """
        if self._use_imagery is False:
            return None

        if self._img_cv is None:
            self._img_cv = cv.CloudVolume(self.image_source,
                                          mip=self._base_imagery_mip,
                                          bounded=False)
        return self._img_cv

    @property
    def segmentation_source(self):
        """Segmentation CloudVolume path
        """
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
        """Segmentation CloudVolume object
        """
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
        """PychunkedGraph client object
        """
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

    def _bounds_to_slices(self, bounds):
        lbounds = np.min(bounds, axis=0)
        ubounds = np.max(bounds, axis=0)+1
        xslice, yslice, zslice = (slice(lbounds[ii], ubounds[ii]) if ubounds[ii] > lbounds[ii]+1
                                  else lbounds[ii] for ii in range(3))
        return xslice, yslice, zslice

    def image_cutout(self, bounds, mip=None):
        """Get an image cutout for a certain location or set of bounds and a mip level.
        
        Parameters
        ----------
        bounds : 2 x 3 list of ints
            A list of a lower bound and upper bound point to bound the cutout in units of voxels in a resolution set by
            the base_resolution parameter
        mip : int, optional
            [description], by default None
        
        Returns
        -------
        [type]
            [description]
        """
        if mip is None:
            mip = self._base_imagery_mip
        bounds_vx = self._rescale_for_mip(bounds, mip, use_cv='image')
        slices = self._bounds_to_slices(bounds_vx)
        return self.image_cv.download(slices, mip=mip)

    def split_segmentation_cutout(self, bounds, root_ids='all', mip=None, include_null_root=False):
        """Generate segmentation cutouts with a single binary mask for each root id, organized as a dict with keys as root ids and masks as values.
        
        Parameters
        ----------
        bounds : [type], optional
            [description], by default None
        root_ids : str, optional
            [description], by default 'all'
        mip : [type], optional
            [description], by default None
        split_by_root_id : bool, optional
            [description], by default False
        
        Returns
        -------
        [type]
            [description]
        """
        seg_img = self.segmentation_cutout(bounds=bounds, root_ids=root_ids, mip=mip)
        split_segmentation = {}
        for root_id in np.unique(seg_img):
            if include_null_root is False:
                if root_id == 0:
                    continue
            split_segmentation[root_id] = (seg_img == root_id).astype(int)
        return split_segmentation

    def segmentation_cutout(self, bounds, root_ids='all', mip=None):
        if mip is None:
            mip = self._base_segmentation_mip

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

    def image_and_segmentation_cutout(self, bounds, image_mip=None, segmentation_mip=None, root_ids='all', allow_resize=True, split_segmentations=False, include_null_root=False):
        """[summary]
        
        Parameters
        ----------
        bounds : [type]
            [description]
        image_mip : [type], optional
            [description], by default None
        segmentation_mip : [type], optional
            [description], by default None
        root_ids : str, optional
            [description], by default 'all'
        allow_resize : bool, optional
            [description], by default True
        split_segmentations : bool, optional
            [description], by default False
        
        Returns
        -------
        [type]
            [description]
        
        Raises
        ------
        DimensionException
            [description]
        """
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

        if allow_resize is False:
            if zoom_to is not None:
                raise DimensionException('Segmentation and imagery are not the same size base image')

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
                                                 mip=segmentation_mip,
                                                 include_null_root=include_null_root)
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
        """[summary]
        
        Parameters
        ----------
        resolution : [type]
            [description]
        
        Returns
        -------
        [type]
            [description]
        """
        resolution = tuple(resolution)
        image_dict, _ = self.mip_resolutions()
        return image_dict.get(resolution, None)

    def segmentation_resolution_to_mip(self, resolution):
        resolution = tuple(resolution)
        _, seg_dict = self.mip_resolutions()
        return seg_dict.get(resolution, None)

    def mip_resolutions(self):
        image_resolution = {}
        for mip in self.image_cv.available_mips:
            image_resolution[tuple(self.image_cv.mip_resolution(mip))] = mip

        segmentation_resolution = {}
        for mip in self.segmentation_cv.available_mips:
            segmentation_resolution[tuple(self.segmentation_cv.mip_resolution(mip))] = mip
        return image_resolution, segmentation_resolution

    def save_imagery(self, filename_prefix, bounds=None, mip=None, precomputed_image=None, slice_axis=2, verbose=False, **kwargs):
        """Save queried or precomputed imagery
        """
        if precomputed_image is None:
            img = self.image_cutout(bounds, mip)
        else:
            img = precomputed_image
        _save_image_slices(filename_prefix, 'imagery', img, slice_axis, 'imagery', verbose=verbose, **kwargs)
        return 

    def save_segmentation_masks(self, filename_prefix, bounds=None, mip=None, root_ids='all', precomputed_segmentation=None, slice_axis=2, verbose=False, **kwargs):
        '''Save queried or precomputed segmentation masks
        '''
        if precomputed_segmentation is None:
            seg_dict = self.segmentation_cutout(bounds=bounds, root_ids=root_ids, mip=None, split_segmentation=True, include_null_root=False)
        else:
            seg_dict = precomputed_segmentation
        
        for root_id, seg_mask in seg_dict.items():
            suffix = f'root_id_{root_id}'
            _save_image_slices(filename_prefix, suffix, seg_mask, slice_axis, 'mask', verbose=verbose, **kwargs)
        return


    def save_image_and_segmentation_masks(self, filename_prefix, bounds=None, image_mip=None, segmentation_mip=None,
                                          root_ids='all', allow_resize=True, imagery=True, segmentation=True,
                                          precomputed_images=None, slice_axis=2, verbose=False, **kwargs):
        """Download and save cutouts plus masks to a stack of files. 
        """
        if precomputed_images is not None:
            img, seg_dict = precomputed_images
        else:
            img, seg_dict = self.image_and_segmentation_cutout(bounds, image_mip=image_mip, segmentation_mip=segmentation_mip, root_ids=root_ids, allow_resize=allow_resize, split_segmentations=True)
        
        if imagery:
            self.save_imagery(filename_prefix, precomputed_image=img, slice_axis=slice_axis, verbose=verbose, **kwargs) 
        if segmentation:
            self.save_segmentation_masks(filename_prefix, precomputed_segmentation=seg_dict, slice_axis=slice_axis, verbose=verbose, **kwargs)
        return

def _save_image_slices(filename_prefix, filename_suffix, img, slice_axis, image_type, verbose=False, **kwargs):
    if image_type == 'imagery':
        to_pil = _greyscale_to_pil
    elif image_type == 'mask':
        to_pil = _binary_mask_to_transparent_pil

    imgs = np.split(img, img.shape[slice_axis], axis=slice_axis)
    if len(imgs) == 1:
        fname = f'{filename_prefix}_{filename_suffix}.png'
        imageio.imwrite(fname, to_pil(imgs[0].squeeze()), **kwargs)
        if verbose:
            print(f'Saved {fname}...')
    else:
        for ii, img_slice in enumerate(imgs):
            fname = f'{filename_prefix}_{filename_suffix}_{ii}.png'
            imageio.imwrite(fname, to_pil(img_slice.squeeze()), **kwargs)
            if verbose:
                print(f'Saved {fname}...')
    return

def _greyscale_to_pil(img):
    img = img.astype(np.uint8)
    pil_img = np.dstack(3*[img.squeeze()[:, :, np.newaxis]])
    return pil_img

def _binary_mask_to_transparent_pil(img):
    """Convert a MxN binary array to an MxNx4 PIL image with fully opaque white for 1 and fully transparent black for 0.
    """
    img = 255 * img.astype(np.uint8)
    pil_img = np.dstack(4*[img.squeeze()[:, :, np.newaxis]])
    return pil_img
