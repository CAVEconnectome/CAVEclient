import fastremap
import numpy as np
import tqdm
import imageio
import cloudvolume as cv
from scipy import ndimage
from functools import partial
from annotationframeworkclient import infoservice
from annotationframeworkclient.chunkedgraph import ChunkedGraphClient
from annotationframeworkclient.endpoints import default_server_address

class DimensionException(Exception):
    """Raised when image dimensions don't match"""
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
            Address of an InfoService host, by default None. If none, defaults to
            https://www.dynamicannotationframework.com
        dataset_name : str, optional
            Dataset name to lookup information for in the InfoService, by default None
        base_resolution : list, optional
            Sets the voxel resolution that bounds will be entered in, by default [4, 4, 40].
        graphene_segmentation : bool, optional
            If true, use the graphene segmentation. If false, use the flat segmentation. By default True.
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
    def __init__(self, image_source=None, segmentation_source=None, server_address=None,
                 dataset_name=None, base_resolution=[4, 4, 40],
                 graphene_segmentation=True, table_name=None,
                 image_mip=0, segmentation_mip=0,
                 segmentation=True, imagery=True):
        self._info = None
        if server_address is None:
            self._server_address = default_server_address
        self._dataset_name = dataset_name
        self._table_name = table_name
        self._chunked_segmentation = graphene_segmentation
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
                                          bounded=False,
                                          progress=False)
        return self._img_cv

    @property
    def segmentation_source(self):
        """Segmentation CloudVolume path
        """
        if self._use_segmentation is False:
            return None
        elif self._segmentation_source is None:
            if self._chunked_segmentation:
                self._segmentation_source = self.info.graphene_source()
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
                                          bounded=False,
                                          progress=False)
        return self._seg_cv

    @property
    def pcg_client(self):
        """PychunkedGraph client object
        """
        if self._use_segmentation is False or if self._chunked_segmentation is False:
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
            Mip level of imagery to get if something other than the default is wanted, by default None
        
        Returns
        -------
        cloudvolume.VolumeCutout
            An n-d image of the image requested with image intensity values as the elements.
        """
        if mip is None:
            mip = self._base_imagery_mip
        bounds_vx = self._rescale_for_mip(bounds, mip, use_cv='image')
        slices = self._bounds_to_slices(bounds_vx)
        return self.image_cv.download(slices, mip=mip)


    def segmentation_cutout(self, bounds, root_ids='all', mip=None, verbose=False):
        """Get a cutout of the segmentation imagery for some or all root ids between set bounds.
        Note that if all root ids are requested in a large region, it could take a long time to query
        all supervoxels.
        
        Parameters
        ----------
        bounds : 2x3 list of ints
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter.
        root_ids : list, None, or 'all', optional
            If a list, only compute the voxels for a specified set of root ids. If None, default to the supervoxel ids. If 'all',
            find all root ids corresponding to the supervoxels in the cutout and get all of them. None, by default 'all'
        mip : int, optional
            Mip level of the segmentation if something other than the defualt is wanted, by default None
        verbose : bool, optional
            If true, prints statements about the progress as it goes. By default, False.
        
        Returns
        -------
        numpy.ndarray 
            Array whose elements correspond to the root id (or, if root_ids=None, the supervoxel id) at each voxel.
        """
        if mip is None:
            mip = self._base_segmentation_mip

        pcg_bounds = self._rescale_for_mip(bounds, 0, use_cv='segmentation')

        bounds_vx = self._rescale_for_mip(bounds, mip, use_cv='segmentation')
        slices = self._bounds_to_slices(bounds_vx)
        if root_ids == 'all':
            seg_cutout = self.segmentation_cv.download(slices, segids=None, mip=mip)
            if self._chunked_segmentation:
                root_id_map = self._all_root_id_map_for_cutout(
                    seg_cutout, pcg_bounds=pcg_bounds, mip=mip, verbose=verbose)
                return fastremap.remap(seg_cutout, root_id_map, preserve_missing_labels=True)
            else:
                return seg_cutout
        else:
            return self.segmentation_cv.download(slices, segids=root_ids, mip=mip)

    def split_segmentation_cutout(self, bounds, root_ids='all', mip=None, include_null_root=False, verbose=False):
        """Generate segmentation cutouts with a single binary mask for each root id, organized as a dict with keys as root ids and masks as values.
        
        Parameters
        ----------
        bounds : 2x3 list of ints
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter.
        root_ids : list, None, or 'all', optional
            If a list, only compute the voxels for a specified set of root ids. If None, default to the supervoxel ids. If 'all',
            find all root ids corresponding to the supervoxels in the cutout and get all of them. None, by default 'all'
        mip : int, optional
            Mip level of the segmentation if something other than the default is wanted, by default None
        include_null_root : bool, optional
            If True, includes root id of 0, which is usually reserved for a null segmentation value. Default is False.
        verbose : bool, optional
            If true, prints statements about the progress as it goes. By default, False.

        Returns
        -------
        dict
            Dict whose keys are root ids and whose values are the binary mask for that root id, with a 1 where the object contains the voxel.
        """
        seg_img = self.segmentation_cutout(bounds=bounds, root_ids=root_ids, mip=mip, verbose=verbose)
        return self.segmentation_masks(seg_img, include_null_root)

    def segmentation_masks(self, seg_img, include_null_root=False):
        """Convert a segmentation array into a dict of binary masks for each root id.
        
        Parameters
        ----------
        seg_img : numpy.ndarray
            Array with voxel values corresponding to the object id at that voxel
        include_null_root : bool, optional
            Create a mask for 0 id, which usually denotes no object, by default False
        
        Returns
        -------
        dict
            Dict of binary masks. Keys are root ids, values are boolean n-d arrays with a 1 where that object is.
        """
        split_segmentation = {}
        for root_id in np.unique(seg_img):
            if include_null_root is False:
                if root_id == 0:
                    continue
            split_segmentation[root_id] = (seg_img == root_id).astype(int)
        return split_segmentation 

    def _all_root_id_map_for_cutout(self, seg_cutout, pcg_bounds=None, mip=None, verbose=False):
        """Helper function to query root ids for all supervoxels in a cutout
        """
        sv_to_root_id = {}

        supervoxel_ids = np.unique(seg_cutout)
        if verbose:
            pbar = tqdm.tqdm(desc='Finding root ids', total=len(supervoxel_ids), unit='supervoxel')
        else:
            pbar = None
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
            if pbar is not None:
                pbar.update(sum(inds_to_update))
        return sv_to_root_id

    def image_and_segmentation_cutout(self, bounds, image_mip=None, segmentation_mip=None, root_ids='all', allow_resize=True, split_segmentations=False, include_null_root=False, verbose=False):
        """Download aligned and scaled imagery and segmentation data at a given resolution.
        
        Parameters
        ----------
        bounds : 2x3 list of ints
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter.
        image_mip : int, optional
            Mip level of the imagery if something other than the default is wanted, by default None
        segmentation_mip : int, optional
            Mip level of the segmentation if something other than the default is wanted, by default None
        root_ids : list, None, or 'all', optional
            If a list, the segmentation cutout only includes voxels for a specified set of root ids.
            If None, default to the supervoxel ids. If 'all', finds all root ids corresponding to the supervoxels
            in the cutout and get all of them. By default 'all'.
        allow_resize : bool, optional
            Allow the lower resolution of the imagery and segmentation (typically imagery) to be upscaled to match whichever is higher resolution, by default True.
            If False, returns an error if the resolutions do not match.
        split_segmentations : bool, optional
            If True, the segmentation is returned as a dict of masks (using split_segmentation_cutout), and if False returned as
            an array with root_ids (using segmentation_cutout), by default False
        include_null_root : bool, optional
            If True, includes root id of 0, which is usually reserved for a null segmentation value. Default is False.
        verbose : bool, optional
            If true, prints statements about the progress as it goes. By default, False.
 
        Returns
        -------
        cloudvolume.VolumeCutout 
            Imagery volume cutout
        
        numpy.ndarray or dict
            Segmentation volume cutout as either an ndarray or dict of masks depending on the split_segmentations flag.
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
        
        if verbose:
            print('Downloading images')
        img = self.image_cutout(bounds=bounds,
                                mip=image_mip)
        img_shape = img.shape

        if verbose:
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
        """Gets the image mip level for a specified voxel resolution, if it exists
        
        Parameters
        ----------
        resolution : 3 element array-like
            x, y, and z resolution per voxel.
        
        Returns
        -------
        int or None
            If the resolution has a mip level for the imagery volume, returns it. Otherwise, returns None.
        """
        resolution = tuple(resolution)
        image_dict, _ = self.mip_resolutions()
        return image_dict.get(resolution, None)

    def segmentation_resolution_to_mip(self, resolution):
        """Gets the segmentation mip level for a specified voxel resolution, if it exists

        Parameters
        ----------
        resolution : 3 element array-like
            x, y, and z resolution per voxel.

        Returns
        -------
        int or None
            If the resolution has a mip level for the segmentation volume, returns it. Otherwise, returns None.
        """
        resolution = tuple(resolution)
        _, seg_dict = self.mip_resolutions()
        return seg_dict.get(resolution, None)

    def mip_resolutions(self):
        """Gets a dict of resolutions and mip levels available for the imagery and segmentation volumes
        
        Returns
        -------
        dict
            Keys are voxel resolution tuples, values are mip levels in the imagery volume as integers
        
        dict
            Keys are voxel resolution tuples, values are mip levels in the segmentation volume as integers
        """
        image_resolution = {}
        for mip in self.image_cv.available_mips:
            image_resolution[tuple(self.image_cv.mip_resolution(mip))] = mip

        segmentation_resolution = {}
        for mip in self.segmentation_cv.available_mips:
            segmentation_resolution[tuple(self.segmentation_cv.mip_resolution(mip))] = mip
        return image_resolution, segmentation_resolution

    def save_imagery(self, filename_prefix, bounds=None, mip=None, precomputed_image=None, slice_axis=2, verbose=False, **kwargs):
        """Save queried or precomputed imagery to png files.
        
        Parameters
        ----------
        filename_prefix : str
            Prefix for the imagery filename. The full filename will be {filename_prefix}_imagery.png
        bounds : 2x3 list of ints, optional
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter. Only used if a precomputed image is not passed. By default, None. 
        mip : int, optional
            Only used if a precomputed image is not passed. Mip level of imagery to get if something other than the default
            is wanted, by default None
        precomputed_image : cloudvolume.VolumeCutout, optional
            Already downloaded VolumeCutout data to save explicitly. If called this way, the bounds and mip arguments will not apply.
            If a precomputed image is not provided, bounds must be specified to download the cutout data. By default None
        slice_axis : int, optional
            If the image data is truly 3 dimensional, determines which axis to use to save serial images, by default 2 (i.e. z-axis)
        verbose : bool, optional
            If True, prints the progress, by default False
        """
        if precomputed_image is None:
            img = self.image_cutout(bounds, mip)
        else:
            img = precomputed_image
        _save_image_slices(filename_prefix, 'imagery', img, slice_axis, 'imagery', verbose=verbose, **kwargs)
        return 

    def save_segmentation_masks(self, filename_prefix, bounds=None, mip=None, root_ids='all', precomputed_masks=None, slice_axis=2, include_null_root=False, segmentation_colormap={}, verbose=False, **kwargs):
        """Save queried or precomputed segmentation masks to png files. Additional kwargs are passed to imageio.imwrite.
        
        Parameters
        ----------
        filename_prefix : str
            Prefix for the segmentation filenames. The full filename will be either {filename_prefix}_root_id_{root_id}.png
            or {filename_prefix}_root_id_{root_id}_{i}.png, depending on if multiple slices of each root id are saved.
        bounds : 2x3 list of ints, optional
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter. Only used if a precomputed segmentation is not passed. By default, None. 
        mip : int, optional
            Only used if a precomputed segmentation is not passed. Mip level of segmentation to get if something other than the default
            is wanted, by default None
        root_ids : list, None, or 'all', optional
            If a list, the segmentation cutout only includes voxels for a specified set of root ids.
            If None, default to the supervoxel ids. If 'all', finds all root ids corresponding to the supervoxels
            in the cutout and get all of them. By default 'all'.
        precomputed_masks : dict, optional
            Already downloaded dict of mask data to save explicitly. If called this way, the bounds and mip arguments will not apply.
            If precomputed_masks are not provided, bounds must be given to download cutout data. By default None
        slice_axis : int, optional
            If the image data is truly 3 dimensional, determines which axis to use to save serial images, by default 2 (i.e. z-axis)
        include_null_root : bool, optional
            If True, includes root id of 0, which is usually reserved for a null segmentation value. Default is False.
        segmentation_colormap : dict, optional
            A dict of root ids to an uint8 RGB color triplet (0-255) or RGBa quadrooplet to optionally color the mask png. Any root id not specified
            will be rendered in white. Color triplets default to full opacity. Default is an empty dictionary.
        verbose : bool, optional
            If True, prints the progress, by default False
        """
        if precomputed_masks is None:
            seg_dict = self.split_segmentation_cutout(
                bounds=bounds, root_ids=root_ids, mip=mip, include_null_root=include_null_root)
        else:
            seg_dict = precomputed_masks
        
        for root_id, seg_mask in seg_dict.items():
            suffix = f'root_id_{root_id}'
            _save_image_slices(filename_prefix, suffix, seg_mask, slice_axis, 'mask',
                               color=segmentation_colormap.get(root_id, None), verbose=verbose, **kwargs)
        return


    def save_image_and_segmentation_masks(self, filename_prefix, bounds=None, image_mip=None, segmentation_mip=None,
                                          root_ids='all', allow_resize=True, precomputed_data=None, slice_axis=2,
                                          segmentation_colormap={}, include_null_root=False, verbose=False, **kwargs):
        """Save aligned and scaled imagery and segmentation mask cutouts as pngs. Kwargs are passed to imageio.imwrite.
        
        Parameters
        ----------
        filename_prefix : str
            Prefix for the segmentation filenames. The full filename will be either {filename_prefix}_root_id_{root_id}.png
            or {filename_prefix}_root_id_{root_id}_{i}.png, depending on if multiple slices of each root id are saved.
        bounds : 2x3 list of ints, optional
            A list of the lower and upper bound point for the cutout. The units are voxels in the resolution set by the
            base_resolution parameter. Only used if a precomputed data is not passed. By default, None. 
        image_mip : int, optional
            Only used if a precomputed data is not passed. Mip level of imagery to get if something other than the default
            is wanted, by default None.
        segmentation_mip : int, optional
            Only used if precomputed data is not passed. Mip level of segmentation to get if something other than the default
            is wanted, by default None
        root_ids : list, None, or 'all', optional
            If a list, the segmentation cutout only includes voxels for a specified set of root ids.
            If None, default to the supervoxel ids. If 'all', finds all root ids corresponding to the supervoxels
            in the cutout and get all of them. By default 'all'.
        allow_resize : bool, optional
            If False, throws an error if image and segmentation mips don't have the same voxel resolution. By default, False.
        precomputed_data : tuple, optional
            Already computed tuple with imagery and segmentation mask data, in that order. If not provided, bounds must be given to download
            cutout data. By default, None.
        slice_axis : int, optional
            If the image data is truly 3 dimensional, determines which axis to use to save serial images, by default 2 (i.e. z-axis)
        segmentation_colormap : dict, optional
            A dict of root ids to an uint8 RGB color triplet (0-255) or RGBa quadrooplet to optionally color the mask png. Any root id not specified
            will be rendered in white. Color triplets default to full opacity. Default is an empty dictionary.
        include_null_root : bool, optional
            If True, includes root id of 0, which is usually reserved for a null segmentation value. By default, False.
        verbose : bool, optional
            If True, prints the progress, by default False
        """ 
        if precomputed_data is not None:
            img, seg_dict = precomputed_data
        else:
            img, seg_dict = self.image_and_segmentation_cutout(bounds=bounds, image_mip=image_mip, segmentation_mip=segmentation_mip, root_ids=root_ids, allow_resize=allow_resize, include_null_root=include_null_root, split_segmentations=True)
        
        self.save_imagery(filename_prefix, precomputed_image=img, slice_axis=slice_axis, verbose=verbose, **kwargs) 
        self.save_segmentation_masks(filename_prefix, precomputed_masks=seg_dict, slice_axis=slice_axis,
                                     segmentation_colormap=segmentation_colormap, verbose=verbose, **kwargs)
        return

def _save_image_slices(filename_prefix, filename_suffix, img, slice_axis, image_type, verbose=False, color=None, **kwargs):
    """Helper function for generic image saving
    """
    if image_type == 'imagery':
        to_pil = _grayscale_to_pil
    elif image_type == 'mask':
        to_pil = partial(_binary_mask_to_transparent_pil, color=color)

    imgs = np.split(img, img.shape[slice_axis], axis=slice_axis)
    if len(imgs) == 1:
        fname = f'{filename_prefix}_{filename_suffix}.png'
        imageio.imwrite(fname, to_pil(imgs[0].squeeze()), **kwargs)
        if verbose:
            print(f'Saved {fname}...')
    else:
        for ii, img_slice in enumerate(imgs):
            fname = f'{filename_prefix}_slice_{ii}_{filename_suffix}.png'
            imageio.imwrite(fname, to_pil(img_slice.squeeze()), **kwargs)
            if verbose:
                print(f'Saved {fname}...')
    return

def _grayscale_to_pil(img, four_channel=False):
    """Helper function to convert one channel uint8 image data to RGB for saving.
    """
    img = img.astype(np.uint8).T
    if four_channel is True:
        sc = 4
    else:
        sc = 3
    pil_img = np.dstack(sc*[img.squeeze()[:, :, np.newaxis]])
    return pil_img

def _binary_mask_to_transparent_pil(img, color=None):
    """Convert a binary array to an MxNx4 RGBa image with fully opaque white (or a specified RGBa color)
    for 1 and fully transparent black for 0.
    """
    if color is None:
        color = [255, 255, 255, 255]
    elif len(color) == 3:
        color = [*color, 255]
    base_img = img.astype(np.uint8).T.squeeze()[:, :, np.newaxis]
    img_r = color[0] * base_img
    img_g = color[1] * base_img
    img_b = color[2] * base_img
    img_a = color[3] * base_img 
    pil_img = np.dstack([img_r, img_g, img_b, img_a])
    return pil_img

def grayscale_to_rgba(img):
    """Convert a 
    
    Parameters
    ----------
    img : numpy.ndarray
        NxM array of values beteen 0-255.
    
    Returns
    -------
    numpy.ndarray
        NxMx4 numpy array with the same grayscale colors.
    """
    return _grayscale_to_pil(img)

def colorize_masks(mask_dict, colormap, default_color=[255, 255, 255, 255]):
    """Colorize a dict of masks using a dict of RGB (or RGBa) colors.
    
    Parameters
    ----------
    mask_dict : dict
        Dict mapping root ids to binary masks.
    colormap : dict 
        Dict mapping root ids to a 3 or 4 element RGBa color between 0-255.
    default_color : list, optional
        RGBa color value between 0-255, by default [255, 255, 255, 255]
    
    Returns
    -------
    dict
        Dict mapping root ids to RGBa color images.
    """
    color_images = {}
    for root_id, mask in mask_dict:
        color_images[root_id] = _binary_mask_to_transparent_pil(mask, colormap.get(root_id, default_color))
    return color_images
