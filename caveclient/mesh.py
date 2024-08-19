try:
    import trimesh

    TRIMESH_ALLOWED = True
except ImportError:
    TRIMESH_ALLOWED = False

from numbers import Integral
from pathlib import Path
from typing import Union, Optional, Literal
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import numpy as np
import os


def _convert_to_trimesh(mesh):
    if TRIMESH_ALLOWED:
        return trimesh.Trimesh(
            vertices=mesh.vertices,
            faces=mesh.faces,
        )
    else:
        raise ValueError(
            'Could not import python package "trimesh". Either keep as_trimesh=False or install with `pip install trimesh`. See https://trimesh.org.'
        )


class MeshClient(object):
    def __init__(
        self,
        over_client=None,
    ):
        self._cv = None
        self._fc = over_client

    @property
    def cv(self):
        if self._cv is None:
            self._build_cv()
        return self._cv

    def _build_cv(self):
        self._cv = self._fc.info.segmentation_cloudvolume()

    def _get_meshes(self, root_ids, progress):
        curr_prog = self.cv.progress is True
        self._cv.progress = progress
        meshes = self.cv.mesh.get(root_ids, allow_missing=False)
        self._cv.progress = curr_prog
        return meshes

    def get_mesh(
        self,
        root_id: int,
        progress: bool = False,
        as_trimesh: bool = False,
    ):
        """Get single mesh from root id

        Parameters
        ----------
        root_id : int
            Root ID for a neuron
        progress : bool, optional
            If True, use progress bar, by default True
        as_trimesh : bool, optional
            If True, converts to a trimesh Trimesh object, by default False

        Returns
        -------
        Mesh
            Mesh
        """
        if not isinstance(root_id, Integral):
            raise ValueError("This function takes only one root id")

        mesh = self._get_meshes(root_id, progress)[root_id]
        if as_trimesh:
            return _convert_to_trimesh(mesh)
        else:
            return mesh

    def get_meshes(
        self,
        root_ids: list,
        progress: bool = True,
        as_trimesh: bool = False,
    ):
        """Get multiple meshes from root ids.

        Parameters
        ----------
        root_ids : list
            List of root ids
        progress : bool, optional
            If True, use progress bar, by default True
        as_trimesh : bool, optional
            If True, converts each mesh to a trimesh Trimesh object, by default False
        """
        meshes = self._get_meshes(root_ids, progress)
        if as_trimesh:
            return {
                root_id: _convert_to_trimesh(meshes[root_id]) for root_id in root_ids
            }
        else:
            return meshes

    def _save_meshes_to_location(self, root_ids, location, format):
        mdict = self.get_meshes(root_ids, as_trimesh=True, progress=False)
        for rid, msh in mdict.items():
            msh.export(location / f"{rid}.{format}")
        return

    def download_meshes(
        self,
        root_ids: list,
        location: Optional[Union[str, Path]] = None,
        format: Literal["stl", "ply", "glb"] = "glb",
        n_processes=-1,
        meshes_per_block=2,
    ):
        """Download meshes to a file directory

        Parameters
        ----------
        root_ids : list
            List of root ids to download
        location : Optional[Union[str, Path]], optional
            Name of target directory or , by default None
        format : Literal["stl", "ply", "glb"], optional
            Mesh file format readable by Trimesh, by default "glb".
        n_processes : int, optional
            Number of parallel download processes. If -1, use all available cpus. By default -1.
        meshes_per_block : int, optional
            Number of meshes to download per block, by default 2.
        """
        if isinstance(location, str):
            location = Path(location)

        if not location.exists():
            os.makedirs(location)
        if n_processes == -1:
            n_processes = os.cpu_count()

        exe = ProcessPoolExecutor(max_workers=n_processes)
        root_id_list = np.array_split(root_ids, len(root_ids) // meshes_per_block)

        dl_func = partial(
            self._save_meshes_to_location, location=location, format=format
        )
        list(exe.map(dl_func, root_id_list))
