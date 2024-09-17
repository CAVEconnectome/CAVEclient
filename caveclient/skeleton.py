import numpy as np
from io import BytesIO
import struct

IDENTITY = np.array(
    [
        [1, 0, 0, 0],
        [0, 1, 0, 0],
        [0, 0, 1, 0],
    ],
    dtype=np.float32,
)


class Skeleton(object):
    """
    A stick figure representation of a 3D object.

    vertices: [[x,y,z], ...] float32
    edges: [[v1,v2], ...] uint32
    segid: numerical ID
    transform: 3x4 scaling and translation matrix (ie homogenous coordinates)
      that represents the transformaton from voxel to physical coordinates.

      Example Identity Matrix:
      [
        [1, 0, 0, 0],
        [0, 1, 0, 0],
        [0, 0, 1, 0]
      ]

    space: 'voxel', 'physical', or user choice (but other choices
      make .physical_space() and .voxel_space() stop working as they
      become meaningless.)

    vertex_attributes: You can specify additional per vertex
      data attributes (the most common are radii and vertex_type)
      that are present in reading Precomputed binary skeletons using
      the following format:
      [
          {
            "id": "radius",
            "data_type": "uint8",
            "num_components": 1,
          }
      ]
    vertex_attribute_values: A dictionary of the vertex attributes
      specified in vertex_attributes. The keys should match the
      'id' field in vertex_attributes and the values should be numpy
      arrays of the same length as the number of vertices.

      These attributes will become object properties. i.e. skel.radius

      Note that for backwards compatibility, skel.radius is treated
      specially and is synonymous with skel.radii.
    """

    def __init__(
        self,
        vertices=None,
        edges=None,
        segid=None,
        transform=None,
        space="voxel",
        vertex_attributes=None,
        vertex_attribute_values=None,
    ):
        self.id = segid
        self.space = space

        if vertices is None:
            self.vertices = np.array([[]], dtype=np.float32).reshape(0, 3)
        elif isinstance(vertices, list):
            self.vertices = np.array(vertices, dtype=np.float32)
        else:
            self.vertices = vertices.astype(np.float32)

        if edges is None:
            self.edges = np.array([[]], dtype=np.uint32).reshape(0, 2)
        elif isinstance(edges, list):
            self.edges = np.array(edges, dtype=np.uint32)
        else:
            self.edges = edges.astype(np.uint32)

        if vertex_attributes is None:
            self.vertex_attributes = None
        else:
            self.vertex_attributes = vertex_attributes
            for attr in vertex_attributes:
                if (
                    attr["num_components"]
                    != vertex_attribute_values[attr["id"]].shape[1]
                ):
                    raise ValueError(
                        "The number of components in the attribute {} ({}) does not match the number of components in the vertex_attribute_values ({})".format(
                            attr["id"],
                            attr["num_components"],
                            vertex_attribute_values[attr["id"]].shape[1],
                        )
                    )
                if attr["data_type"] != vertex_attribute_values[attr["id"]].dtype:
                    raise ValueError(
                        "The data type of the attribute {} ({}) does not match the data type of the vertex_attribute_values ({})".format(
                            attr["id"],
                            attr["data_type"],
                            vertex_attribute_values[attr["id"]].dtype,
                        )
                    )
                setattr(self, attr["id"], vertex_attribute_values[attr["id"]])

        if transform is None:
            self.transform = np.copy(IDENTITY)
        else:
            self.transform = np.array(transform).reshape((3, 4))

    @property
    def transform(self):
        return self._transform

    @transform.setter
    def transform(self, val):
        self._transform = np.array(val, dtype=np.float32).reshape((3, 4))

    def to_precomputed(self):
        edges = self.edges.astype(np.uint32)
        vertices = self.vertices.astype(np.float32)

        result = BytesIO()

        # Write number of positions and edges as first two uint32s
        result.write(struct.pack("<II", vertices.size // 3, edges.size // 2))
        result.write(vertices.tobytes("C"))
        result.write(edges.tobytes("C"))

        def writeattr(attr, dtype, text):
            if attr is None:
                return

            attr = attr.astype(dtype)

            if attr.shape[0] != vertices.shape[0]:
                raise ValueError(
                    "Number of {} {} ({}) must match the number of vertices ({}).".format(
                        dtype, text, attr.shape[0], vertices.shape[0]
                    )
                )

            result.write(attr.tobytes("C"))

        for attr in self.vertex_attributes:
            arr = getattr(self, attr["id"])
            writeattr(arr, np.dtype(attr["data_type"]), attr["id"])

        return result.getvalue()

    @classmethod
    def from_precomputed(kls, skelbuf, segid=None, vertex_attributes=None):
        """
        Convert a buffer into a Skeleton object.

        Format:
        num vertices (Nv) (uint32)
        num edges (Ne) (uint32)
        XYZ x Nv (float32)
        edge x Ne (2x uint32)

        Default Vertex Attributes:

          radii x Nv (optional, float32)
          vertex_type x Nv (optional, req radii, uint8) (SWC definition)

        Specify your own:

        vertex_attributes = [
          {
            'id': name of attribute,
            'num_components': int,
            'data_type': dtype,
          },
        ]

        More documentation:
        https://github.com/seung-lab/cloud-volume/wiki/Advanced-Topic:-Skeletons-and-Point-Clouds
        """
        if len(skelbuf) < 8:
            raise ValueError(
                "{} bytes is fewer than needed to specify the number of verices and edges.".format(
                    len(skelbuf)
                )
            )

        num_vertices, num_edges = struct.unpack("<II", skelbuf[:8])
        min_format_length = 8 + 12 * num_vertices + 8 * num_edges

        if len(skelbuf) < min_format_length:
            raise ValueError(
                "The input skeleton was {} bytes but the format requires {} bytes.".format(
                    len(skelbuf), min_format_length
                )
            )

        vstart = 2 * 4  # two uint32s in
        vend = vstart + num_vertices * 3 * 4  # float32s
        vertbuf = skelbuf[vstart:vend]

        estart = vend
        eend = estart + num_edges * 4 * 2  # 2x uint32s

        edgebuf = skelbuf[estart:eend]

        vertices = np.frombuffer(vertbuf, dtype="<f4").reshape((num_vertices, 3))
        edges = np.frombuffer(edgebuf, dtype="<u4").reshape((num_edges, 2))

        skeleton = Skeleton(vertices, edges, segid=segid)

        if len(skelbuf) == min_format_length:
            return skeleton

        if vertex_attributes is None:
            vertex_attributes = NotImplemented

        start = eend
        end = -1
        for attr in vertex_attributes:
            num_components = int(attr["num_components"])
            data_type = np.dtype(attr["data_type"])
            end = start + num_vertices * num_components * data_type.itemsize
            attrbuf = np.frombuffer(skelbuf[start:end], dtype=data_type)

            if num_components > 1:
                attrbuf = attrbuf.reshape((num_vertices, num_components))

            setattr(skeleton, attr["id"], attrbuf)
            start = end

        skeleton.vertex_attributes = vertex_attributes

        return skeleton
