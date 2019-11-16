import numpy as np
import requests
from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg

def package_bounds(bounds):
    bounds_str=[]
    for b in bounds:
        bounds_str.append("-".join(str(b2) for b2 in b))
    bounds_str = "_".join(bounds_str)
    return bounds_str

class ChunkedGraphClient(object):
    def __init__(self, server_address=None, dataset_name=None,
                 table_name=None):
        if server_address is None:
            self._server_address = endpoints.default_server_address
        else:
            self._server_address = server_address
        if table_name is None:
            info_client = infoservice.InfoServiceClient(server_address=self._server_address)
            pcg_vs = info_client.pychunkedgraph_viewer_source(dataset_name=dataset_name)
            table_name = pcg_vs.split('/')[-1]
        self.table_name = table_name
        self.session = requests.Session()
        self.info_cache = dict()

        self._default_url_mapping = {"cg_server_address": self._server_address,
                                     "table_id": self.table_name}

    @property
    def default_url_mapping(self):
        return self._default_url_mapping.copy()

    def get_root_id(self, supervoxel_id, bounds=None):
        endpoint_mapping = self.default_url_mapping
        url = cg['handle_root'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[supervoxel_id])
        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_merge_log(self, root_id):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['merge_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])
     
        assert(response.status_code == 200)
        return response.json()

    def get_change_log(self, root_id):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['change_log'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])
     
        assert(response.status_code == 200)
        return response.json()

    def get_leaves(self, root_id, bounds=None):
        """
        get the supervoxels for this root_id

        params
        ------
        root_id: uint64 root id to find supervoxels for
        bounds: 3x2 numpy array of bounds [[minx,maxx],[miny,maxy],[minz,maxz]]
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['leaves_from_root'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)

        response = self.session.post(url, json=[root_id], params=query_d)
     
        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)

    def get_children(self, node_id):
        """
        get the children of any node in the hierarchy

        :param node_id: np.uint64
        :return: list of np.uint64
        """
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['node_id'] = node_id
        url = cg['handle_children'].format_map(endpoint_mapping)

        response = self.session.post(url)

        assert(response.status_code == 200)
        return np.frombuffer(response.content, dtype=np.uint64)


    def get_contact_sites(self, root_id, bounds=None, calc_partners=False):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['contact_sites'].format_map(endpoint_mapping)
        query_d = {}
        if bounds is not None:
            query_d['bounds'] = package_bounds(bounds)
        query_d['partners']=calc_partners
        response = self.session.post(url, json=[root_id], params=query_d)
        contact_d = response.json()
        return {int(k):v for k,v in contact_d.items()}

    @property
    def cloudvolume_path(self):
        return cg['cloudvolume_path'].format_map(self.default_url_mapping)
    