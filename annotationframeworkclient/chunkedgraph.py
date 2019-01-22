import numpy as np
import requests
from annotationframeworkclient import endpoints
from annotationframeworkclient import infoservice
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg

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

    def get_root_id(self, supervoxel_id):
        endpoint_mapping = self.default_url_mapping
        url = cg['handle_root'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[supervoxel_id])
        assert(response.status_code == 200)
        return np.squeeze(np.frombuffer(response.content, dtype=np.uint64)).tolist()
    
    def get_leaves(self, root_id):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg['leaves_from_root'].format_map(endpoint_mapping)
        response = self.session.post(url, json=[root_id])
        assert(response.status_code == 200)
        return np.squeeze(np.frombuffer(response.content, dtype=np.uint64)).tolist()
