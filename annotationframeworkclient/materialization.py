from annotationframeworkclient.endpoints import materialization_endpoints as mte
from annotationframeworkclient.base import ClientBase
import json

class MaterializationClient(ClientBase):
    def __init__(self, dataset_name=None, server_address=None):
        super(MaterializationClient).__init__(dataset_name=dataset_name,
                                              server_address=server_address)
        self._default_url_mapping = {'m_server_address': self._server_adddress}

    def datasets(self):
        url = mte['datasets'].format_map(self.default_url_mapping)
        response = self.session.get(url)
        assert(response.status_code == 200)
        return response.json()

    def get_dataset_version(self, dataset_name=None):
        if dataset_name is None:
            dataset_name = self.dataset_name

        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['dataset_name'] = dataset_name
        url = mte['get_dataset_version'].format_map(endpoint_mapping)

        response = self.session.get(url)
        assert(response.status_code==200)
        return response.json()