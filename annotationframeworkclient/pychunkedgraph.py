import re
from numpy import frombuffer, uint64
from urllib.parse import urlparse
from annotationframeworkclient.base import ClientBase
from annotationframeworkclient.endpoints import chunkedgraph_endpoints as cg_endpoints

class PychunkedgraphClient(ClientBase):
    def __init__(self, server_address=None, table_id=None, cv_path=None):
        if server_address is None or table_name is None:
            if cv_path is None:
                raise ValueError("Must set either cv path or server address and table name")
            else:
                cv_parsed = urlparse(cv_path)
                if cv_parsed.scheme=='graphene':
                    cv_parsed = urlparse(cv_parsed.netloc+cv_parsed.path)               
                server_address = cv_parsed.scheme + '://' + cv_parsed.netloc
                qry = re.search('\/segmentation\/.*\/(.*)', cv_parsed.path)
                table_id = qry.groups()[0]
        super(PychunkedgraphClient, self).__init__(server_address=server_address,
                                                   dataset_name=None)

        self._table_id = table_id
        self._default_url_mapping = {'cg_server_address': self._server_adddress,
                                     'table_id': self._table_id}
    
    def get_root(self, supervoxel_id, debug=False):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['supervoxel_id'] = supervoxel_id
        url = cg_endpoints['get_root'].format_map(endpoint_mapping)
        if debug==True:
            return url
        else:
            response = self.session.get(url)
            assert(response.status_code==200)
            return frombuffer(response.content, dtype=uint64)

    def get_leaves(self, root_id, debug=False):
        endpoint_mapping = self.default_url_mapping
        endpoint_mapping['root_id'] = root_id
        url = cg_endpoints['get_leaves'].format_map(endpoint_mapping)
        if debug==True:
            return url
        else:
            response = self.session.get(url)
            assert(response.status_code==200)
            return frombuffer(response.content, dtype=uint64)
