from re import match
from .conftest import test_info, TEST_LOCAL_SERVER, TEST_DATASTACK
import pytest
import responses
import numpy as np
from annotationframeworkclient.endpoints import chunkedgraph_endpoints_v1
import datetime
import time
from urllib.parse import urlencode




def binary_body_match(body):
    def match(request_body):
        return body == request_body
    return match

class TestChunkedgraph():
    
    _default_endpoint_map = {
        'cg_server_address': TEST_LOCAL_SERVER,
        'table_id': test_info['segmentation_source'].split('/')[-1],
    }

    @responses.activate
    def test_get_roots(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        url=chunkedgraph_endpoints_v1['get_roots'].format_map(endpoint_mapping)
        svids = np.array([97557743795364048,75089979126506763], dtype=np.uint64)
        root_ids = np.array([864691135217871271, 864691135566275148], dtype=np.uint64)
        now = datetime.datetime.utcnow()
        query_d = {"timestamp": time.mktime(now.timetuple())}
        qurl = url + "?" + urlencode(query_d)
        responses.add(responses.POST,
                    url=qurl,
                    body = root_ids.tobytes(),
                    match=[binary_body_match(svids.tobytes())])

        new_root_ids = myclient.chunkedgraph.get_roots(svids, timestamp=now)
        assert(np.all(new_root_ids==root_ids))
        myclient.chunkedgraph._default_timestamp=now
        new_root_ids = myclient.chunkedgraph.get_roots(svids)
        assert(np.all(new_root_ids==root_ids))

        query_d = {"timestamp": time.mktime(now.timetuple()),
                   "stop_layer": 3}
        qurl = url + "?" + urlencode(query_d)
        responses.add(responses.POST,
                    url=qurl,
                    body = root_ids.tobytes(),
                    match=[binary_body_match(svids.tobytes())])
        new_root_ids = myclient.chunkedgraph.get_roots(svids,
                                                       timestamp=now,
                                                       stop_layer=3)
        assert(np.all(new_root_ids==root_ids))
        
        endpoint_mapping['supervoxel_id']=svids[0]
        url = chunkedgraph_endpoints_v1['handle_root'].format_map(endpoint_mapping)
        query_d = {"timestamp": time.mktime(now.timetuple())}
        qurl = url + "?" + urlencode(query_d)
        responses.add(responses.GET,
                    url=qurl,
                    json = {'root_id':int(root_ids[0])})
        qroot_id = myclient.chunkedgraph.get_root_id(svids[0], timestamp=now)
        assert(qroot_id == root_ids[0])

    @responses.activate
    def test_get_leaves(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135217871271
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['leaves_from_root'].format_map(endpoint_mapping)
        

        bad_bounds = np.array([[0,0,0,2],[100,100,100,0]])
        with pytest.raises(ValueError):
            myclient.chunkedgraph.get_leaves(root_id, bounds=bad_bounds)

        bounds = np.array([[0,0,0],[100,200,300]]).T
        bounds_str = "0-100_0-200_0-300"
        query_d = {'bounds': bounds_str}
        urlq = url + "?" + urlencode(query_d)

        svlist = [97557743795364048,75089979126506763]
        svids = np.array(svlist, dtype=np.int64)
        responses.add(responses.GET,
                      json={"leaf_ids":svlist},
                      url=urlq)

        svids_ret = myclient.chunkedgraph.get_leaves(root_id, bounds=bounds)
        assert(np.all(svids==svids_ret))

        query_d = {'bounds': bounds_str,
                   'stop_layer': 2}
        urlq = url + "?" + urlencode(query_d)
        responses.add(responses.GET,
                      json={"leaf_ids":svlist},
                      url=urlq)
        svids_ret = myclient.chunkedgraph.get_leaves(root_id, bounds=bounds, stop_layer=2)
        assert(np.all(svids==svids_ret))


    @responses.activate
    def test_get_root(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135217871271
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['leaves_from_root'].format_map(endpoint_mapping)
        

        bad_bounds = np.array([[0,0,0,2],[100,100,100,0]])
        with pytest.raises(ValueError):
            myclient.chunkedgraph.get_leaves(root_id, bounds=bad_bounds)

        bounds = np.array([[0,0,0],[100,200,300]]).T
        bounds_str = "0-100_0-200_0-300"
        query_d = {'bounds': bounds_str}
        urlq = url + "?" + urlencode(query_d)

        svlist = [97557743795364048,75089979126506763]
        svids = np.array(svlist, dtype=np.int64)
        responses.add(responses.GET,
                      json={"leaf_ids":svlist},
                      url=urlq)

        svids_ret = myclient.chunkedgraph.get_leaves(root_id, bounds=bounds)
        assert(np.all(svids==svids_ret))

        query_d = {'bounds': bounds_str,
                   'stop_layer': 2}
        urlq = url + "?" + urlencode(query_d)
        responses.add(responses.GET,
                      json={"leaf_ids":svlist},
                      url=urlq)
        svids_ret = myclient.chunkedgraph.get_leaves(root_id, bounds=bounds, stop_layer=2)
        assert(np.all(svids==svids_ret))

    @responses.activate
    def test_merge_log(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135217871271
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['merge_log'].format_map(endpoint_mapping)
        
        merge_log = {'merge_edge_coords': [[[[85785, 68475, 20988]], [[85717, 67955, 20964]]],
                    [[[86511, 70071, 20870]], [[86642, 70011, 20913]]],
                    [[[80660, 67637, 19735]], [[80946, 67810, 19735]]],
                    [[[84680, 63424, 20735]], [[84696, 63464, 20735]]],
                    [[[94096, 71152, 19934]], [[94096, 71168, 19937]]],
                    [[[89728, 72692, 20008]], [[89668, 72839, 19996]]],
                    [[[82492, 71488, 21534]], [[82726, 71281, 21584]]],
                    [[[85221, 69913, 20891]], [[85104, 70003, 20856]]]],
                    'merge_edges': [[[88393382627986340, 88322876444801990]],
                    [[88534532433083295, 88604901177276829]],
                    [[86985732732043081, 87056170195711450]],
                    [[88040164517305351, 88040164517304487]],
                    [[90645869502201091, 90645869502200218]],
                    [[89450013234655197, 89450081954148949]],
                    [[87479345001609186, 87549713745838644]],
                    [[88182619992741827, 88182688712176449]]]}
        
        responses.add(responses.GET,
                      json=merge_log,
                      url=url)

        qmerge_log = myclient.chunkedgraph.get_merge_log(root_id)
        assert(merge_log == qmerge_log)
        
    @responses.activate
    def test_change_log(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135217871271
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['change_log'].format_map(endpoint_mapping)
        
        change_log={'n_mergers': 2,
        'n_splits': 2,
        'operations_ids': [178060,
                    178059,
                    178046,
                    178050],
        'past_ids': [864691135181922050,
                    864691135761746230,
                    864691135785389764,
                    864691135583980920],
        'user_info': {'160': {'n_mergers': 1, 'n_splits': 1},
        '161': {'n_mergers': 1},
        '164': {'n_splits': 1}}}
        
        responses.add(responses.GET,
                      json=change_log,
                      url=url)

        qchange_log = myclient.chunkedgraph.get_change_log(root_id)
        assert(change_log == qchange_log)
    
    @responses.activate
    def test_children(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135217871271
        endpoint_mapping['node_id']=root_id
        url=chunkedgraph_endpoints_v1['handle_children'].format_map(endpoint_mapping)
        
        children_list = [792633534440329101, 828662331442575736, 792633534440186368]
        children_ids = np.array([children_list])
        
        
        responses.add(responses.GET,
                      json={'children_ids':children_list},
                      url=url)

        qchildren_ids = myclient.chunkedgraph.get_children(root_id)
        assert(np.all(children_ids==qchildren_ids))
    
    # waiting for backend fix
    # @responses.activate
    # def test_contact_sites(self, myclient):
    #     endpoint_mapping = self._default_endpoint_map
    #     root_id = 864691135217871271
    #     endpoint_mapping['node_id']=root_id
    #     url=chunkedgraph_endpoints_v1['handle_children'].format_map(endpoint_mapping)
        
    #     children_list = [792633534440329101, 828662331442575736, 792633534440186368]
    #     children_ids = np.array([children_list])
        
        
    #     responses.add(responses.GET,
    #                   json={'children_ids':children_list},
    #                   url=url)

    #     qchildren_ids = myclient.chunkedgraph.get_children(root_id)
    #     assert(np.all(children_ids==qchildren_ids))
    
    # waiting for backend to fix to finish
    # @responses.activate
    # def test_find_path(self, myclient):
    #     endpoint_mapping = self._default_endpoint_map
    #     root_id = 864691135217871271
    #     endpoint_mapping['node_id']=root_id
    #     url=chunkedgraph_endpoints_v1['handle_children'].format_map(endpoint_mapping)
        
    #     children_list = [792633534440329101, 828662331442575736, 792633534440186368]
    #     children_ids = np.array([children_list])
        
        
    #     responses.add(responses.GET,
    #                   json={'children_ids':children_list},
    #                   url=url)

    #     qchildren_ids = myclient.chunkedgraph.get_children(root_id)
    #     assert(np.all(children_ids==qchildren_ids))

    
    @responses.activate
    def test_get_subgraph(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135776832352
        
        bounds = np.array([[120241, 120441],
                            [103825, 104025],
                            [ 21350,  21370]])
        bounds_str = "120241-120441_103825-104025_21350-21370"

        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['get_subgraph'].format_map(endpoint_mapping)
        query_d={
            'bounds':bounds_str
        }
        qurl = url + "?" + urlencode(query_d)
        nodes_list=[[97832277702483859, 97832277702483868],
        [97832277702483868, 97832277702489688],
        [97832277702505017, 97832277702505025]]
        affinity_list = [2486.50634766,    7.49544525,   18.80846024]
        area_list = [2486,    7,   18]
        
        nodes = np.array(nodes_list, dtype=np.int64)
        affinities = np.array(affinity_list, dtype=np.float64)
        areas = np.array(area_list, dtype=np.int32)

        responses.add(responses.GET,
                      json={'nodes':nodes_list,
                            'affinities':affinity_list,
                            'areas':area_list},
                      url=qurl)

        qnodes, qaffinities, qareas = myclient.chunkedgraph.get_subgraph(root_id, bounds=bounds)
        assert(np.all(qnodes==nodes))
        assert(np.all(affinities==qaffinities))
        assert(np.all(areas==qareas))

    @responses.activate
    def test_get_lvl2subgraph(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135776832352
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['lvl2_graph'].format_map(endpoint_mapping)
       
        lvl2_graph_list=[[164471753114911373, 164471821834388004],
                    [164471753114911373, 164542121859089069],
                    [164471753114911412, 164542121859089069],
                    [164471821834388004, 164542190578565862]]

        lvl2_graph = np.array(lvl2_graph_list, dtype=np.int64)
        
        responses.add(responses.GET,
                      json={'edge_graph':lvl2_graph_list},
                      url=url)

        qlvl2_graph = myclient.chunkedgraph.level2_chunk_graph(root_id)
        assert(np.all(qlvl2_graph==lvl2_graph))

    @responses.activate
    def test_get_remeshing(self, myclient):
        endpoint_mapping = self._default_endpoint_map
        root_id = 864691135776832352
        endpoint_mapping['root_id']=root_id
        url=chunkedgraph_endpoints_v1['remesh_level2_chunks'].format_map(endpoint_mapping)
       
        chunkid_list=[164471753114911373, 164471821834388004]

        chunk_ids = np.array(chunkid_list, dtype=np.int64)
        
        responses.add(responses.POST,
                      status=200,
                      url=url,
                      match=[responses.json_params_matcher({'new_lvl2_ids':chunkid_list})])
                      
        myclient.chunkedgraph.remesh_level2_chunks(chunk_ids)
        myclient.chunkedgraph.remesh_level2_chunks(chunkid_list)