import pytest
import requests
from annotationframeworkclient import FrameworkClient
import os
from annotationframeworkclient.endpoints import materialization_endpoints_v2,\
    chunkedgraph_endpoints_v1, chunkedgraph_endpoints_common
import pandas as pd 
import responses
import pyarrow as pa
from urllib.parse import urlencode
from .conftest import test_info, TEST_LOCAL_SERVER, TEST_DATASTACK
import datetime
import time
import numpy as np

def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert(info == test_info)

def binary_body_match(body):
    def match(request_body):
        return body == request_body
    return match

class TestMatclient():
    default_mapping = {
            'me_server_address': TEST_LOCAL_SERVER,
            'cg_server_address': TEST_LOCAL_SERVER,
            'table_id': test_info['segmentation_source'].split('/')[-1],
            'datastack_name': TEST_DATASTACK,
            'table_name': test_info['synapse_table'],
            'version': 1
        }
    endpoints=materialization_endpoints_v2
    @responses.activate
    def test_matclient(self, myclient):
        endpoint_mapping = self.default_mapping
        api_versions_url = chunkedgraph_endpoints_common['get_api_versions'].format_map(endpoint_mapping)
        responses.add(
            responses.GET,
            url=api_versions_url,
            json=[0,1],
            status=200
        )

        versionurl = self.endpoints['versions'].format_map(endpoint_mapping)

        responses.add(
            responses.GET,
            url=versionurl,
            json=[1],
            status=200
        )

        url = self.endpoints['simple_query'].format_map(endpoint_mapping)
        query_d={'return_pyarrow': True,
            'split_positions': False}
        query_string = urlencode(query_d)
        url = url + "?" + query_string
        correct_query_data = {
            'filter_in_dict':{
                test_info['synapse_table']:{
                    'pre_pt_root_id': [500]
                }
            },
            'filter_notin_dict':{
                test_info['synapse_table']:{
                    'post_pt_root_id': [501]
                }
            },
            'filter_equal_dict':{
                test_info['synapse_table']:{
                    'size': 100
                }
            },
            'offset':0,
            'limit':1000
        }
        df=pd.read_pickle('tests/test_data/synapse_query.pkl')
        
        context = pa.default_serialization_context()
        serialized = context.serialize(df)

        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={'content-type': 'x-application/pyarrow'},
            match=[
                responses.json_params_matcher(correct_query_data)
            ]
        )
    
        df=myclient.materialize.query_table(test_info['synapse_table'],
                                        filter_in_dict={'pre_pt_root_id': [500]},
                                        filter_out_dict={'post_pt_root_id': [501]},
                                        filter_equal_dict={'size':100},
                                        limit=1000,
                                        offset=0)
        assert(len(df)==1000)
        assert(type(df)==pd.DataFrame)

        correct_metadata=[{'version': 1,
                   'expires_on': '2021-04-19T08:10:00.255735',
                   'id': 84,
                   'valid': True,
                   'time_stamp': '2021-04-12T08:10:00.255735',
                   'datastack': 'minnie65_phase3_v1'}]
        
        past_timestamp = datetime.datetime.strptime(correct_metadata[0]['time_stamp'],
                                                        '%Y-%m-%dT%H:%M:%S.%f')

        md_url = self.endpoints['versions_metadata'].format_map(endpoint_mapping)
        responses.add(
            responses.GET,
            url=md_url,
            json=correct_metadata,
            status=200
        )

        bad_time = datetime.datetime(year=2020, month=4,day=19, hour=0)
        good_time = datetime.datetime(year=2021, month=4, day=19, hour=0)

        with pytest.raises(ValueError):
            df=myclient.materialize.live_query(test_info['synapse_table'],
                                                bad_time,
                                                filter_in_dict={'pre_pt_root_id': [600]},
                                                filter_out_dict={'post_pt_root_id': [601]},
                                                filter_equal_dict={'size':100},
                                                limit=1000,
                                                offset=0)

        ### live query test

        is_latest_url = chunkedgraph_endpoints_v1['is_latest_roots'].format_map(endpoint_mapping)
        past_id_mapping_url = chunkedgraph_endpoints_v1['past_id_mapping'].format_map(endpoint_mapping)
        get_roots_url = chunkedgraph_endpoints_v1['get_roots'].format_map(endpoint_mapping)

        query_d ={
            'timestamp_past':time.mktime(past_timestamp.timetuple()),
            'timestamp_future':time.mktime(good_time.timetuple())
        }
        qpast_id_mapping_url = past_id_mapping_url + "?" + urlencode(query_d)
        root_ids_list1 = [103,201]
        id_map_str1={'future_id_map': {},
                    'past_id_map': {'201': [100],
                        '103': [103]}}
        responses.add(responses.GET,
                      status=200,
                      url=qpast_id_mapping_url,
                      json=id_map_str1,
                      match=[responses.json_params_matcher({'root_ids':root_ids_list1})])
        
        root_ids_list2 = [201,203]
        id_map_str2={'future_id_map': {},
                    'past_id_map': {'201': [100],
                        '203': [101,102]}}

        responses.add(responses.GET,
                      status=200,
                      url=qpast_id_mapping_url,
                      json=id_map_str2,
                      match=[responses.json_params_matcher({'root_ids':root_ids_list2})])


        root_ids_list3 = [203]
        id_map_str3={'future_id_map': {},
                    'past_id_map': {'203': [101,102]}}
        responses.add(responses.GET,
                      status=200,
                      url=qpast_id_mapping_url,
                      json=id_map_str3,
                      match=[responses.json_params_matcher({'root_ids':root_ids_list3})])

        query_d ={
            'timestamp':time.mktime(good_time.timetuple())
        }
        q_is_latest_now = is_latest_url + "?" + urlencode(query_d)

        responses.add(responses.POST,
                    status=200,
                    url=q_is_latest_now,
                    json={'is_latest':[True,True]},
                    match=[responses.json_params_matcher({'node_ids':[103,201]})])
        responses.add(responses.POST,
                    status=200,
                    url=q_is_latest_now,
                    json={'is_latest':[True,True]},
                    match=[responses.json_params_matcher({'node_ids':[201,203]})])
        responses.add(responses.POST,
                    status=200,
                    url=q_is_latest_now,
                    json={'is_latest':[True]},
                    match=[responses.json_params_matcher({'node_ids':[203]})])            
        responses.add(responses.POST,
                    status=200,
                    url=q_is_latest_now,
                    json={'is_latest':[False,True]},
                    match=[responses.json_params_matcher({'node_ids':[100,103]})])
        responses.add(responses.POST,
                    status=200,
                    url=q_is_latest_now,
                    json={'is_latest':[False,False,False,True]},
                    match=[responses.json_params_matcher({'node_ids':[100,101,102,103]})])
        
        qurl = get_roots_url + "?" + urlencode(query_d)
        root_ids = np.array([200,201,201], dtype=np.int64)
        svids = np.array([1,3,4], dtype=np.int64)
        responses.add(responses.POST,
                    url=qurl,
                    body = root_ids.tobytes(),
                    match=[binary_body_match(svids.tobytes())])

        root_ids = np.array([203,203,200,203], dtype=np.int64)
        svids = np.array([5,6,2,7], dtype=np.int64)
        responses.add(responses.POST,
                    url=qurl,
                    body = root_ids.tobytes(),
                    match=[binary_body_match(svids.tobytes())])

        url = self.endpoints['simple_query'].format_map(endpoint_mapping)
        query_d={'return_pyarrow': True,
                 'split_positions': False}
        query_string = urlencode(query_d)
        url = url + "?" + query_string

        df=pd.read_pickle('tests/test_data/live_query_before.pkl')
        
        context = pa.default_serialization_context()
        serialized = context.serialize(df)
        correct_query_data = {
            'filter_in_dict':{
                test_info['synapse_table']:{
                    'pre_pt_root_id': [100,103]
                }
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={'content-type': 'x-application/pyarrow'},
            match=[
                responses.json_params_matcher(correct_query_data)
            ]
        )
        correct_query_data = {
            'filter_in_dict':{
                test_info['synapse_table']:{
                    'post_pt_root_id': [100,101,102]
                }
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={'content-type': 'x-application/pyarrow'},
            match=[
                responses.json_params_matcher(correct_query_data)
            ]
        )
        correct_query_data = {
            'filter_in_dict':{
                test_info['synapse_table']:{
                    'post_pt_root_id': [101,102]
                }
            }
        }
        responses.add(
            responses.POST,
            url=url,
            body=serialized.to_buffer().to_pybytes(),
            headers={'content-type': 'x-application/pyarrow'},
            match=[
                responses.json_params_matcher(correct_query_data)
            ]
        )

        dfq = myclient.materialize.live_query(test_info['synapse_table'],
                                              good_time,
                                              filter_in_dict={'pre_pt_root_id':[201,103]})

        dfr = pd.read_pickle('tests/test_data/live_query_after1.pkl')
        assert(np.all(dfq.pre_pt_root_id==dfr.pre_pt_root_id))
        assert(np.all(dfq.post_pt_root_id==dfr.post_pt_root_id))

        
        dfq = myclient.materialize.live_query(test_info['synapse_table'],
                                              good_time,
                                              filter_in_dict={'post_pt_root_id':[201,203]})

        dfr = pd.read_pickle('tests/test_data/live_query_after2.pkl')
        assert(np.all(dfq.pre_pt_root_id==dfr.pre_pt_root_id))
        assert(np.all(dfq.post_pt_root_id==dfr.post_pt_root_id))

        dfq = myclient.materialize.live_query(test_info['synapse_table'],
                                              good_time,
                                              filter_equal_dict={'post_pt_root_id':203})
        dfr = pd.read_pickle('tests/test_data/live_query_after3.pkl')
        assert(np.all(dfq.pre_pt_root_id==dfr.pre_pt_root_id))
        assert(np.all(dfq.post_pt_root_id==dfr.post_pt_root_id))