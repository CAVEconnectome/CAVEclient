import pytest
import requests
from annotationframeworkclient import FrameworkClient
import os
from annotationframeworkclient.endpoints import materialization_endpoints_v2,\
    chunkedgraph_endpoints_v1
import pandas as pd 
import responses
import pyarrow as pa
from urllib.parse import urlencode
from .conftest import test_info, TEST_LOCAL_SERVER, TEST_DATASTACK
import datetime
import time

def test_info_d(myclient):
    info = myclient.info.get_datastack_info()
    assert(info == test_info)

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
        is_latest_url = chunkedgraph_endpoints_v1['is_latest_roots'].format_map(endpoint_mapping)
        past_id_mapping = chunkedgraph_endpoints_v1['past_id_mapping'].format_map(endpoint_mapping)

        query_d ={
            'timestamp_past':time.mktime(past_timestamp.timetuple()),
            'timestamp_future':time.mktime(good_time.timetuple())
        }
        qpast_id_mapping_url = url + "?" + urlencode(query_d)
        root_ids_list = [600,601]
        id_map_str={'future_id_map': {},
                    'past_id_map': {'600': [500,502],
                        '601': [501]}}

        responses.add(responses.GET,
                status=200,
                url=qpast_id_mapping_url,
                json=id_map_str,
                match=[responses.json_params_matcher({'root_ids':root_ids_list})])
        query_d ={
            'timestamp':time.mktime(good_time.timetuple())
        }
        q_is_latest_now = is_latest_url + "?" + urlencode(query_d)

        responses.add(responses.GET,
                    status=200,
                    url=q_is_latest_now,
                    json=[False,False],
                    match=[responses.json_params_matcher({'node_ids':[500,501]})])
                    