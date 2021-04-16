from re import match
from .conftest import test_info, TEST_LOCAL_SERVER, TEST_DATASTACK
import pytest
import responses
import numpy as np
from annotationframeworkclient.endpoints import chunkedgraph_endpoints_v1
import datetime
import time
from urllib.parse import urlencode


endpoint_map = {
    'cg_server_address': TEST_LOCAL_SERVER,
    'table_id': test_info['segmentation_source'].split('/')[-1]
}


def binary_body_match(body):
    def match(request_body):
        return body == request_body
    return match


@responses.activate
def test_get_roots(myclient):
    url=chunkedgraph_endpoints_v1['get_roots'].format_map(endpoint_map)

    svids = np.array([97557743795364048,75089979126506763], dtype=np.uint64)
    root_ids = np.array([864691135217871271, 864691135566275148], dtype=np.uint64)
    now = datetime.datetime.utcnow()
    query_d = {"timestamp": time.mktime(now.timetuple())}
    query_string = urlencode(query_d)
    url = url + "?" + query_string
    responses.add(responses.POST,
                  url=url,
                  body = root_ids.tobytes(),
                  match=[binary_body_match(svids.tobytes())])

    new_root_ids = myclient.chunkedgraph.get_roots(svids, timestamp=now)
    assert(np.all(new_root_ids==root_ids))