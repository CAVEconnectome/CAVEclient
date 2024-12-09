import responses

from caveclient import CAVEclient, endpoints

from .conftest import (
    datastack_dict,
    global_client,  # noqa: F401
    mat_apiv2_specified_client,  # noqa: F401
    test_info,
    version_specified_client,  # noqa: F401
)

sk_mapping = {
    "skeleton_server_address": datastack_dict["local_server"],
    "datastack_name": datastack_dict["datastack_name"],
}

info_mapping = {
    "i_server_address": datastack_dict["global_server"],
    "datastack_name": datastack_dict["datastack_name"],
}
url_template = endpoints.infoservice_endpoints_v2["datastack_info"]
info_url = url_template.format_map(info_mapping)


class TestSkeletonsClient:
    sk_endpoints = endpoints.skeletonservice_endpoints_v1

    @responses.activate
    def test_create_client(self):
        responses.add(responses.GET, url=info_url, json=test_info, status=200)
        _ = CAVEclient(
            datastack_dict["datastack_name"],
            server_address=datastack_dict["global_server"],
            write_server_cache=False,
        )

    @responses.activate
    def test_get_version(self, myclient, mocker):
        metadata_url = self.sk_endpoints.get("get_version").format_map(sk_mapping)
        responses.add(responses.GET, url=metadata_url, json="0.1.2", status=200)

        print(myclient.skeleton.get_version())
        # client.skeleton.get_version()
