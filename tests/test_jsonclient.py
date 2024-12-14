import responses
import json

from .conftest import datastack_dict


json_v1 = "{json_server_address}/nglstate/api/v1"
jsonservice_endpoints_v1 = {
    "upload_state": json_v1 + "/post",
    "upload_state_w_id": json_v1 + "/post/{state_id}",
    "get_state": json_v1 + "/{state_id}",
    "get_state_raw": json_v1 + "/raw/{state_id}",
    "get_properties": json_v1 + "/property/{state_id}/info",
    "upload_properties": json_v1 + "/property/post",
    "get_properties_raw": json_v1 + "/property/raw/{state_id}",
    "upload_properties_w_id": json_v1 + "/property/post/{state_id}",
    "get_version": json_v1 + "/version",
}


@responses.activate()
def test_basic_state(myclient):
    global_server = datastack_dict["global_server"]
    state_id = 1234
    url = f"{global_server}/nglstate/api/v1/{state_id}"
    responses.add(responses.GET, url=url, json={"layers": ["img", "seg"]}, status=200)
    state = myclient.state.get_state_json(state_id)
    assert "img" in state["layers"]

    direct_url = "https://my-fake-url.com/direct_state.json"
    responses.add(
        responses.GET,
        url=direct_url,
        json={"layers": ["direct_img", "direct_seg"]},
        status=200,
    )
    state = myclient.state.get_state_json(direct_url)
    assert "direct_seg" in state["layers"]


@responses.activate
def test_get_neuroglancer_info(myclient):
    ngl_url = "https://my-fake-url.com"
    url = f"{ngl_url}/version.json"
    responses.add(responses.GET, url=url, json={"version": "1.0.0"}, status=200)
    info = myclient.state.get_neuroglancer_info(ngl_url)
    assert info["version"] == "1.0.0"


@responses.activate
def test_get_property_json(myclient):
    global_server = datastack_dict["global_server"]
    state_id = 1234
    url = f"{global_server}/nglstate/api/v1/property/{state_id}/info"
    responses.add(
        responses.GET, url=url, json={"properties": ["prop1", "prop2"]}, status=200
    )
    properties = myclient.state.get_property_json(state_id)
    assert "prop1" in properties["properties"]


@responses.activate
def test_upload_state_json(myclient):
    global_server = datastack_dict["global_server"]
    json_state = {"layers": ["img", "seg"]}
    url = f"{global_server}/nglstate/api/v1/post"
    responses.add(responses.POST, url=url, body="/1234", status=200)
    state_id = myclient.state.upload_state_json(json_state)
    assert state_id == 1234

    ## test the get_state_json method
    url = f"{global_server}/nglstate/api/v1/{state_id}"
    responses.add(responses.GET, url=url, json=json_state, status=200)
    state = myclient.state.get_state_json(state_id)
    assert "img" in state["layers"]


@responses.activate
def test_upload_property_json(myclient):
    global_server = datastack_dict["global_server"]
    property_json = {"properties": ["prop1", "prop2"]}
    url = f"{global_server}/nglstate/api/v1/property/post"
    responses.add(responses.POST, url=url, body="/1234", status=200)
    state_id = myclient.state.upload_property_json(property_json)
    assert state_id == 1234


def test_save_state_json_local(myclient, tmp_path):
    json_state = {"layers": ["img", "seg"]}
    filename = tmp_path / "state.json"
    myclient.state.save_state_json_local(json_state, str(filename))
    with open(filename, "r") as f:
        saved_state = json.load(f)
    assert saved_state["layers"] == ["img", "seg"]


@responses.activate
def test_build_neuroglancer_url(myclient):
    state_id = 1234
    ngl_url = "https://my-fake-url.com"
    global_server = datastack_dict["global_server"]
    url = f"{global_server}/nglstate/api/v1/{state_id}"
    responses.add(responses.GET, url=f"{ngl_url}/version.json", json={}, status=200)
    built_url = myclient.state.build_neuroglancer_url(state_id, ngl_url=ngl_url)
    assert built_url.startswith(ngl_url)
