import responses

from .conftest import datastack_dict


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
