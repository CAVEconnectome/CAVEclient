from caveclient import CAVEclient
import os


def test_global_1():
    client = CAVEclient(token=os.environ["CAVE_TOKEN"])
    datastacks = client.info.get_datastacks()
    assert len(datastacks) > 10
