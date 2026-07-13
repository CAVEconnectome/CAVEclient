from caveclient.format_utils import (
    format_cave_explorer,
    format_verbose_graphene,
    output_map,
)


def test_neuroglancer_uses_cave_explorer_format():
    assert output_map["neuroglancer"] is format_cave_explorer


def test_cave_explorer_https_source():
    assert (
        format_cave_explorer("https://example.com/segmentation/table")
        == "graphene://middleauth+https://example.com/segmentation/table"
    )


def test_verbose_graphene_unknown_scheme_returns_none():
    assert format_verbose_graphene("gs://bucket/path") is None
