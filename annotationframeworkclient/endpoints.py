
default_server_address = "https://www.dynamicannotationframework.com"


annotationengine_endpoints = {
    "datasets": "{ae_server_address}/annotation/datasets",
    "annotation_types": "{ae_server_address}/annotation/dataset/{dataset_name}",
    "existing_annotation": "{ae_server_address}/annotation/dataset/{dataset_name}/"
                           "{annotation_type}/{annotation_id}",
    "new_annotation": "{ae_server_address}/annotation/dataset/{dataset_name}/"
                      "{annotation_type}",
    "supervoxel": "{ae_server_address}/voxel/dataset/{dataset_name}/{x}_{y}_{z}",
    "existing_segment_annotation": "{server_address}/chunked_annotation/dataset/{dataset_name}/"
                                   "rootid/{root_id}/{annotation_type}",
}

infoservice_endpoints = {
    "datasets": "{i_server_address}/info/api/datasets",
    "dataset_info": "{i_server_address}/info/api/dataset/{dataset_name}",
}

chunkedgraph_endpoints = {
    # "handle_table": "{cg_server_address}/segmentation/1.0/table",
    "handle_root": "{cg_server_address}/segmentation/1.0/graph/root",
    # "handle_merge": "{cg_server_address}/segmentation/1.0/graph/merge",
    # "handle_split": "{cg_server_address}/segmentation/1.0/graph/split",
    # "handle_children": "{cg_server_address}/segmentation/1.0/segment/{parent_id}/childen",
    # "handle_leaves": "{cg_server_address}/segmentation/1.0/segment/{root_id}/leaves",
    # "handle_leaves_from_leaf": "{cg_server_address}/segmentation/1.0/segment/{atomic_id}/leaves_from_leave",
    # "handle_subgraph": "{cg_server_address}/segmentation/1.0/segment/{root_id}/subgraph",
}

schema_endpoints = {
    "schema"
}