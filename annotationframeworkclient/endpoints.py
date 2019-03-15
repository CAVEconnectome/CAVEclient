
default_server_address = "https://www.dynamicannotationframework.com"


annotationengine_endpoints = {
    "datasets": "{ae_server_address}/annotation/datasets",
    "table_names": "{ae_server_address}/annotation/dataset/{dataset_name}",
    "existing_annotation": "{ae_server_address}/annotation/dataset/{dataset_name}/"
                           "{table_name}/{annotation_id}",
    "new_annotation": "{ae_server_address}/annotation/dataset/{dataset_name}/"
                      "{table_name}",
    "supervoxel": "{ae_server_address}/voxel/dataset/{dataset_name}/{x}_{y}_{z}",
    "existing_segment_annotation": "{server_address}/chunked_annotation/dataset/{dataset_name}/"
                                   "rootid/{root_id}/{table_name}",
}

infoservice_endpoints = {
    "datasets": "{i_server_address}/info/api/datasets",
    "dataset_info": "{i_server_address}/info/api/dataset/{dataset_name}",
}

chunkedgraph_endpoints = {
    "get_root": "{cg_server_address}/segmentation/1.0/{table_id}/graph/{supervoxel_id}/root",
    "get_leaves": "{cg_server_address}/segmentation/1.0/{table_id}/segment/{root_id}/leaves",
    "get_leaves_from_leaves": "{cg_server_address}/segmentation/1.0/{table_id}/segment/{supervoxel_id}/leaves_from_leave"
}

schema_endpoints = {
    "schema" : "{emas_server_address}/schema/type",
    "schema_definition": "{emas_server_address}/schema/type/{schema_type}",
}

jsonservice_endpoints = {
    "upload_state" : "{json_server_address}/nglstate/post",
    "get_state": "{json_server_address}/nglstate/{state_id}",
    'get_state_raw': "{json_server_address}/nglstate/raw/{state_id}",
}

materialization_endpoints = {
    "datasets": "{m_server_address}/materialize/api/dataset",
    "get_dataset_version": "{m_server_address}/materialize/api/dataset/{dataset_name}",
}