



annotationengine_endpoints = {
    "datasets": "{server_address}/annotation/datasets",
    "dataset_schema": "{server_address}/annotation/dataset/{dataset_name}",
    "existing_annotation": "{server_address}/annotation/dataset/{dataset_name}/"
                           "{annotation_type}/{annotation_id}",
    "new_annotation": "{server_address}/annotation/dataset/{dataset_name}/"
                      "{annotation_type}",
    "supervoxel": "{server_address}/voxel/dataset/{dataset_name}/{x}_{y}_{z}",
    "existing_segment_annotation": "{server_address}/chunked_annotation/dataset/{dataset_name}/"
                                   "rootid/{object_id}/{annotation_type}",
}

infoservice_endpoints = {
    "datasets": "{server_address}/api/datasets",
    "dataset_info": "{server_address}/api/dataset/{dataset_name}",
}