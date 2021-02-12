
default_global_server_address = "https://globalv1.daf-apis.com"

# -------------------------------
# ------ AnnotationEngine endpoints
# -------------------------------

annotation_common = {}

anno_legacy = "{ae_server_address}/annotation"
annotation_endpoints_legacy = {
    "datasets": anno_legacy + "/datasets",
    "table_names": anno_legacy + "/dataset/{dataset_name}",
    "existing_annotation": anno_legacy + "/dataset/{dataset_name}/{table_name}/{annotation_id}",
    "new_annotation": anno_legacy + "/dataset/{dataset_name}/{table_name}",
}

anno_v2 = "{ae_server_address}/annotation/api/v2"
annotation_endpoints_v2 = {
    "tables": anno_v2 + "/aligned_volume/{aligned_volume_name}/table",
    "table_info": anno_v2 + "/aligned_volume/{aligned_volume_name}/table/{table_name}",
    "annotations": anno_v2 + "/aligned_volume/{aligned_volume_name}/table/{table_name}/annotations",
    "table_count": anno_v2 + "/aligned_volume/{aligned_volume_name}/table/{table_name}/count",
}


materialization_common = {}
mat_v2_api = "{me_server_address}/materialize/api/v2"
materialization_endpoints_v2 = {
    "simple_query": mat_v2_api + "/datastack/{datastack_name}/version/{version}/table/{table_name}/query",
    "join_query": mat_v2_api + "/datastack/{datastack_name}/version/{version}/query",
    "annotations": mat_v2_api + "/datastack/{datastack_name}/version/{version}/table/{table_name}",
    "table_count": mat_v2_api + "/datastack/{datastack_name}/version/{version}/table/{table_name}/count",
    "versions": mat_v2_api + "/datastack/{datastack_name}/versions",
    "version_metadata": mat_v2_api + "/datastack/{datastack_name}/version/{version}",
    "tables": mat_v2_api + "/datastack/{datastack_name}/version/{version}/tables",
    "metadata": mat_v2_api + "/datastack/{datastack_name}/version/{version}/table/{table_name}/metadata",
}
materialization_api_versions = {2: materialization_endpoints_v2}
annotation_api_versions = {0: annotation_endpoints_legacy,
                           2: annotation_endpoints_v2}

# -------------------------------
# ------ Infoservice endpoints
# -------------------------------

infoservice_common = {}

info_v1 = "{i_server_address}/info/api"
infoservice_endpoints_v1 = {
    "datasets": info_v1 + "/datasets",
    "dataset_info": info_v1 + "/dataset/{dataset_name}",
}

info_v2 = "{i_server_address}/info/api/v2"
infoservice_endpoints_v2 = {
    "aligned_volumes": info_v2 + "/aligned_volume",
    "aligned_volume_info": info_v2 + "/aligned_volume/{aligned_volume_name}",
    "aligned_volume_by_id": info_v2 + "/aligned_volume/id/{aligned_volume_id}",
    "datastacks": info_v2 + "/datastacks",
    "datastack_info": info_v2 + "/datastack/full/{datastack_name}",
}

infoservice_api_versions = {1: infoservice_endpoints_v1,
                            2: infoservice_endpoints_v2}

# -------------------------------
# ------ Pychunkedgraph endpoints
# -------------------------------

pcg_common = "{cg_server_address}/segmentation"
chunkedgraph_endpoints_common = {
    "get_api_versions": pcg_common + "/api/versions",
    'info': pcg_common + "/table/{table_id}/info",
}

pcg_legacy = "{cg_server_address}/segmentation/1.0"
chunkedgraph_endpoints_legacy = {
    # "handle_table": "{cg_server_address}/segmentation/1.0/table",
    "handle_root": pcg_legacy + "/{table_id}/graph/root",
    "handle_children": pcg_legacy + "/segment/{node_id}/children",
    # "info": pcg_legacy + "/{table_id}/info",
    "leaves_from_root": pcg_legacy + "/{table_id}/segment/{root_id}/leaves",
    "merge_log": pcg_legacy + "/{table_id}/segment/{root_id}/merge_log",
    "change_log":  pcg_legacy + "/{table_id}/segment/{root_id}/change_log",
    "contact_sites": pcg_legacy + "/{table_id}/segment/{root_id}/contact_sites",
    "cloudvolume_path": "graphene://" + pcg_legacy + "/{table_id}",
}

pcg_v1 = "{cg_server_address}/segmentation/api/v1"
pcg_meshing_v1 = "{cg_server_address}/meshing/api/v1"
chunkedgraph_endpoints_v1 = {
    'handle_root': pcg_v1 + "/table/{table_id}/node/{supervoxel_id}/root",
    "handle_roots": pcg_v1 + "/table/{table_id}/roots",
    'handle_children': pcg_v1 + "/table/{table_id}/node/{root_id}/childen",
    'leaves_from_root': pcg_v1 + "/table/{table_id}/node/{root_id}/leaves",
    'do_merge': pcg_v1 + "/table/{table_id}/merge",
    'get_roots': pcg_v1 + "/table/{table_id}/roots_binary",
    'merge_log': pcg_v1 + "/table/{table_id}/root/{root_id}/merge_log",
    'change_log': pcg_v1 + "/table/{table_id}/root/{root_id}/change_log",
    'contact_sites': pcg_v1 + "/table/{table_id}/node/{root_id}/contact_sites",
    'contact_sites_pairwise': pcg_v1 + "/table/{table_id}/contact_sites_pair/{root_id_1}/{root_id_2}",
    'cloudvolume_path': "graphene://" + pcg_v1 + "/{table_id}",
    'find_path': pcg_v1 + "/table/{table_id}/graph/find_path",
    'lvl2_graph': pcg_v1 + "/table/{table_id}/node/{root_id}/lvl2_graph",
    'remesh_level2_chunks': pcg_meshing_v1 + "/table/{table_id}/remeshing",
    'get_subgraph': pcg_v1 + "/table/{table_id}/node/{root_id}/subgraph"
}

chunkedgraph_api_versions = {0: chunkedgraph_endpoints_legacy,
                             1: chunkedgraph_endpoints_v1,
                             }

# -------------------------------
# ------ EMAnnotationSchemas endpoints
# -------------------------------
schema_common = "{emas_server_address}/schema"
schema_endpoints_common = {
     "get_api_versions": schema_common + "/versions",
}

schema_v1 = "{emas_server_address}/schema"
schema_endpoints_v1 = {
    "schema": schema_v1 + "/type",
    "schema_definition": schema_v1 + "/type/{schema_type}"
}
schema_v2 = "{emas_server_address}/schema/api/v2"
schema_endpoints_v2 = {
    "schema": schema_v2 + "/type",
    "schema_definition": schema_v2 + "/type/{schema_type}"
}

schema_api_versions = {1: schema_endpoints_v1,
                       2: schema_endpoints_v2}

# -------------------------------
# ------ StateServer endpoints
# -------------------------------

jsonservice_common = {}

json_v1 = "{json_server_address}/nglstate/api/v1"
jsonservice_endpoints_v1 = {
    "upload_state": json_v1 + "/post",
    "upload_state_w_id": json_v1 + "/post/{state_id}",
    "get_state": json_v1 + "/{state_id}",
    'get_state_raw': json_v1 + "/raw/{state_id}",
}

json_legacy = "{json_server_address}/nglstate"
jsonservice_endpoints_legacy = {
    "upload_state": json_legacy + "/post",
    "get_state": json_legacy + "/{state_id}",
    'get_state_raw': json_legacy + "/raw/{state_id}",
}

jsonservice_api_versions = {0: jsonservice_endpoints_legacy,
                            1: jsonservice_endpoints_v1,
                            }

# -------------------------------
# ------ Auth endpoints
# -------------------------------

auth_common = {}

v1_auth = "{auth_server_address}/auth/api/v1"
auth_endpoints_v1 = {
    "refresh_token": v1_auth + "/refresh_token"
}

auth_api_versions = {1: auth_endpoints_v1,
                     }
