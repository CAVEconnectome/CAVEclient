default_global_server_address = "https://global.daf-apis.com"

# -------------------------------
# ------ AnnotationEngine endpoints
# -------------------------------

annotation_common = {}

anno_legacy = "{ae_server_address}/annotation"
annotation_endpoints_legacy = {
    "datasets": anno_legacy + "/datasets",
    "table_names": anno_legacy + "/dataset/{dataset_name}",
    "existing_annotation": anno_legacy
    + "/dataset/{dataset_name}/{table_name}/{annotation_id}",
    "new_annotation": anno_legacy + "/dataset/{dataset_name}/{table_name}",
}

anno_v2 = "{ae_server_address}/annotation/api/v2"
annotation_endpoints_v2 = {
    "tables": anno_v2 + "/aligned_volume/{aligned_volume_name}/table",
    "table_info": anno_v2 + "/aligned_volume/{aligned_volume_name}/table/{table_name}",
    "annotations": anno_v2
    + "/aligned_volume/{aligned_volume_name}/table/{table_name}/annotations",
    "table_count": anno_v2
    + "/aligned_volume/{aligned_volume_name}/table/{table_name}/count",
}

annotation_api_versions = {0: annotation_endpoints_legacy, 2: annotation_endpoints_v2}

# -------------------------------
# ------ MaterializationEngine endpoints
# -------------------------------

materialization_common = {
    "get_api_versions": "{me_server_address}/materialize/api/versions"
}
mat_v2_api = "{me_server_address}/materialize/api/v2"
mat_v3_api = "{me_server_address}/materialize/api/v3"
materialization_endpoints_v2 = {
    "get_api_versions": "{me_server_address}/api/versions",
    "simple_query": mat_v2_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/query",
    "join_query": mat_v2_api + "/datastack/{datastack_name}/version/{version}/query",
    "annotations": mat_v2_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}",
    "table_count": mat_v2_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/count",
    "versions": mat_v2_api + "/datastack/{datastack_name}/versions",
    "version_metadata": mat_v2_api + "/datastack/{datastack_name}/version/{version}",
    "tables": mat_v2_api + "/datastack/{datastack_name}/version/{version}/tables",
    "metadata": mat_v2_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/metadata",
    "versions_metadata": mat_v2_api + "/datastack/{datastack_name}/metadata",
    "ingest_annotation_table": mat_v2_api
    + "/materialize/run/ingest_annotations/datastack/{datastack_name}/{table_name}",
    "segmentation_metadata": mat_v2_api
    + "/datastack/{datastack_name}/table/{table_name}/segmentation_metadata",
    "live_live_query": mat_v3_api + "/datastack/{datastack_name}/query",
    "lookup_supervoxel_ids": mat_v2_api
    + "/materialize/run/lookup_svid/datastack/{datastack_name}/{table_name}",
}
materialization_endpoints_v3 = {
    "get_api_versions": "{me_server_address}/api/versions",
    "simple_query": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/query",
    "join_query": mat_v3_api + "/datastack/{datastack_name}/version/{version}/query",
    "table_count": mat_v2_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/count",
    "versions": mat_v3_api + "/datastack/{datastack_name}/versions",
    "version_metadata": mat_v3_api + "/datastack/{datastack_name}/version/{version}",
    "tables": mat_v2_api + "/datastack/{datastack_name}/version/{version}/tables",
    "metadata": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/table/{table_name}/metadata",
    "all_tables_metadata": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/tables/metadata",
    "versions_metadata": mat_v3_api + "/datastack/{datastack_name}/metadata",
    "ingest_annotation_table": mat_v2_api
    + "/materialize/run/ingest_annotations/datastack/{datastack_name}/{table_name}",
    "segmentation_metadata": mat_v3_api
    + "/datastack/{datastack_name}/table/{table_name}/segmentation_metadata",
    "live_live_query": mat_v3_api + "/datastack/{datastack_name}/query",
    "lookup_supervoxel_ids": mat_v2_api
    + "/materialize/run/lookup_svid/datastack/{datastack_name}/{table_name}",
    "get_views": mat_v3_api + "/datastack/{datastack_name}/version/{version}/views",
    "get_view_metadata": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/views/{view_name}/metadata",
    "view_query": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/views/{view_name}/query",
    "view_schema": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/views/{view_name}/schema",
    "view_schemas": mat_v3_api
    + "/datastack/{datastack_name}/version/{version}/views/schemas",
    "unique_string_values": mat_v3_api
    + "/datastack/{datastack_name}/table/{table_name}/unique_string_values",
}

materialization_api_versions = {
    2: materialization_endpoints_v2,
    3: materialization_endpoints_v3,
}

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
    "datastacks_from_aligned_volume": info_v2
    + "/aligned_volume/{aligned_volume_name}/datastacks",
}

infoservice_api_versions = {1: infoservice_endpoints_v1, 2: infoservice_endpoints_v2}

# -------------------------------
# ------ Pychunkedgraph endpoints
# -------------------------------

pcg_common = "{cg_server_address}/segmentation"
chunkedgraph_endpoints_common = {
    "get_api_versions": pcg_common + "/api/versions",
    "get_version": pcg_common + "/api/version",
    "info": pcg_common + "/table/{table_id}/info",
}

pcg_legacy = "{cg_server_address}/segmentation/1.0"
chunkedgraph_endpoints_legacy = {
    # "handle_table": "{cg_server_address}/segmentation/1.0/table",
    "handle_root": pcg_legacy + "/{table_id}/graph/root",
    "handle_children": pcg_legacy + "/segment/{node_id}/children",
    # "info": pcg_legacy + "/{table_id}/info",
    "leaves_from_root": pcg_legacy + "/{table_id}/segment/{root_id}/leaves",
    "merge_log": pcg_legacy + "/{table_id}/segment/{root_id}/merge_log",
    "change_log": pcg_legacy + "/{table_id}/segment/{root_id}/change_log",
    "contact_sites": pcg_legacy + "/{table_id}/segment/{root_id}/contact_sites",
    "cloudvolume_path": "graphene://" + pcg_legacy + "/{table_id}",
}

pcg_v1 = "{cg_server_address}/segmentation/api/v1"
pcg_meshing_v1 = "{cg_server_address}/meshing/api/v1"
chunkedgraph_endpoints_v1 = {
    "handle_root": pcg_v1 + "/table/{table_id}/node/{supervoxel_id}/root",
    "handle_roots": pcg_v1 + "/table/{table_id}/roots",
    "handle_children": pcg_v1 + "/table/{table_id}/node/{root_id}/children",
    "leaves_from_root": pcg_v1 + "/table/{table_id}/node/{root_id}/leaves",
    "do_merge": pcg_v1 + "/table/{table_id}/merge",
    "get_roots": pcg_v1 + "/table/{table_id}/roots_binary",
    "merge_log": pcg_v1 + "/table/{table_id}/root/{root_id}/merge_log",
    "change_log": pcg_v1 + "/table/{table_id}/root/{root_id}/change_log",
    "tabular_change_log": pcg_v1 + "/table/{table_id}/tabular_change_log_many",
    "contact_sites": pcg_v1 + "/table/{table_id}/node/{root_id}/contact_sites",
    "contact_sites_pairwise": pcg_v1
    + "/table/{table_id}/contact_sites_pair/{root_id_1}/{root_id_2}",
    "cloudvolume_path": "graphene://" + pcg_v1 + "/{table_id}",
    "find_path": pcg_v1 + "/table/{table_id}/graph/find_path",
    "lvl2_graph": pcg_v1 + "/table/{table_id}/node/{root_id}/lvl2_graph",
    "remesh_level2_chunks": pcg_meshing_v1 + "/table/{table_id}/remeshing",
    "get_subgraph": pcg_v1 + "/table/{table_id}/node/{root_id}/subgraph",
    "handle_lineage_graph": pcg_v1 + "/table/{table_id}/lineage_graph_multiple",
    "past_id_mapping": pcg_v1 + "/table/{table_id}/past_id_mapping",
    "operation_details": pcg_v1 + "/table/{table_id}/operation_details",
    "user_operations": pcg_v1 + "/table/{table_id}/user_operations",
    "is_latest_roots": pcg_v1 + "/table/{table_id}/is_latest_roots",
    "root_timestamps": pcg_v1 + "/table/{table_id}/root_timestamps",
    "delta_roots": pcg_v1 + "/table/{table_id}/delta_roots",
    "preview_split": pcg_v1 + "/table/{table_id}/graph/split_preview",
    "valid_nodes": pcg_v1 + "/table/{table_id}/valid_nodes",
    "execute_split": pcg_v1 + "/table/{table_id}/split",
    "undo": pcg_v1 + "/table/{table_id}/undo",
    "oldest_timestamp": pcg_v1 + "/table/{table_id}/oldest_timestamp",
}

chunkedgraph_api_versions = {
    0: chunkedgraph_endpoints_legacy,
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
    "schema_definition": schema_v1 + "/type/{schema_type}",
}
schema_v2 = "{emas_server_address}/schema/api/v2"
schema_endpoints_v2 = {
    "schema": schema_v2 + "/type",
    "schema_definition": schema_v2 + "/type/{schema_type}",
    "schema_definition_multi": schema_v2 + "/types",
    "schema_definition_all": schema_v2 + "/types_all",
}

schema_api_versions = {1: schema_endpoints_v1, 2: schema_endpoints_v2}

# -------------------------------
# ------ StateServer endpoints
# -------------------------------

jsonservice_common = {}

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

json_legacy = "{json_server_address}/nglstate"
jsonservice_endpoints_legacy = {
    "upload_state": json_legacy + "/post",
    "get_state": json_legacy + "/{state_id}",
    "get_state_raw": json_legacy + "/raw/{state_id}",
}

jsonservice_api_versions = {
    0: jsonservice_endpoints_legacy,
    1: jsonservice_endpoints_v1,
}

# -------------------------------
# ------ Auth endpoints
# -------------------------------

auth_common = {}

v1_auth = "{auth_server_address}/auth/api/v1"
auth_endpoints_v1 = {
    "refresh_token": v1_auth + "/refresh_token",
    "create_token": v1_auth + "/create_token",
    "get_tokens": v1_auth + "/user/token",
    "get_users": v1_auth + "/user",
    "get_group_users": v1_auth + "/group/{group_id}/user",
}

auth_api_versions = {
    1: auth_endpoints_v1,
}


# -------------------------------
# ------ L2Cache endpoints
# -------------------------------
l2cache_common = "{l2cache_server_address}/schema"
l2cache_endpoints_common = {
    # "get_api_versions": schema_common + "/versions",
}

l2cache_v1 = "{l2cache_server_address}/l2cache/api/v1"
l2cache_endpoints_v1 = {
    "l2cache_data": l2cache_v1 + "/table/{table_id}/attributes",
    "l2cache_meta": l2cache_v1 + "/attribute_metadata",
    "l2cache_table_mapping": l2cache_v1 + "/table_mapping",
}

l2cache_api_versions = {1: l2cache_endpoints_v1}

# -------------------------------
# ------ Neuroglancer endpoints
# -------------------------------

fallback_ngl_endpoint = "https://neuroglancer.neuvue.io/"
ngl_endpoints_common = {
    "get_info": "{ngl_url}/version.json",
    "fallback_ngl_url": fallback_ngl_endpoint,
}

# -------------------------------
# ------ Skeleton endpoints
# -------------------------------

skeletonservice_common = {}

skeleton_common = "{skeleton_server_address}/skeletoncache/api"
skeleton_v1 = "{skeleton_server_address}/skeletoncache/api/v1"
skeletonservice_endpoints_v1 = {
    "get_version": skeleton_common + "/version",
    "skeleton_info": skeleton_v1 + "/{datastack_name}/precomputed/skeleton/info",
    "get_skeleton_via_rid": skeleton_v1
    + "/{datastack_name}/precomputed/skeleton/{root_id}",
    "get_skeleton_via_skvn_rid": skeleton_v1
    + "/{datastack_name}/precomputed/skeleton/{skeleton_version}/{root_id}",
    "get_skeleton_via_skvn_rid_fmt": skeleton_v1
    + "/{datastack_name}/precomputed/skeleton/{skeleton_version}/{root_id}/{output_format}",
}
skeletonservice_api_versions = {1: skeletonservice_endpoints_v1}
