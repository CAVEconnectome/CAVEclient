import attrs
import warnings
import re
from cachetools import cached, TTLCache, keys

# json schema column types that can act as potential columns for looking at tables
ALLOW_COLUMN_TYPES = ["integer", "boolean", "string", "float"]
# Helper functions for turning schema field names ot column names

def bound_pt_position(pt):
    return f"{pt}_position"

def bound_pt_root_id(pt):
    return f"{pt}_root_id"

def add_with_suffix(namesA, namesB, suffix):
    all_names = []
    rename_map = {}
    for name in namesA:
        if name in namesB:
            new_name = f"{name}{suffix}"
            all_names.append(new_name)
            rename_map[new_name] = name
        else:
            all_names.append(name)
    return all_names, rename_map

def pop_empty(filter_dict):
    keys_to_pop = []
    for k in filter_dict.keys():
        if filter_dict[k] is None:
            keys_to_pop.append(k)
    for k in keys_to_pop:
        filter_dict.pop(k)
    return filter_dict

def combine_names(tableA, namesA, tableB, namesB, suffixes):
    table_map = {}
    final_namesA, rename_mapA = add_with_suffix(namesA, namesB, suffixes[0])
    final_namesB, rename_mapB = add_with_suffix(namesB, namesA, suffixes[1])

    table_map = {name: tableA for name in final_namesA}
    table_map.update({name: tableB for name in final_namesB})

    rename_map = {**rename_mapA, **rename_mapB}

    return final_namesA + final_namesB, table_map, rename_map

def get_all_metadata(client):
    tables = client.materialize.get_tables()
    meta = client.materialize.get_tables_metadata()
    return {
        tn: md
        for tn, md in zip(tables, meta)
    }

def is_list_like(x):
    if isinstance(x, str):
        return False
    try:
        len(x)
        return True
    except:
        return False

def update_spatial_dict(spatial_dict):
    new_dict = {}
    for k in spatial_dict:
        nm = re.match('(.*)_bbox$', k).groups()[0]
        new_dict[nm] = spatial_dict[k]
    return new_dict

def filter_empty(filter_dict):
    new_dict = {}
    for k, v in filter_dict.items():
        if is_list_like(v) and len(v) == 0:
            continue
        new_dict[k] = v
    return new_dict

def replace_empty_with_none(filter_dict):
    if len(filter_dict) == 0:
        return None
    else:
        return filter_dict

_schema_cache = TTLCache(maxsize=128, ttl=86_400)


def _schema_key(schema_name, client, **kwargs):
    allow_types = kwargs.get("allow_types", ALLOW_COLUMN_TYPES)
    key = keys.hashkey(schema_name, str(allow_types))
    return key


@cached(cache=_schema_cache, key=_schema_key)
def get_col_info(
    schema_name,
    client,
    spatial_point="BoundSpatialPoint",
    unbound_spatial_point="SpatialPoint",
    allow_types=ALLOW_COLUMN_TYPES,
    add_fields=["id"],
    omit_fields=[],
):
    schema = client.schema.schema_definition(schema_name)
    sp_name = f"#/definitions/{spatial_point}"
    unbd_sp_name = f"#/definitions/{unbound_spatial_point}"
    n_sp = 0
    sn = schema["$ref"].split("/")[-1]
    add_cols = []
    pt_names = []
    unbnd_pt_names = []
    for k, v in schema["definitions"][sn]["properties"].items():
        if v.get("$ref", "") == sp_name:
            pt_names.append(k)
            n_sp += 1
        elif v.get("$ref", "") == unbd_sp_name:
            unbnd_pt_names.append(k)
        else:
            if k in omit_fields:
                continue
            # Field type is format if exists, type otherwise
            if v.get("format", v.get("type")) in allow_types:
                add_cols.append(k)
    return pt_names, add_fields + add_cols, unbnd_pt_names


_table_cache = TTLCache(maxsize=128, ttl=86_400)

def _table_key(table_name, meta, client, **kwargs):
    merge_schema = kwargs.get("merge_schema", True)
    allow_types = kwargs.get("allow_types", ALLOW_COLUMN_TYPES)
    key = keys.hashkey(table_name, merge_schema, str(allow_types))
    return key


@cached(cache=_table_cache, key=_table_key)
def get_table_info(
    tn, meta, client, allow_types=ALLOW_COLUMN_TYPES, merge_schema=True, suffixes=["", "_ref"]
):
    """Get the point column and additional columns from a table

    Parameters
    ----------
    tn : str
        Table name
    client : CAVEclient
        Client
    omit_cols : list, optional
        List of strings for tables to omit from the list. By default, ['valid', 'get_id']

    Returns
    -------
    pt
        Point column prefix
    cols
        List of additional columns names
    column_map
        Dict mapping columns to table names
    """
    ref_table = meta.get("reference_table")
    if ref_table is None or merge_schema is False:
        schema = meta["schema"]
        ref_pts = []
        ref_cols = []
        ref_unbd_pts = []
        name_base = tn
        name_ref = None
    else:
        schema = table_metadata(ref_table, client).get("schema")
        ref_pts, ref_cols, ref_unbd_pts = get_col_info(
            meta["schema"], client, allow_types=allow_types, omit_fields=["target_id"]
        )
        name_base = ref_table
        name_ref = tn

    base_pts, base_cols, base_unbd_pts = get_col_info(
        schema, client, allow_types=allow_types
    )

    all_pts, pt_map, rename_map_pt = combine_names(
        name_base, base_pts, name_ref, ref_pts, suffixes
    )
    all_vals, val_map, rename_map_val = combine_names(
        name_base, base_cols, name_ref, ref_cols, suffixes
    )
    all_unbd_pts, unbd_pt_map, rename_map_unbd_pt = combine_names(
        name_base, base_unbd_pts, name_ref, ref_unbd_pts, suffixes
    )
    rename_map = {**rename_map_pt, **rename_map_val, **rename_map_unbd_pt}
    column_map = {"id": name_base, **pt_map, **val_map, **unbd_pt_map}
    return (
        all_pts,
        all_vals,
        all_unbd_pts,
        column_map,
        rename_map,
        [name_base, name_ref],
        meta.get('description'),
    )


_metadata_cache = TTLCache(maxsize=128, ttl=86_400)


def _metadata_key(tn, client):
    key = keys.hashkey(tn)
    return key


@cached(cache=_metadata_cache, key=_metadata_key)
def table_metadata(table_name, client):
    "Caches getting table metadata"
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore")
        meta = client.materialize.get_table_metadata(table_name)
    if "schema" not in meta:
        meta["schema"] = meta.get("schema_type")
    return meta


def make_class_vals(pts, val_cols, unbd_pts, table_map, rename_map, table_list):
    class_vals = {
        "_reference_table": attrs.field(
            init=False, default=table_list[1], metadata={"is_meta": True}
        ),
        "_base_table": attrs.field(
            init=False, default=table_list[0], metadata={"is_meta": True}
        ),
    }
    for pt in pts:
        pt_name_orig = rename_map.get(pt, pt)
        class_vals[bound_pt_root_id(pt)] = attrs.field(
            default=None,
            metadata={
                "table": table_map[pt],
                "original_name": bound_pt_root_id(pt_name_orig),
            },
        )
    for val in val_cols:
        class_vals[val] = attrs.field(
            default=None,
            metadata={
                "table": table_map[val],
                "original_name": rename_map.get(val, val),
            },
        )
    for pt in pts + unbd_pts:
        pt_name_orig = rename_map.get(pt, pt)
        class_vals[f"{bound_pt_position(pt)}_bbox"] = attrs.field(
            default=None,
            metadata={
                "is_bbox": True,
                "table": table_map[pt],
                "original_name": f"{bound_pt_position(pt_name_orig)}_bbox",
            },
        )
    return class_vals


def make_kwargs_mixin(client):
    class MakeQueryKwargs(object):
        def __attrs_post_init__(self):
            tables = set(
                [
                    x.metadata.get("table")
                    for x in attrs.fields(type(self))
                    if x.metadata.get("table")
                ]
            )
            filter_equal_dict = {
                tn: filter_empty(attrs.asdict(
                    self,
                    filter=lambda a, v: is_list_like(v) == False
                    and v is not None
                    and a.metadata.get("is_bbox", False) == False
                    and a.metadata.get("is_meta", False) == False
                    and a.metadata.get("table") == tn,
                ))
                for tn in tables
            }
            filter_in_dict = {
                tn: filter_empty(attrs.asdict(
                    self,
                    filter=lambda a, v: is_list_like(v) == True
                    and v is not None
                    and a.metadata.get("is_bbox", False) == False
                    and a.metadata.get("is_meta", False) == False
                    and a.metadata.get("table") == tn,
                ))
                for tn in tables
            }
            spatial_dict = {
                tn: update_spatial_dict(
                        attrs.asdict(
                            self,
                            filter=lambda a, v: a.metadata.get("is_bbox", False)
                            and v is not None
                            and a.metadata.get("is_meta", False) == False
                            and a.metadata.get("table") == tn,
                    )
                )
                for tn in tables
            }

            self.filter_kwargs_live = {
                "filter_equal_dict": replace_empty_with_none(filter_empty(filter_equal_dict)),
                "filter_in_dict": replace_empty_with_none(filter_empty(filter_in_dict)),
                "filter_spatial_dict": replace_empty_with_none(filter_empty(spatial_dict)),
            }
            if len(tables)==2:
                self.filter_kwargs_mat = self.filter_kwargs_live
            else:
                self.filter_kwargs_mat = {
                    k: replace_empty_with_none(self.filter_kwargs_live[k].get(list(tables)[0],[])) 
                    for k in ["filter_equal_dict", "filter_in_dict", "filter_spatial_dict"] if self.filter_kwargs_live[k] is not None
                }
            
            pop_empty(self.filter_kwargs_live)
            pop_empty(self.filter_kwargs_mat)

            if len(tables) == 1:
                self.joins_kwargs = {"joins": None}
                self.basic_join = None
            else:
                self.joins_kwargs = {
                    "joins": [
                        [self._reference_table, "target_id", self._base_table, "id"]
                    ]
                }
                self.basic_join = [
                    [self._reference_table, "target_id"],
                    [self._base_table, "id"],
                ]

        def query(
            self,
            select_columns=None,
            offset=None,
            limit=None,
            split_positions=False,
            materialization_version=None,
            timestamp=None,
            metadata=True,
            desired_resolution=None,
            get_counts=False,
        ):
            if self._reference_table is None:
                qry_table = self._base_table
                return client.materialize.query_table(
                    qry_table,
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    split_positions=split_positions,
                    materialization_version=materialization_version,
                    desired_resolution=desired_resolution,
                    timestamp=timestamp,
                    get_counts=get_counts,
                    metadata=metadata,
                    **self.filter_kwargs_mat,
                )
            else:
                qry_table = self._reference_table
                return client.materialize.join_query(
                    tables=self.basic_join,
                    select_columns=select_columns,
                    offset=offset,
                    limit=limit,
                    split_positions=split_positions,
                    materialization_version=materialization_version,
                    desired_resolution=desired_resolution,
                    suffixes={self._reference_table: "_ref", self._base_table: ""},
                    metadata=metadata,
                    **self.filter_kwargs_mat,
                )

        def live_query(
            self,
            timestamp,
            offset=None,
            limit=None,
            split_positions=False,
            metadata=True,
            desired_resolution=None,
            allow_missing_lookups=False,
        ):
            if self._reference_table is None:
                qry_table = self._base_table
                return client.materialize.live_live_query(
                    table=qry_table,
                    timestamp=timestamp,
                    offset=offset,
                    limit=limit,
                    split_positions=split_positions,
                    desired_resolution=desired_resolution,
                    allow_missing_lookups=allow_missing_lookups,
                    metadata=metadata,
                    **self.filter_kwargs_live,
                )
            else:
                qry_table = self._reference_table
                return client.materialize.live_live_query(
                    table=qry_table,
                    timestamp=timestamp,
                    offset=offset,
                    limit=limit,
                    split_positions=split_positions,
                    desired_resolution=desired_resolution,
                    suffixes={self._reference_table: "_ref", self._base_table: ""},
                    allow_missing_lookups=allow_missing_lookups,
                    metadata=metadata,
                    **self.filter_kwargs_live,
                    **self.joins_kwargs,
                )

    return MakeQueryKwargs


def make_query_filter(table_name, meta, client):
    pts, val_cols, all_unbd_pts, table_map, rename_map, table_list, desc = get_table_info(
        table_name, meta, client
    )
    class_vals = make_class_vals(
        pts, val_cols, all_unbd_pts, table_map, rename_map, table_list
    )
    QueryFilter = attrs.make_class(
        table_name, class_vals, bases=(make_kwargs_mixin(client),)
    )
    QueryFilter.__doc__ = desc
    return QueryFilter


class TableManager(object):
    """Use schema definitions to generate query filters for each table.
    """
    def __init__(self, client):
        self._client = client
        self._table_metadata = get_all_metadata(self._client)
        self._tables = sorted(list(self._table_metadata.keys()))
        for tn in self._tables:
            setattr(self, tn, make_query_filter(tn, self._table_metadata[tn], client)) 

    def __getitem__(self, key):
        return getattr(self, key)
    
    def __repr__(self):
        return str(self._tables)