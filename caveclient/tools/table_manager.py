import datetime
import logging
import re
import warnings
from itertools import chain
from typing import Optional

import attrs
from cachetools import TTLCache, cached, keys

logger = logging.getLogger(__name__)

# json schema column types that can act as potential columns for looking at tables
ALLOW_COLUMN_TYPES = ["integer", "boolean", "string", "float"]
NUMERIC_COLUMN_TYPES = ["integer", "float"]
SPATIAL_POINT_TYPES = ["SpatialPoint"]

# Helper functions for turning schema field names ot column names


class InvalidInequalityException(Exception):
    pass


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


def get_all_table_metadata(client, meta=None):
    if meta is None:
        meta = client.materialize.get_tables_metadata()
    tables = []
    for m in meta:
        if m.get("annotation_table"):
            tables.append(m["annotation_table"])
        else:
            tables.append(m["table_name"])
    return {tn: md for tn, md in zip(tables, meta)}


def get_all_view_metadata(client):
    views = client.materialize.get_views()
    view_schema = client.materialize.get_view_schemas()
    return views, view_schema


def _parse_inequality(x, ineq):
    if isinstance(x, dict):
        return ineq in x
    else:
        return False


def is_equal_like(x):
    return not is_list_like(x)


def is_list_like(x):
    if isinstance(x, str):
        return False
    if hasattr(x, "__len__"):
        return True
    else:
        return False


def is_isin_like(x):
    return is_list_like(x) and not isinstance(x, dict)


def is_lessthan(x):
    return _parse_inequality(x, "<")


def is_greaterthan(x):
    return _parse_inequality(x, ">")


def is_lessthan_equal(x):
    return _parse_inequality(x, "<=")


def is_greaterthan_equal(x):
    return _parse_inequality(x, ">=")


def update_spatial_dict(spatial_dict):
    new_dict = {}
    for k in spatial_dict:
        nm = re.match("(.*)_bbox$", k).groups()[0]
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


def populate_schema_cache(client, schema_definitions=None):
    if schema_definitions is None:
        schema_definitions = client.schema.schema_definition_all()
        if schema_definitions is None:
            schema_definitions = {sn: None for sn in client.schema.get_schemas()}
    for schema_name, schema_definition in schema_definitions.items():
        get_col_info(schema_name, client, schema_definition=schema_definition)


def populate_table_cache(client, metadata=None):
    if metadata is None:
        metadata = get_all_table_metadata(client)
    for tn, meta in metadata.items():
        table_metadata(tn, client, meta=meta)


@cached(cache=_schema_cache, key=_schema_key)
def get_col_info(
    schema_name,
    client,
    spatial_point="BoundSpatialPoint",
    unbound_spatial_point="SpatialPoint",
    allow_types=ALLOW_COLUMN_TYPES,
    add_fields=["id"],
    omit_fields=[],
    schema_definition=None,
    numeric_types=NUMERIC_COLUMN_TYPES,
):
    nonnumeric_types = [t for t in allow_types if t not in numeric_types]
    numeric_types = [t for t in allow_types if t in numeric_types]
    if schema_definition is None:
        schema = client.schema.schema_definition(schema_name)
    else:
        schema = schema_definition.copy()
    sp_name = f"#/definitions/{spatial_point}"
    unbd_sp_name = f"#/definitions/{unbound_spatial_point}"
    n_sp = 0
    sn = schema["$ref"].split("/")[-1]
    add_cols = []
    pt_names = []
    unbnd_pt_names = []
    numeric_cols = []
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
            if v.get("format", v.get("type")) in nonnumeric_types:
                add_cols.append(k)
            if v.get("format", v.get("type")) in numeric_types:
                numeric_cols.append(k)
    return pt_names, add_fields + add_cols, numeric_cols, unbnd_pt_names


_table_cache = TTLCache(maxsize=128, ttl=86_400)


def _table_key(table_name, meta, client, **kwargs):
    merge_schema = kwargs.get("merge_schema", True)
    allow_types = kwargs.get("allow_types", ALLOW_COLUMN_TYPES)
    key = keys.hashkey(table_name, merge_schema, str(allow_types))
    return key


def get_view_info(
    view_name,
    meta,
    schema,
    allow_types=ALLOW_COLUMN_TYPES,
    spatial_types=SPATIAL_POINT_TYPES,
):
    """Assemble

    Parameters
    ----------
    view_name : _type_
        _description_
    meta : _type_
        _description_
    schema : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    desc = meta.get("description", "")
    is_live = meta.get("live_compatible", False)
    pts = []
    vals = [k for k, v in schema.items() if v["type"] in allow_types]
    unbd_pts = [k for k, v in schema.items() if v["type"] in spatial_types]
    column_map = {k: view_name for k in vals + unbd_pts}
    rename_map = {}
    return (
        pts,
        vals,
        unbd_pts,
        column_map,
        rename_map,
        [view_name, None],
        desc,
        is_live,
    )


@cached(cache=_table_cache, key=_table_key)
def get_table_info(
    tn,
    meta,
    client,
    allow_types=ALLOW_COLUMN_TYPES,
    merge_schema=True,
    suffixes=["", "_ref"],
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
    if ref_table is not None:
        if len(ref_table) == 0:
            ref_table = None
    if ref_table is None or merge_schema is False:
        schema = meta["schema"]
        ref_pts = []
        ref_cols = []
        ref_numeric = []
        ref_unbd_pts = []
        name_base = tn
        name_ref = None
    else:
        schema = table_metadata(ref_table, client).get("schema")
        ref_pts, ref_cols, ref_numeric, ref_unbd_pts = get_col_info(
            meta["schema"], client, allow_types=allow_types, omit_fields=["target_id"]
        )
        name_base = ref_table
        name_ref = tn

    base_pts, base_cols, base_numeric, base_unbd_pts = get_col_info(
        schema, client, allow_types=allow_types
    )

    all_pts, pt_map, rename_map_pt = combine_names(
        name_base, base_pts, name_ref, ref_pts, suffixes
    )
    all_vals, val_map, rename_map_val = combine_names(
        name_base, base_cols + base_numeric, name_ref, ref_cols + ref_numeric, suffixes
    )

    numeric_vals = []
    for k in all_vals:
        if val_map[k] == name_base:
            if k in base_numeric:
                numeric_vals.append(k)
        elif val_map[k] == name_ref:
            if k in ref_numeric:
                numeric_vals.append(k)

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
        meta.get("description"),
        numeric_vals,
    )


_metadata_cache = TTLCache(maxsize=128, ttl=86_400)


def _metadata_key(tn, client, **kwargs):
    key = keys.hashkey(tn)
    return key


@cached(cache=_metadata_cache, key=_metadata_key)
def table_metadata(table_name, client, meta=None):
    "Caches getting table metadata"
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore")
        if meta is None:
            meta = client.materialize.get_table_metadata(table_name)
    if "schema" not in meta:
        meta["schema"] = meta.get("schema_type")
    return meta


def make_class_vals(
    pts,
    val_cols,
    unbd_pts,
    table_map,
    rename_map,
    table_list,
    numeric_vals,
    raw_points=False,
):
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
                "is_numeric": val in numeric_vals,
            },
        )
    for pt in pts + unbd_pts:
        pt_name_orig = rename_map.get(pt, pt)
        if raw_points:
            bbox_name_orig = pt_name_orig
            bbox_name = pt
        else:
            bbox_name_orig = f"{bound_pt_position(pt_name_orig)}_bbox"
            bbox_name = f"{bound_pt_position(pt)}_bbox"
        class_vals[bbox_name] = attrs.field(
            default=None,
            metadata={
                "is_bbox": True,
                "table": table_map[pt],
                "original_name": bbox_name_orig,
            },
        )
    return class_vals


def rename_fields(filter_dict, cls):
    rename_map = {}
    for a in attrs.fields(type(cls)):
        if a.metadata.get("original_name"):
            rename_map[a.name] = a.metadata["original_name"]
    fix_dict = {}
    for tn in filter_dict:
        for k in filter_dict[tn]:
            if k in rename_map:
                fix_dict[tn] = k
    for tn, k in fix_dict.items():
        filter_dict[tn][rename_map[k]] = filter_dict[tn].pop(k)
    return filter_dict


def make_kwargs_mixin(client, is_view=False, live_compatible=True):
    class BaseQueryKwargs(object):
        def __attrs_post_init__(self):
            tables = set(
                [
                    x.metadata.get("table")
                    for x in attrs.fields(type(self))
                    if x.metadata.get("table")
                ]
            )
            filter_equal_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_equal_like(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False  # noqa E712
                        and a.metadata.get("is_meta", False) is False  # noqa E712
                        and a.metadata.get("table") == tn,
                    )
                )
                for tn in tables
            }
            filter_equal_dict = rename_fields(filter_equal_dict, self)

            filter_in_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_isin_like(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False  # noqa E712
                        and a.metadata.get("is_meta", False) is False  # noqa E712
                        and a.metadata.get("table") == tn,
                    )
                )
                for tn in tables
            }
            filter_in_dict = rename_fields(filter_in_dict, self)

            filter_lt_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_lessthan(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False
                        and a.metadata.get("is_meta", False) is False
                        and a.metadata.get("is_numeric", False) is True
                        and a.metadata.get("table") == tn,
                        value_serializer=lambda _, __, v: v.get("<"),
                    )
                )
                for tn in tables
            }
            filter_lt_dict = rename_fields(filter_lt_dict, self)

            filter_gt_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_greaterthan(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False
                        and a.metadata.get("is_meta", False) is False
                        and a.metadata.get("is_numeric", False) is True
                        and a.metadata.get("table") == tn,
                        value_serializer=lambda _, __, v: v.get(">"),
                    )
                )
                for tn in tables
            }
            filter_gt_dict = rename_fields(filter_gt_dict, self)

            filter_geq_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_greaterthan_equal(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False
                        and a.metadata.get("is_meta", False) is False
                        and a.metadata.get("is_numeric", False) is True
                        and a.metadata.get("table") == tn,
                        value_serializer=lambda _, __, v: v.get(">="),
                    )
                )
                for tn in tables
            }
            filter_geq_dict = rename_fields(filter_geq_dict, self)

            filter_leq_dict = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: is_lessthan_equal(v)
                        and v is not None
                        and a.metadata.get("is_bbox", False) is False
                        and a.metadata.get("is_meta", False) is False
                        and a.metadata.get("is_numeric", False) is True
                        and a.metadata.get("table") == tn,
                        value_serializer=lambda _, __, v: v.get("<="),
                    )
                )
                for tn in tables
            }
            filter_leq_dict = rename_fields(filter_leq_dict, self)

            spatial_dict = {
                tn: update_spatial_dict(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: a.metadata.get("is_bbox", False)
                        and v is not None
                        and a.metadata.get("is_meta", False) is False  # noqa E712
                        and a.metadata.get("table") == tn,
                    )
                )
                for tn in tables
            }
            spatial_dict = rename_fields(spatial_dict, self)

            invalid_inequality_queries = {
                tn: filter_empty(
                    attrs.asdict(
                        self,
                        filter=lambda a, v: (
                            is_greaterthan(v)
                            or is_greaterthan_equal(v)
                            or is_lessthan(v)
                            or is_lessthan_equal(v)
                        )
                        and a.metadata.get("table") == tn
                        and a.metadata.get("is_numeric", False) is False,
                    )
                )
                for tn in tables
            }

            if sum([len(v) for k, v in invalid_inequality_queries.items()]) > 0:
                bad_fields = [
                    list(v.keys())
                    for k, v in invalid_inequality_queries.items()
                    if len(v) > 0
                ]
                msg = f"Cannot use inequality for non-numeric fields: {list(chain.from_iterable(bad_fields))}"
                raise InvalidInequalityException(msg)

            self.filter_kwargs_live = {
                "filter_equal_dict": replace_empty_with_none(
                    filter_empty(filter_equal_dict)
                ),
                "filter_in_dict": replace_empty_with_none(filter_empty(filter_in_dict)),
                "filter_spatial_dict": replace_empty_with_none(
                    filter_empty(spatial_dict)
                ),
                "filter_greater_dict": replace_empty_with_none(
                    filter_empty(filter_gt_dict)
                ),
                "filter_less_dict": replace_empty_with_none(
                    filter_empty(filter_lt_dict)
                ),
                "filter_greater_equal_dict": replace_empty_with_none(
                    filter_empty(filter_geq_dict)
                ),
                "filter_less_equal_dict": replace_empty_with_none(
                    filter_empty(filter_leq_dict)
                ),
            }
            if len(tables) == 2:
                self.filter_kwargs_mat = self.filter_kwargs_live
            else:
                self.filter_kwargs_mat = {
                    k: replace_empty_with_none(
                        self.filter_kwargs_live[k].get(list(tables)[0], [])
                    )
                    for k in [
                        "filter_equal_dict",
                        "filter_in_dict",
                        "filter_spatial_dict",
                        "filter_greater_dict",
                        "filter_less_dict",
                        "filter_greater_equal_dict",
                        "filter_less_equal_dict",
                    ]
                    if self.filter_kwargs_live[k] is not None
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

    if not is_view:

        class TableQueryKwargs(BaseQueryKwargs):
            def query(
                self,
                select_columns: Optional[list] = None,
                offset: Optional[int] = None,
                limit: Optional[int] = None,
                split_positions: bool = False,
                materialization_version: Optional[int] = None,
                timestamp: Optional[datetime.datetime] = None,
                metadata: bool = True,
                desired_resolution: Optional[list] = None,
                get_counts: bool = False,
                log_warning: bool = True,
                random_sample: Optional[int] = None,
            ):
                """Set data return options for the specified query

                Parameters
                ----------
                select_columns : list, optional
                    List of columns to be returned, by default None
                offset : int, optional
                    Sets how many rows to skip before starting to return results, by default None
                limit : int, optional
                    Sets the total number of results returned, by default None
                split_positions : bool, optional
                    If True, leaves position data in separate columns by componnet, by default False
                materialization_version : int, optional
                    Specifies a non-default materialization version, by default None
                timestamp : datetime.datetime, optional
                    Sets the timestamp to look up root ids at, by default None
                metadata : bool, optional
                    Whether to return table and query metadata under the `.attrs` property, by default True
                desired_resolution : list, optional
                    A three element vector setting the return resolution of position information, by default None
                get_counts : bool, optional
                    If True, only return the number of rows that match the query, by default False
                log_warning : bool, optional
                    Whether to log warnings associated with tables
                random_sample: int, optional
                    If set, will return a random sample of the data, by default None

                Returns
                -------
                pd.DataFrame
                    Query data
                """
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
                        log_warning=log_warning,
                        random_sample=random_sample,
                        **self.filter_kwargs_mat,
                    )
                elif timestamp is None:
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
                        log_warning=log_warning,
                        random_sample=random_sample,
                        **self.filter_kwargs_mat,
                    )
                else:
                    return self.live_query(
                        timestamp=timestamp,
                        offset=offset,
                        limit=limit,
                        split_positions=split_positions,
                        metadata=metadata,
                        desired_resolution=desired_resolution,
                        allow_missing_lookups=False,
                        log_warning=log_warning,
                        random_sample=random_sample,
                        **self.filter_kwargs_mat,
                    )

            def live_query(
                self,
                timestamp: datetime.datetime,
                offset: Optional[int] = None,
                limit: Optional[int] = None,
                split_positions: bool = False,
                metadata: bool = True,
                desired_resolution: Optional[list] = None,
                allow_missing_lookups: bool = False,
                log_warning: bool = True,
                random_sample: Optional[int] = None,
            ):
                """Set data return options for the specified live query

                Parameters
                ----------
                timestamp :
                offset : int, optional
                    Sets how many rows to skip before starting to return results, by default None
                limit : int, optional
                    Sets the total number of results returned, by default None
                split_positions : bool, optional
                    If True, leaves position data in separate columns by componnet, by default False
                metadata : bool, optional
                    Whether to return table and query metadata under the `.attrs` property, by default True
                desired_resolution : list, optional
                    A three element vector setting the return resolution of position information, by default None
                allow_missing_lookups: bool, optional
                    If True, will return values even if the database is still ingesting new information, by default False.
                    IMPORTANT: If set to True, the database could return different answers to the same query at the same timestamp.
                    Do not set to True if writing code you intended to give consistent answers every time.
                log_warning : bool, optional
                    Whether to log warnings associated with tables
                random_sample: int, optional
                    If set, will return a random sample of the data, by default None

                Returns
                -------
                pd.DataFrame
                    Query data
                """

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
                        log_warning=log_warning,
                        random_sample=random_sample,
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
                        log_warning=log_warning,
                        random_sample=random_sample,
                        **self.filter_kwargs_live,
                        **self.joins_kwargs,
                    )

        return TableQueryKwargs
    else:

        class ViewQueryKwargs(BaseQueryKwargs):
            def query(
                self,
                select_columns: Optional[list] = None,
                offset: Optional[int] = None,
                limit: Optional[int] = None,
                split_positions: bool = False,
                materialization_version: Optional[int] = None,
                metadata: bool = True,
                desired_resolution: Optional[list] = None,
                get_counts: bool = False,
                log_warning: bool = True,
                random_sample: Optional[int] = None,
            ):
                """Query views through the table interface

                Parameters
                ----------
                select_columns : list[str], optional
                    Specification of columns to return, by default None
                offset : int, optional
                    Integer offset from the beginning of the table to return, by default None.
                    Used when tables are too large to return in one query.
                limit : int, optional
                    Maximum number of rows to return, by default None
                split_positions : bool, optional
                    If true, returns each point coordinate as a separate column, by default False
                materialization_version : int, optional
                    Query a specified materialization version, by default None
                metadata : bool, optional
                    If true includes query and table metadata in the .attrs property of the returned dataframe, by default True
                desired_resolution : list[int], optional
                    Sets the 3d point resolution in nm, by default None.
                    If default, uses the values in the table directly.
                get_counts : bool, optional
                    Only return number of rows in the query, by default False
                log_warning : bool, optional
                    Whether to log warnings associated with tables
                random_sample: int, optional
                    If set, will return a random sample of the data, by default None
                """
                return client.materialize.query_view(
                    self._base_table,
                    metadata=metadata,
                    desired_resolution=desired_resolution,
                    materialization_version=materialization_version,
                    split_positions=split_positions,
                    limit=limit,
                    offset=offset,
                    select_columns=select_columns,
                    get_counts=get_counts,
                    random_sample=random_sample,
                    **self.filter_kwargs_mat,
                )

    return ViewQueryKwargs


def make_query_filter(table_name, meta, client):
    (
        pts,
        val_cols,
        all_unbd_pts,
        table_map,
        rename_map,
        table_list,
        desc,
        numeric_vals,
    ) = get_table_info(table_name, meta, client)

    class_vals = make_class_vals(
        pts, val_cols, all_unbd_pts, table_map, rename_map, table_list, numeric_vals
    )
    QueryFilter = attrs.make_class(
        table_name, class_vals, bases=(make_kwargs_mixin(client),)
    )
    QueryFilter.__doc__ = desc
    setattr(QueryFilter, "get_all", QueryFilter().query)
    setattr(QueryFilter, "get_all_live", QueryFilter().live_query)

    fields = [
        x.name
        for x in attrs.fields(QueryFilter)
        if not x.metadata.get("is_meta", False)
    ]
    setattr(QueryFilter, "fields", fields)
    numeric_fields = [
        x.name for x in attrs.fields(QueryFilter) if x.metadata.get("is_numeric", False)
    ]
    setattr(QueryFilter, "numeric_fields", numeric_fields)
    spatial_fields = [
        x.name for x in attrs.fields(QueryFilter) if x.metadata.get("is_bbox", False)
    ]
    setattr(QueryFilter, "spatial_fields", spatial_fields)

    return QueryFilter


def make_query_filter_view(view_name, meta, schema, client):
    (
        pts,
        val_cols,
        all_unbd_pts,
        table_map,
        rename_map,
        table_list,
        desc,
        live_compatible,
    ) = get_view_info(view_name, meta, schema)

    numeric_vals = [k for k, v in schema.items() if v["type"] in NUMERIC_COLUMN_TYPES]

    class_vals = make_class_vals(
        pts, val_cols, all_unbd_pts, table_map, rename_map, table_list, numeric_vals
    )

    ViewQueryFilter = attrs.make_class(
        view_name,
        class_vals,
        bases=(
            make_kwargs_mixin(client, is_view=True, live_compatible=live_compatible),
        ),
    )
    ViewQueryFilter.__doc__ = desc

    setattr(ViewQueryFilter, "get_all", ViewQueryFilter().query)

    fields = [
        x.name
        for x in attrs.fields(ViewQueryFilter)
        if not x.metadata.get("is_meta", False)
    ]
    setattr(ViewQueryFilter, "fields", fields)
    numeric_fields = [
        x.name
        for x in attrs.fields(ViewQueryFilter)
        if x.metadata.get("is_numeric", False)
    ]
    setattr(ViewQueryFilter, "numeric_fields", numeric_fields)
    spatial_fields = [
        x.name
        for x in attrs.fields(ViewQueryFilter)
        if x.metadata.get("is_bbox", False)
    ]
    setattr(ViewQueryFilter, "spatial_fields", spatial_fields)

    return ViewQueryFilter


class BaseManager:
    """Base class for managers that handle query filters for tables and views."""

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        return key in self._tables

    def __repr__(self) -> str:
        return str(self._tables)

    def _repr_html_(self) -> str:
        """Generates an HTML representation for viewing tables in interactive environments."""
        html_string = """<html>
            <head></head>
            <body>
                """
        for table_name in self.table_names:
            value = self._table_metadata[table_name]
            html_string += f"""
                <details>
                    <p>{value["description"]}</p>
                    <summary>{table_name}</summary>
                </details>
                """
        html_string += """
            </body>
        </html>"""
        return html_string

    def _repr_mimebundle_(self, include=None, exclude=None):
        """Somehow necessary for IPython to detect _repr_html_ for subclasses."""
        return {"text/html": self._repr_html_()}, {}

    @property
    def table_names(self):
        return self._tables

    def find(self, search_str):
        """Find tables that contain a string. Case is ignored.

        Parameters
        ----------
        search_str : str


        Returns
        -------
        list
            Table names with that string
        """
        return [tn for tn in self.table_names if search_str.lower() in tn.lower()]

    def __len__(self):
        return len(self._tables)


class TableManager(BaseManager):
    def __init__(self, client, metadata=None, schema=None):
        self._client = client
        self._table_metadata = get_all_table_metadata(self._client, meta=metadata)
        self._tables = sorted(list(self._table_metadata.keys()))
        populate_schema_cache(client, schema_definitions=schema)
        populate_table_cache(client, metadata=self._table_metadata)
        for tn in self._tables:
            setattr(self, tn, make_query_filter(tn, self._table_metadata[tn], client))


class ViewManager(BaseManager):
    def __init__(self, client, view_metadata=None, view_schema=None):
        self._client = client
        if view_metadata is None or view_schema is None:
            view_metadata, view_schema = get_all_view_metadata(self._client)
        else:
            self._table_metadata = view_metadata
        self._tables = sorted(list(self._table_metadata.keys()))
        for vn in self._tables:
            setattr(
                self,
                vn,
                make_query_filter_view(
                    vn, self._table_metadata[vn], view_schema[vn], client
                ),
            )
