from unittest.mock import MagicMock

import pytest

from caveclient.query import FilterKind
from caveclient.query.tables import (
    TableManager,
    TableQuery,
    ViewManager,
    _Part,
    classify_table_schema,
    classify_view_schema,
)

# a synapse-like annotation schema (JSON-schema form)
SYNAPSE_SCHEMA = {
    "$ref": "#/definitions/Synapse",
    "definitions": {
        "Synapse": {
            "properties": {
                "pre_pt": {"$ref": "#/definitions/BoundSpatialPoint"},
                "post_pt": {"$ref": "#/definitions/BoundSpatialPoint"},
                "ctr_pt": {"$ref": "#/definitions/SpatialPoint"},
                "size": {"type": "float"},
                "tag": {"type": "string"},
                "valid": {"type": ["boolean", "null"]},
            }
        }
    },
}


class TestClassifyTableSchema:
    def test_expands_points_and_scalars(self):
        kinds = classify_table_schema(SYNAPSE_SCHEMA)
        assert kinds["id"] is FilterKind.NUMERIC
        assert kinds["pre_pt_position"] is FilterKind.POSITION
        assert kinds["pre_pt_root_id"] is FilterKind.ID
        assert kinds["pre_pt_supervoxel_id"] is FilterKind.ID
        assert kinds["ctr_pt_position"] is FilterKind.POSITION
        assert "ctr_pt_root_id" not in kinds  # unbound point: position only
        assert kinds["size"] is FilterKind.NUMERIC
        assert kinds["tag"] is FilterKind.STRING
        assert kinds["valid"] is FilterKind.BOOLEAN  # handles ["boolean","null"]


class TestClassifyViewSchema:
    def test_name_and_type_heuristics(self):
        kinds = classify_view_schema(
            {
                "root_id": {"type": "integer"},
                "n_input": {"type": "integer"},
                "pt_position": {"type": "array"},
                "cell_type": {"type": "string"},
            }
        )
        assert kinds["root_id"] is FilterKind.ID
        assert kinds["n_input"] is FilterKind.NUMERIC
        assert kinds["pt_position"] is FilterKind.POSITION
        assert kinds["cell_type"] is FilterKind.STRING


def make_tq(client=None, name="synapses", kind="table"):
    part = _Part(
        name=name, kind=kind, column_kinds=classify_table_schema(SYNAPSE_SCHEMA)
    )
    return TableQuery(client or MagicMock(), part, description="a synapse table")


class TestTableQueryFiltering:
    def test_kwarg_filter_builds_table(self):
        client = MagicMock()
        make_tq(client)(pre_pt_root_id=[1, 2], size__gt=100).query(version=3)
        tables = client.query.call_args.args[0]
        assert len(tables) == 1
        t = tables[0]
        assert t.name == "synapses"
        assert t.filter_in == {"pre_pt_root_id": [1, 2]}
        assert t.filter_greater == {"size": 100}

    def test_handle_expression_filter(self):
        client = MagicMock()
        syn = make_tq(client)
        syn.query(syn.size > 100, syn.pre_pt_root_id.isin([7]), version=3)
        t = client.query.call_args.args[0][0]
        assert t.filter_greater == {"size": 100}
        assert t.filter_in == {"pre_pt_root_id": [7]}

    def test_kwargs_and_exprs_combine(self):
        client = MagicMock()
        syn = make_tq(client)
        syn(size__lt=500).query(syn.pre_pt_root_id == 7, version=3)
        t = client.query.call_args.args[0][0]
        assert t.filter_less == {"size": 500}
        assert t.filter_equal == {"pre_pt_root_id": 7}

    def test_select_lands_on_table(self):
        client = MagicMock()
        make_tq(client).query(select=["size", "pre_pt_root_id"], version=3)
        t = client.query.call_args.args[0][0]
        assert t.select == ["size", "pre_pt_root_id"]

    def test_select_method_is_per_table_in_joins(self):
        client = MagicMock()
        syn = make_tq(client, name="synapses")
        nuc = make_tq(client, name="nuclei")
        syn.select("size").join(nuc.select("tag"), on="post_pt_root_id").query(
            version=3
        )
        tables = {t.name: t for t in client.query.call_args.args[0]}
        assert tables["synapses"].select == ["size"]
        assert tables["nuclei"].select == ["tag"]

    def test_query_options_passed_through(self):
        client = MagicMock()
        make_tq(client).query(version=3, limit=10, split_positions=True)
        kwargs = client.query.call_args.kwargs
        assert kwargs["version"] == 3
        assert kwargs["limit"] == 10
        assert kwargs["split_positions"] is True

    def test_live_query_is_timestamp_query(self):
        client = MagicMock()
        make_tq(client).live_query("TS")
        assert client.query.call_args.kwargs["timestamp"] == "TS"


class TestTableQueryJoin:
    def test_join_builds_two_tables_with_join_cols(self):
        client = MagicMock()
        syn = make_tq(client, name="synapses")
        nuc = make_tq(client, name="nuclei")
        joined = syn(size__gt=100).join(nuc, on=("post_pt_root_id", "pre_pt_root_id"))
        joined.query(version=3)
        tables = client.query.call_args.args[0]
        assert [t.name for t in tables] == ["synapses", "nuclei"]
        assert tables[0].join_on == "post_pt_root_id"
        assert tables[1].join_on == "pre_pt_root_id"
        # the synapses filter stays on the synapses table
        assert tables[0].filter_greater == {"size": 100}

    def test_join_routes_filters_by_table(self):
        client = MagicMock()
        syn = make_tq(client, name="synapses")
        nuc = make_tq(client, name="nuclei")
        # filter from each table's own handle
        joined = syn.join(nuc, on="post_pt_root_id")
        joined.query(syn.size > 1, nuc.size < 9, version=3)
        tables = {t.name: t for t in client.query.call_args.args[0]}
        assert tables["synapses"].filter_greater == {"size": 1}
        assert tables["nuclei"].filter_less == {"size": 9}


REF_SCHEMA = {
    "$ref": "#/definitions/CellTypeReference",
    "definitions": {
        "CellTypeReference": {
            "properties": {
                "cell_type": {"type": "string"},
                "target_id": {"type": "integer"},
            }
        }
    },
}


def make_reference_tq(client=None):
    # an annotation table whose reference table is "nuc" (synapse-like schema)
    ref_kinds = classify_table_schema(SYNAPSE_SCHEMA)
    part = _Part(
        name="mtypes",
        kind="table",
        column_kinds=classify_table_schema(REF_SCHEMA),
        merge_reference=True,
        reference=("nuc", ref_kinds),
    )
    return TableQuery(client or MagicMock(), part, description="ref table")


class TestReferenceColumns:
    def test_reference_columns_are_filterable(self):
        tq = make_reference_tq()
        # own columns and the reference table's columns are both exposed
        assert tq.columns["cell_type"] is FilterKind.STRING  # own
        assert tq.columns["pre_pt_root_id"] is FilterKind.ID  # from reference

    def test_primary_only_filter_stays_single_table(self):
        client = MagicMock()
        make_reference_tq(client)(cell_type="L2a").query(version=3)
        tables = client.query.call_args.args[0]
        # no reference column filtered -> single table, query_table merges the ref
        assert [t.name for t in tables] == ["mtypes"]
        assert tables[0].merge_reference is True

    def test_reference_column_filter_builds_join(self):
        client = MagicMock()
        # filter a reference-table column -> auto-join the reference
        make_reference_tq(client)(pre_pt_root_id=[1, 2]).query(version=3)
        tables = {t.name: t for t in client.query.call_args.args[0]}
        assert set(tables) == {"mtypes", "nuc"}
        assert tables["mtypes"].join_on == "target_id"
        assert tables["nuc"].join_on == "id"
        assert tables["nuc"].suffix == "_ref"
        # the filter routed to the reference table; primary no longer auto-merges
        assert tables["nuc"].filter_in == {"pre_pt_root_id": [1, 2]}
        assert tables["mtypes"].merge_reference is False

    def test_colliding_reference_column_exposed_as_ref(self):
        # `id` exists on every table, so the reference's `id` collides and the
        # server returns it as `id_ref`; it must be filterable under that name.
        tq = make_reference_tq()
        assert tq.columns["id"] is FilterKind.NUMERIC  # primary's id
        assert tq.columns["id_ref"] is FilterKind.NUMERIC  # reference's id
        # the handle carries the real wire name and the reference table
        assert tq.id_ref.name == "id"
        assert tq.id_ref.table == "nuc"

    def test_colliding_reference_column_filter_routes_to_real_name(self):
        client = MagicMock()
        # filter `id_ref` -> reference join, filter on the reference's real `id`
        make_reference_tq(client)(id_ref=[5, 6]).query(version=3)
        tables = {t.name: t for t in client.query.call_args.args[0]}
        assert set(tables) == {"mtypes", "nuc"}
        assert tables["nuc"].filter_in == {"id": [5, 6]}
        # primary's own id is unaffected and still routes to the primary
        client.reset_mock()
        make_reference_tq(client)(id=9).query(version=3)
        only = client.query.call_args.args[0]
        assert [t.name for t in only] == ["mtypes"]
        assert only[0].filter_equal == {"id": 9}


class TestTableQueryIntrospection:
    def test_columns_and_handles(self):
        syn = make_tq()
        assert syn.columns["size"] is FilterKind.NUMERIC
        assert "size" in dir(syn)
        assert syn.size.kind is FilterKind.NUMERIC
        assert syn.__doc__ == "a synapse table"

    def test_unknown_column_attribute_errors(self):
        with pytest.raises(AttributeError):
            make_tq().nonexistent_column

    def test_column_handle_by_item(self):
        syn = make_tq()
        assert syn["size"].kind is FilterKind.NUMERIC
        with pytest.raises(KeyError, match="has no column"):
            syn["nope"]

    def test_signature_exposes_columns(self):
        # shift-tab / help() should show the columns, not an opaque **kwargs
        import inspect

        sig = inspect.signature(make_tq())
        assert sig.parameters["size"].kind is inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters["size"].annotation == "numeric"
        assert sig.parameters["size"].default is None
        assert sig.parameters["pre_pt_root_id"].annotation == "id"
        # trailing **kwargs signals col__op= forms are also accepted
        assert any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )


def make_table_manager(client=None):
    # get_tables_metadata returns a list of dicts, each with table_name
    tables_metadata = [
        {
            "table_name": "synapses_pni_2",
            "schema_type": "synapse",
            "description": "synapses",
        },
        {"table_name": "nuclei", "schema_type": "synapse", "description": "nuclei"},
    ]
    schemas = {"synapse": SYNAPSE_SCHEMA}
    fc = MagicMock()
    fc.materialize = client or MagicMock()
    return TableManager.build(fc, tables_metadata, schemas)


class TestAccessor:
    def test_attribute_and_item_access_return_tablequery(self):
        mgr = make_table_manager()
        assert isinstance(mgr.synapses_pni_2, TableQuery)
        assert isinstance(mgr["nuclei"], TableQuery)

    def test_names_find_contains(self):
        mgr = make_table_manager()
        assert mgr.names == ["nuclei", "synapses_pni_2"]
        assert mgr.find("syn") == ["synapses_pni_2"]
        assert "nuclei" in mgr
        assert "missing" not in mgr

    def test_dir_includes_table_names(self):
        assert "synapses_pni_2" in dir(make_table_manager())

    def test_unknown_table_attribute_errors(self):
        with pytest.raises(AttributeError, match="no table named"):
            make_table_manager().not_a_table

    def test_unknown_table_item_errors(self):
        with pytest.raises(KeyError):
            make_table_manager()["not_a_table"]

    def test_access_carries_columns_and_description(self):
        tq = make_table_manager().synapses_pni_2
        assert tq.columns["size"] is FilterKind.NUMERIC
        assert tq.__doc__ == "synapses"

    def test_end_to_end_via_manager(self):
        client = MagicMock()
        mgr = make_table_manager(client)
        mgr.synapses_pni_2(pre_pt_root_id=[1]).query(version=3)
        t = client.query.call_args.args[0][0]
        assert t.name == "synapses_pni_2"
        assert t.filter_in == {"pre_pt_root_id": [1]}

    def test_view_manager_kind(self):
        fc = MagicMock()
        vm = ViewManager.build(
            fc,
            {"soma_counts": {"description": "v"}},
            {"soma_counts": {"root_id": {"type": "integer"}, "n": {"type": "integer"}}},
        )
        tq = vm.soma_counts
        assert tq._primary.kind == "view"
        assert tq._primary.merge_reference is False
        assert tq.columns["root_id"] is FilterKind.ID
