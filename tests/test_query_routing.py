import datetime
from unittest.mock import MagicMock

import pytest
from packaging.version import Version

from caveclient.query import (
    At,
    ColumnHandle,
    Filter,
    FilterKind,
    FilterOp,
    Join,
    QuerySpec,
    Source,
)
from caveclient.query.backends import (
    Capabilities,
    DeltalakeBackend,
    LiveBackend,
    LiveEmulationBackend,
    MaterializedBackend,
    Switchboard,
    UnroutableQueryError,
    ViewBackend,
)
from caveclient.query.serialize import (
    filters_to_method_kwargs,
    joins_to_pairs,
    joins_to_quads,
)

UTC = datetime.timezone.utc
NOW = datetime.datetime(2024, 1, 1, tzinfo=UTC)


def frozen_table(**kw):
    return QuerySpec(source=Source("t", kind="table"), at=At(version=3), **kw)


def live_table(**kw):
    return QuerySpec(source=Source("t", kind="table"), at=At(timestamp=NOW), **kw)


class TestRouting:
    def setup_method(self):
        self.sb = Switchboard()

    def test_frozen_table_routes_to_materialized(self):
        caps = Capabilities(server_version=Version("5.20.0"))
        assert self.sb.route(frozen_table(), caps).name == "materialized"

    def test_frozen_view_routes_to_view(self):
        spec = QuerySpec(source=Source("v", kind="view"), at=At(version=3))
        caps = Capabilities(server_version=Version("5.20.0"))
        assert self.sb.route(spec, caps).name == "view"

    def test_live_table_modern_server_routes_to_live(self):
        caps = Capabilities(server_version=Version("5.20.0"), has_chunkedgraph=True)
        assert self.sb.route(live_table(), caps).name == "live"

    def test_live_table_old_server_falls_back_to_emulation(self):
        caps = Capabilities(server_version=Version("5.0.0"), has_chunkedgraph=True)
        assert self.sb.route(live_table(), caps).name == "live_emulation"

    def test_live_view_unroutable_in_phase1(self):
        spec = QuerySpec(source=Source("v", kind="view"), at=At(timestamp=NOW))
        caps = Capabilities(server_version=Version("5.20.0"), has_chunkedgraph=True)
        with pytest.raises(UnroutableQueryError, match="does not support timestamp"):
            self.sb.route(spec, caps)

    def test_live_view_routes_to_live_when_server_advertises_compatible(self):
        # forward-compatible: when Phase 2 ships and the server flips the flag
        spec = QuerySpec(source=Source("v", kind="view"), at=At(timestamp=NOW))
        caps = Capabilities(
            server_version=Version("6.0.0"),
            has_chunkedgraph=True,
            source_live_compatible=True,
        )
        assert self.sb.route(spec, caps).name == "live"

    def test_live_table_no_chunkedgraph_old_server_is_unroutable(self):
        caps = Capabilities(server_version=Version("5.0.0"), has_chunkedgraph=False)
        with pytest.raises(UnroutableQueryError) as exc:
            self.sb.route(live_table(), caps)
        # message names each backend's reason
        assert "live:" in str(exc.value)
        assert "live_emulation:" in str(exc.value)

    def test_dataset_source_is_unroutable_until_phase3(self):
        spec = QuerySpec(source=Source("d", kind="dataset"), at=At(version=1))
        caps = Capabilities(server_version=Version("5.20.0"))
        with pytest.raises(UnroutableQueryError):
            self.sb.route(spec, caps)


class TestCanHandleReasons:
    def test_materialized_declines_live(self):
        reason = MaterializedBackend().can_handle(live_table(), Capabilities())
        assert isinstance(reason, str) and "versioned" in reason

    def test_live_declines_old_server_with_version_in_message(self):
        caps = Capabilities(server_version=Version("4.0.0"))
        reason = LiveBackend().can_handle(live_table(), caps)
        assert isinstance(reason, str) and "5.13.0" in reason

    def test_view_declines_non_view(self):
        reason = ViewBackend().can_handle(frozen_table(), Capabilities())
        assert isinstance(reason, str) and "views only" in reason

    def test_emulation_requires_chunkedgraph(self):
        caps = Capabilities(has_chunkedgraph=False)
        reason = LiveEmulationBackend().can_handle(live_table(), caps)
        assert isinstance(reason, str) and "chunkedgraph" in reason

    def test_deltalake_always_declines(self):
        spec = QuerySpec(source=Source("d", kind="dataset"), at=At(version=1))
        assert DeltalakeBackend().can_handle(spec, Capabilities()) is not True


class TestExecutionDelegation:
    def test_materialized_calls_query_table_with_flat_filters(self):
        client = MagicMock()
        spec = frozen_table(
            filters=(
                Filter(
                    ColumnHandle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, [1, 2]
                ),
                Filter(ColumnHandle("size", FilterKind.NUMERIC), FilterOp.NOT_IN, [9]),
            ),
            limit=50,
        )
        MaterializedBackend().execute(spec, client)
        client.query_table.assert_called_once()
        _, kwargs = client.query_table.call_args
        assert kwargs["materialization_version"] == 3
        assert kwargs["limit"] == 50
        # flat single-table filters
        assert kwargs["filter_in_dict"] == {"pre_pt_root_id": [1, 2]}
        # NOT_IN uses the method's filter_out_dict argument name
        assert kwargs["filter_out_dict"] == {"size": [9]}

    def test_live_calls_live_live_query_with_nested_filters(self):
        client = MagicMock()
        client._reference_join_for.return_value = (None, None)
        spec = live_table(
            filters=(
                Filter(ColumnHandle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, [1]),
            )
        )
        LiveBackend().execute(spec, client)
        client.live_live_query.assert_called_once()
        _, kwargs = client.live_live_query.call_args
        assert kwargs["timestamp"] == NOW
        # nested {table: {col: val}} for the live endpoint
        assert kwargs["filter_in_dict"] == {"t": {"pre_pt_root_id": [1]}}

    def test_view_calls_query_view(self):
        client = MagicMock()
        spec = QuerySpec(source=Source("v", kind="view"), at=At(version=2))
        ViewBackend().execute(spec, client)
        client.query_view.assert_called_once_with(
            "v",
            materialization_version=2,
            select_columns=None,
            offset=None,
            limit=None,
            split_positions=False,
            desired_resolution=None,
            metadata=True,
            get_counts=False,
            random_sample=None,
        )


class TestJoins:
    J = Join("syn", "post_pt_root_id", "nuc", "pt_root_id")

    def test_pairs_encoding_single_join(self):
        assert joins_to_pairs([self.J]) == [
            ["syn", "post_pt_root_id"],
            ["nuc", "pt_root_id"],
        ]

    def test_pairs_encoding_rejects_multiple(self):
        with pytest.raises(ValueError, match="single explicit join"):
            joins_to_pairs([self.J, self.J])

    def test_quads_encoding_handles_multiple(self):
        assert joins_to_quads([self.J, self.J]) == [
            ["syn", "post_pt_root_id", "nuc", "pt_root_id"],
            ["syn", "post_pt_root_id", "nuc", "pt_root_id"],
        ]

    def test_frozen_single_join_routes_to_materialized(self):
        spec = QuerySpec(
            source=Source("syn", kind="table", joins=(self.J,)), at=At(version=3)
        )
        backend = Switchboard().route(
            spec, Capabilities(server_version=Version("5.20.0"))
        )
        assert backend.name == "materialized"

    def test_frozen_multi_join_is_refused(self):
        spec = QuerySpec(
            source=Source("syn", kind="table", joins=(self.J, self.J)),
            at=At(version=3),
        )
        with pytest.raises(UnroutableQueryError, match="single explicit join"):
            Switchboard().route(spec, Capabilities(server_version=Version("5.20.0")))

    def test_frozen_join_delegates_to_join_query_with_pairs(self):
        client = MagicMock()
        spec = QuerySpec(
            source=Source("syn", kind="table", joins=(self.J,), suffixes={"syn": ""}),
            at=At(version=3),
            filters=(
                Filter(
                    ColumnHandle("size", FilterKind.NUMERIC, table="syn"),
                    FilterOp.GREATER,
                    100,
                ),
            ),
        )
        MaterializedBackend().execute(spec, client)
        client.join_query.assert_called_once()
        args, kwargs = client.join_query.call_args
        assert args[0] == [["syn", "post_pt_root_id"], ["nuc", "pt_root_id"]]
        assert kwargs["suffixes"] == {"syn": ""}
        # join filters are nested by table
        assert kwargs["filter_greater_dict"] == {"syn": {"size": 100}}

    def test_live_join_delegates_to_live_live_query_with_quads(self):
        client = MagicMock()
        spec = QuerySpec(
            source=Source("syn", kind="table", joins=(self.J,)),
            at=At(timestamp=NOW),
        )
        LiveBackend().execute(spec, client)
        _, kwargs = client.live_live_query.call_args
        assert kwargs["joins"] == [["syn", "post_pt_root_id", "nuc", "pt_root_id"]]


class TestFlatVsNestedKwargs:
    def test_flat_rejects_multiple_tables(self):
        filters = [
            Filter(ColumnHandle("a", FilterKind.NUMERIC), FilterOp.EQUAL, 1),
            Filter(
                ColumnHandle("b", FilterKind.NUMERIC, table="other"), FilterOp.EQUAL, 2
            ),
        ]
        with pytest.raises(ValueError, match="single table"):
            filters_to_method_kwargs(filters, default_table="t", nested=False)

    def test_nested_allows_multiple_tables(self):
        filters = [
            Filter(ColumnHandle("a", FilterKind.NUMERIC), FilterOp.EQUAL, 1),
            Filter(
                ColumnHandle("b", FilterKind.NUMERIC, table="other"), FilterOp.EQUAL, 2
            ),
        ]
        out = filters_to_method_kwargs(filters, default_table="t", nested=True)
        assert out["filter_equal_dict"] == {"t": {"a": 1}, "other": {"b": 2}}
