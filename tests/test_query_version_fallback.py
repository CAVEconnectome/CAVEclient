import datetime

from caveclient.query import At, QuerySpec, Source, resolve_version_fallback

UTC = datetime.timezone.utc
TS = datetime.datetime(2024, 6, 1, tzinfo=UTC)


def versioned(v):
    return QuerySpec(source=Source("t", kind="table"), at=At(version=v))


def never(_v):
    raise AssertionError("timestamp_lookup should not be called")


class TestVersionFallback:
    def test_available_version_is_unchanged(self):
        spec, fell_back = resolve_version_fallback(
            versioned(943), available_versions={943, 944}, timestamp_lookup=never
        )
        assert not fell_back
        assert spec.at.version == 943

    def test_stale_version_falls_back_to_timestamp(self):
        spec, fell_back = resolve_version_fallback(
            versioned(900),
            available_versions={943, 944},
            timestamp_lookup=lambda v: TS,
        )
        assert fell_back
        assert spec.at.version is None
        assert spec.at.timestamp == TS
        assert spec.is_live

    def test_stale_version_falls_back_using_its_own_timestamp(self):
        seen = {}

        def lookup(v):
            seen["v"] = v
            return TS

        resolve_version_fallback(
            versioned(900), available_versions={943}, timestamp_lookup=lookup
        )
        assert seen["v"] == 900

    def test_unknown_version_with_no_timestamp_is_left_alone(self):
        # timestamp_lookup returns None -> no fallback, original spec returned so
        # the natural "version not found" error surfaces downstream
        spec, fell_back = resolve_version_fallback(
            versioned(7), available_versions={943}, timestamp_lookup=lambda v: None
        )
        assert not fell_back
        assert spec.at.version == 7

    def test_no_version_pinned_is_unchanged(self):
        spec = QuerySpec(source=Source("t", kind="table"), at=At())
        out, fell_back = resolve_version_fallback(
            spec, available_versions={943}, timestamp_lookup=never
        )
        assert not fell_back
        assert out is spec

    def test_timestamp_query_is_unchanged(self):
        spec = QuerySpec(source=Source("t", kind="table"), at=At(timestamp=TS))
        out, fell_back = resolve_version_fallback(
            spec, available_versions={943}, timestamp_lookup=never
        )
        assert not fell_back
        assert out is spec

    def test_filters_preserved_across_fallback(self):
        from caveclient.query import ColumnHandle, Filter, FilterKind, FilterOp

        spec = QuerySpec(
            source=Source("t", kind="table"),
            at=At(version=900),
            filters=(
                Filter(ColumnHandle("pre_pt_root_id", FilterKind.ID), FilterOp.IN, [1]),
            ),
            limit=50,
        )
        out, fell_back = resolve_version_fallback(
            spec, available_versions={943}, timestamp_lookup=lambda v: TS
        )
        assert fell_back
        assert out.filters == spec.filters
        assert out.limit == 50
