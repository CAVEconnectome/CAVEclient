"""Tests for process-wide caching of server-scoped metadata."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import responses
from packaging.version import Version
from requests.exceptions import HTTPError

from caveclient import CAVEclient, endpoints, servermeta
from caveclient.servermeta import MetaKey, ServerMetadataRegistry

# `server_cache_on`, `no_real_network` and the autouse reset fixtures are
# discovered automatically from tests/conftest.py.
from .conftest import datastack_dict, test_info

GLOBAL = datastack_dict["global_server"]
LOCAL = datastack_dict["local_server"]
STACK = datastack_dict["datastack_name"]

INFO_URL = endpoints.infoservice_endpoints_v2["datastack_info"].format_map(
    {"i_server_address": GLOBAL, "datastack_name": STACK}
)
INFO_VERSION_URL = endpoints.infoservice_common["get_version"].format_map(
    {"i_server_address": GLOBAL}
)
CG_VERSION_URL = endpoints.chunkedgraph_endpoints_common["get_version"].format_map(
    {"cg_server_address": LOCAL}
)
CG_APIV_URL = endpoints.chunkedgraph_endpoints_common["get_api_versions"].format_map(
    {"cg_server_address": LOCAL}
)
MAT_VERSION_URL = endpoints.materialization_common["get_version"].format_map(
    {"me_server_address": LOCAL}
)
MAT_APIV_URL = endpoints.materialization_common["get_api_versions"].format_map(
    {"me_server_address": LOCAL}
)


def _stub_info():
    responses.add(responses.GET, INFO_URL, json=test_info, status=200)


def _stub_cg_apiv():
    responses.add(responses.GET, CG_APIV_URL, json=[0, 1], status=200)


def _build(token="tok_a", **kwargs):
    return CAVEclient(
        STACK,
        server_address=GLOBAL,
        write_server_cache=False,
        auth_token=token,
        **kwargs,
    )


class _Counter:
    """A counting -- and optionally blocking -- `responses` callback."""

    def __init__(self, payload, status=200, gate=None, entered=None):
        self.payload = payload
        self.status = status
        self.gate = gate
        self.entered = entered
        self.count = 0
        self._lock = threading.Lock()

    def __call__(self, request):
        with self._lock:
            self.count += 1
        if self.entered is not None:
            self.entered.release()
        if self.gate is not None:
            assert self.gate.wait(timeout=15), "gate was never released"
        body = f'"{self.payload}"' if self.status == 200 else "{}"
        return (self.status, {"Content-Type": "application/json"}, body)


class TestOneLookupPerServer:
    @responses.activate
    def test_cache_off_repeats_every_lookup(self):
        _stub_info()
        _stub_cg_apiv()
        counter = _Counter("2.999.0")
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        for _ in range(5):
            assert _build().chunkedgraph.server_version == Version("2.999.0")

        assert counter.count == 5

    @responses.activate
    def test_cache_on_looks_up_once(self, server_cache_on):
        _stub_info()
        apiv = responses.add(responses.GET, CG_APIV_URL, json=[0, 1], status=200)
        counter = _Counter("2.999.0")
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        versions = [_build().chunkedgraph.server_version for _ in range(5)]

        assert versions == [Version("2.999.0")] * 5
        assert counter.count == 1
        assert apiv.call_count == 1

    @responses.activate
    def test_info_payload_is_cached_per_datastack(self, server_cache_on):
        info = responses.add(responses.GET, INFO_URL, json=test_info, status=200)
        for _ in range(4):
            _build()
        assert info.call_count == 1

    @responses.activate
    def test_distinct_servers_keep_distinct_entries(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        info_v = _Counter("999.0.0")
        cg_v = _Counter("2.999.0")
        responses.add_callback(responses.GET, INFO_VERSION_URL, callback=info_v)
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=cg_v)

        for _ in range(4):
            client = _build()
            assert client.info.server_version == Version("999.0.0")
            assert client.chunkedgraph.server_version == Version("2.999.0")

        assert (info_v.count, cg_v.count) == (1, 1)


class TestTokenSharing:
    @responses.activate
    def test_different_tokens_share_an_entry(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        counter = _Counter("2.999.0")
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        first = _build(token="a" * 32)
        second = _build(token="b" * 32)

        assert first.chunkedgraph.server_version == second.chunkedgraph.server_version
        assert counter.count == 1

    @responses.activate
    def test_each_client_still_sends_its_own_token(self):
        """Sharing a cached version must not mean sharing credentials."""
        _stub_info()
        _stub_cg_apiv()
        responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        _build(token="a" * 32)
        _build(token="b" * 32)

        headers = {
            call.request.headers["Authorization"]
            for call in responses.calls
            if call.request.url == INFO_URL
        }
        assert len(headers) == 2

    def test_token_fingerprint_hides_the_token(self):
        token = "super-secret-token-value"
        fingerprint = servermeta.token_fingerprint({"Authorization": f"Bearer {token}"})
        assert token not in fingerprint
        assert fingerprint != servermeta.token_fingerprint(
            {"Authorization": "Bearer other"}
        )
        assert servermeta.token_fingerprint({}) == "anonymous"


class TestSingleFlight:
    def test_concurrent_cold_lookups_issue_one_call(self):
        """The primary guarantee, tested without HTTP so it cannot flake."""
        servermeta.set_server_cache_defaults(enabled=True)
        registry = ServerMetadataRegistry()
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))

        calls = 0
        calls_lock = threading.Lock()
        leader_inside = threading.Event()
        release_leader = threading.Event()
        n = 8
        gate = threading.Barrier(n, timeout=15)

        def fetch():
            nonlocal calls
            with calls_lock:
                calls += 1
            leader_inside.set()
            assert release_leader.wait(timeout=15), "leader was never released"
            return Version("2.999.0")

        def worker():
            gate.wait()
            return registry.get_or_fetch(key, fetch)

        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = [pool.submit(worker) for _ in range(n)]
            assert leader_inside.wait(timeout=15)
            # Let the other threads pile up behind the leader before releasing it,
            # so a late arrival cannot masquerade as successful coalescing.
            time.sleep(0.2)
            release_leader.set()
            results = [f.result(timeout=15) for f in futures]

        assert calls == 1
        assert results == [Version("2.999.0")] * n

    def test_a_slow_lookup_does_not_block_a_different_key(self):
        servermeta.set_server_cache_defaults(enabled=True)
        registry = ServerMetadataRegistry()
        inside, release = threading.Event(), threading.Event()

        def slow():
            inside.set()
            assert release.wait(timeout=15)
            return "a"

        with ThreadPoolExecutor(max_workers=2) as pool:
            slow_future = pool.submit(
                registry.get_or_fetch,
                MetaKey(servermeta.CATEGORY_SERVER_VERSION, "https://a.test"),
                slow,
            )
            assert inside.wait(timeout=15)
            fast_future = pool.submit(
                registry.get_or_fetch,
                MetaKey(servermeta.CATEGORY_SERVER_VERSION, "https://b.test"),
                lambda: "b",
            )
            assert fast_future.result(timeout=5) == "b"
            release.set()
            assert slow_future.result(timeout=15) == "a"

    @responses.activate
    def test_concurrent_client_builds_issue_one_version_call(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        gate = threading.Event()
        entered = threading.Semaphore(0)
        counter = _Counter("2.999.0", gate=gate, entered=entered)
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        n = 6
        barrier = threading.Barrier(n, timeout=20)

        def worker():
            client = _build()
            barrier.wait()
            return client.chunkedgraph.server_version

        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = [pool.submit(worker) for _ in range(n)]
            assert entered.acquire(timeout=20)
            time.sleep(0.2)
            gate.set()
            results = [f.result(timeout=20) for f in futures]

        assert counter.count == 1
        assert results == [Version("2.999.0")] * n


class TestInvalidateAndRefresh:
    @responses.activate
    def test_invalidate_forces_a_refetch(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        counter = _Counter("2.999.0")
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        _build().chunkedgraph.server_version
        _build().chunkedgraph.server_version
        assert counter.count == 1

        servermeta.invalidate(server_address=LOCAL)
        _build().chunkedgraph.server_version
        assert counter.count == 2

        servermeta.invalidate()
        _build().chunkedgraph.server_version
        assert counter.count == 3

    @responses.activate
    def test_invalidate_by_datastack_reaches_server_scoped_entries(
        self, server_cache_on
    ):
        _stub_info()
        _stub_cg_apiv()
        counter = _Counter("2.999.0")
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        _build().chunkedgraph.server_version
        assert counter.count == 1

        # The version entry names no datastack; the reverse index has to find it.
        servermeta.invalidate(datastack=STACK)
        _build().chunkedgraph.server_version
        assert counter.count == 2

    def test_refresh_replaces_the_value(self):
        servermeta.set_server_cache_defaults(enabled=True)
        registry = ServerMetadataRegistry()
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        values = iter(["1.0.0", "2.0.0"])

        def fetch():
            return next(values)

        assert registry.get_or_fetch(key, fetch) == "1.0.0"
        assert registry.get_or_fetch(key, fetch) == "1.0.0"
        assert registry.refresh(key, fetch) == "2.0.0"
        assert registry.get_or_fetch(key, fetch) == "2.0.0"

    def test_a_failed_refresh_keeps_the_previous_value(self):
        servermeta.set_server_cache_defaults(enabled=True)
        registry = ServerMetadataRegistry()
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        assert registry.get_or_fetch(key, lambda: "1.0.0") == "1.0.0"

        def boom():
            raise RuntimeError("upstream is down")

        with pytest.raises(RuntimeError):
            registry.refresh(key, boom)

        assert registry.get_or_fetch(key, lambda: pytest.fail("refetched")) == "1.0.0"


class TestRegistrySemantics:
    def test_disabled_registry_never_stores(self):
        registry = ServerMetadataRegistry()
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        calls = []
        for _ in range(3):
            registry.get_or_fetch(key, lambda: calls.append(1) or "v")
        assert len(calls) == 3
        assert registry.stats()["size"] == 0

    def test_entries_expire(self):
        now = [1000.0]
        servermeta.set_server_cache_defaults(enabled=True, ttl_server_version=60)
        registry = ServerMetadataRegistry(timer=lambda: now[0])
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        values = iter(["1.0.0", "2.0.0"])

        def fetch():
            return next(values)

        assert registry.get_or_fetch(key, fetch) == "1.0.0"
        now[0] += 59
        assert registry.get_or_fetch(key, fetch) == "1.0.0"
        now[0] += 2
        assert registry.get_or_fetch(key, fetch) == "2.0.0"

    def test_mutable_payloads_are_isolated(self):
        servermeta.set_server_cache_defaults(enabled=True)
        registry = ServerMetadataRegistry()
        key = MetaKey(servermeta.CATEGORY_INFO, GLOBAL, datastack=STACK)

        first = registry.get_or_fetch(key, lambda: {"a": {"nested": 1}})
        first["a"]["nested"] = 999
        second = registry.get_or_fetch(key, lambda: pytest.fail("refetched"))
        assert second["a"]["nested"] == 1

    def test_a_copied_client_shares_the_registry(self):
        import copy

        registry = ServerMetadataRegistry()
        assert copy.deepcopy(registry) is registry
        assert copy.copy(registry) is registry


class TestLaziness:
    @responses.activate
    def test_construction_makes_no_version_call(self):
        _stub_info()
        _stub_cg_apiv()
        version = responses.add(
            responses.GET, CG_VERSION_URL, json="2.999.0", status=200
        )

        chunkedgraph = _build().chunkedgraph
        assert version.call_count == 0

        assert chunkedgraph.server_version == Version("2.999.0")
        assert version.call_count == 1

        assert chunkedgraph.server_version == Version("2.999.0")
        assert version.call_count == 1, "the version should be memoized per client"

    @responses.activate
    def test_building_a_client_touches_only_the_info_endpoint(self):
        _stub_info()
        for url in (INFO_VERSION_URL, CG_VERSION_URL, MAT_VERSION_URL):
            responses.add(responses.GET, url, json="999.0.0", status=200)

        _build()

        assert [call.request.url for call in responses.calls] == [INFO_URL]

    @responses.activate
    def test_private_alias_still_resolves(self):
        """`skeletonservice` reads `_server_version` directly in many places."""
        _stub_info()
        _stub_cg_apiv()
        responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        chunkedgraph = _build().chunkedgraph
        assert chunkedgraph._server_version == Version("2.999.0")
        assert chunkedgraph._server_version == chunkedgraph.server_version

    @responses.activate
    def test_eager_option_restores_construction_time_lookup(self):
        servermeta.set_server_cache_defaults(eager_server_version=True)
        _stub_info()
        _stub_cg_apiv()
        responses.add(responses.GET, INFO_VERSION_URL, json="999.0.0", status=200)
        version = responses.add(
            responses.GET, CG_VERSION_URL, json="2.999.0", status=200
        )
        _build().chunkedgraph
        assert version.call_count == 1


class TestFailuresAreNotCached:
    @responses.activate
    def test_a_missing_version_endpoint_is_cached(self, server_cache_on):
        """A 404 is a definite answer about the deployment, so it is cacheable."""
        _stub_info()
        _stub_cg_apiv()
        counter = _Counter(None, status=404)
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=counter)

        for _ in range(3):
            assert _build().chunkedgraph.server_version is None
        assert counter.count == 1

    @responses.activate
    def test_a_server_error_is_not_cached(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        # 500 rather than 503: 503 is in the retry status list and would be
        # re-issued by the session, breaking the call counts below.
        bad = responses.add(responses.GET, CG_VERSION_URL, json={}, status=500)
        good = responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        with pytest.raises(HTTPError):
            _build().chunkedgraph.server_version
        assert _build().chunkedgraph.server_version == Version("2.999.0")
        assert (bad.call_count, good.call_count) == (1, 1)

    @responses.activate
    def test_an_auth_failure_is_not_cached(self, server_cache_on):
        """Caching a 401 as `None` would disable version gating process-wide."""
        _stub_info()
        _stub_cg_apiv()
        bad = responses.add(responses.GET, CG_VERSION_URL, json={}, status=401)
        good = responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        with pytest.raises(HTTPError):
            _build(token="expired").chunkedgraph.server_version
        assert _build(token="fresh").chunkedgraph.server_version == Version("2.999.0")
        assert (bad.call_count, good.call_count) == (1, 1)

    @responses.activate
    def test_an_api_version_blip_does_not_pin_the_fallback(self, server_cache_on):
        """A momentary failure must not lock the client to its fallback API."""
        _stub_info()
        responses.add(responses.GET, MAT_VERSION_URL, json="999.0.0", status=200)
        bad = responses.add(responses.GET, MAT_APIV_URL, json={}, status=500)
        good = responses.add(responses.GET, MAT_APIV_URL, json=[2, 3], status=200)

        first = _build().materialize
        assert first.api_version == 2, "should have fallen back"

        second = _build().materialize
        assert second.api_version == 3, "should have recovered"
        assert (bad.call_count, good.call_count) == (1, 1)


class TestSessionConfiguration:
    @responses.activate
    def test_api_versions_request_carries_a_timeout(self):
        _stub_info()
        responses.add(responses.GET, CG_APIV_URL, json=[0, 1], status=200)
        responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        _build().chunkedgraph

        call = next(c for c in responses.calls if c.request.url == CG_APIV_URL)
        assert call.request.req_kwargs["timeout"] is not None

    @responses.activate
    def test_api_versions_goes_through_a_retrying_session(self):
        """A bare `requests.get` would not retry, so the count proves the fix."""
        from caveclient.session_config import set_session_defaults

        set_session_defaults(max_retries=2, backoff_factor=0)
        _stub_info()
        # 503 is in the retry status list, so the adapter must re-issue it.
        flaky = responses.add(responses.GET, CG_APIV_URL, json={}, status=503)
        responses.add(responses.GET, CG_VERSION_URL, json="2.999.0", status=200)

        _build().chunkedgraph

        assert flaky.call_count == 3  # one attempt plus two retries


class TestMockIntegrity:
    def test_warmed_clients_need_no_further_network(self, myclient, no_real_network):
        """Every warmed sub-client resolved its version inside the stub window."""
        for name in ("info", "chunkedgraph", "materialize", "state", "skeleton"):
            assert getattr(myclient, name).server_version is not None

    def test_services_without_a_version_endpoint_report_none(
        self, myclient, no_real_network
    ):
        assert myclient.annotation.server_version is None
        assert myclient.l2cache.server_version is None
        assert myclient.schema.server_version is None


class TestCachedClient:
    @responses.activate
    def test_cached_client_reuses_the_info_payload(self):
        from caveclient.tools.caching import CachedClient

        info = responses.add(responses.GET, INFO_URL, json=test_info, status=200)
        for _ in range(3):
            client = CachedClient(
                STACK,
                server_address=GLOBAL,
                auth_token="tok",
                write_server_cache=False,
            )
            assert client.datastack_name == STACK
        assert info.call_count == 1

    def test_legacy_names_remain_importable(self):
        from caveclient.tools import caching

        assert callable(caching.CachedClient)
        assert callable(caching.stored_info_cache)
        assert caching.server_key("ds", "https://x.test") == caching.server_key(
            "ds", "https://x.test", auth_token="ignored"
        )
