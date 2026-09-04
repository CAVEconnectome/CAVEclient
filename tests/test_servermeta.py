"""Tests for process-wide caching of server-scoped metadata."""

import os
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import responses
from packaging.version import Version
from requests.exceptions import HTTPError

from caveclient import CAVEclient, endpoints, servermeta
from caveclient.servermeta import MetaKey

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


def _cached_entries():
    return sum(len(cache) for cache, _, _ in servermeta._storage.values())


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
        servermeta.enable_server_cache()
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
            return servermeta.get_or_fetch(key, fetch)

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
        servermeta.enable_server_cache()
        inside, release = threading.Event(), threading.Event()

        def slow():
            inside.set()
            assert release.wait(timeout=15)
            return "a"

        with ThreadPoolExecutor(max_workers=2) as pool:
            slow_future = pool.submit(
                servermeta.get_or_fetch,
                MetaKey(servermeta.CATEGORY_SERVER_VERSION, "https://a.test"),
                slow,
            )
            assert inside.wait(timeout=15)
            fast_future = pool.submit(
                servermeta.get_or_fetch,
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

        assert servermeta.invalidate(server_address=LOCAL) >= 1
        _build().chunkedgraph.server_version
        assert counter.count == 2

        assert servermeta.invalidate() >= 1
        _build().chunkedgraph.server_version
        assert counter.count == 3

    @responses.activate
    def test_invalidate_by_server_leaves_other_servers_alone(self, server_cache_on):
        _stub_info()
        _stub_cg_apiv()
        info_v = _Counter("999.0.0")
        cg_v = _Counter("2.999.0")
        responses.add_callback(responses.GET, INFO_VERSION_URL, callback=info_v)
        responses.add_callback(responses.GET, CG_VERSION_URL, callback=cg_v)

        client = _build()
        client.info.server_version
        client.chunkedgraph.server_version

        servermeta.invalidate(server_address=LOCAL)
        client = _build()
        client.info.server_version
        client.chunkedgraph.server_version
        assert (info_v.count, cg_v.count) == (1, 2)

    @responses.activate
    def test_refresh_rebuilds_sub_clients_and_repeats_negotiation(
        self, server_cache_on
    ):
        _stub_info()
        apiv = responses.add(responses.GET, CG_APIV_URL, json=[0, 1], status=200)
        version = responses.add(
            responses.GET, CG_VERSION_URL, json="2.999.0", status=200
        )
        client = _build()
        before = client.chunkedgraph
        before.server_version
        assert (apiv.call_count, version.call_count) == (1, 1)

        servermeta.refresh(client)

        assert client.chunkedgraph is not before
        assert (apiv.call_count, version.call_count) == (2, 2)
        assert client._materialize is None, "unbuilt services stay unbuilt"


class TestDependencyFloor:
    def test_cachetools_supports_single_flight(self):
        """`condition=` arrived in cachetools 6.0.0.

        Without it `cached()` raises TypeError during client construction, so a
        resolution below the floor breaks the package outright. Assert it here
        to fail with an explanatory message rather than deep in a constructor.
        """
        import inspect

        from cachetools import cached

        assert "condition" in inspect.signature(cached).parameters, (
            "installed cachetools is too old: `cached(condition=...)` requires "
            "cachetools >= 6.0.0, which caveclient depends on for single-flight "
            "server metadata lookups"
        )


class TestCacheSemantics:
    def test_disabled_cache_never_stores(self):
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        calls = []
        for _ in range(3):
            servermeta.get_or_fetch(key, lambda: calls.append(1) or "v")
        assert len(calls) == 3
        assert _cached_entries() == 0

    def test_per_client_override_beats_the_process_setting(self):
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        calls = []
        for _ in range(3):
            servermeta.get_or_fetch(key, lambda: calls.append(1) or "v", enabled=True)
        assert len(calls) == 1

        servermeta.enable_server_cache()
        for _ in range(3):
            servermeta.get_or_fetch(key, lambda: calls.append(1) or "v", enabled=False)
        assert len(calls) == 4

    def test_always_on_categories_ignore_the_switch(self):
        key = MetaKey(servermeta.CATEGORY_TABLE_METADATA, LOCAL, STACK, ("t",))
        calls = []
        for _ in range(3):
            servermeta.get_or_fetch(key, lambda: calls.append(1) or {}, enabled=False)
        assert len(calls) == 1

    def test_entries_expire(self, monkeypatch):
        now = [1000.0]
        monkeypatch.setattr(servermeta, "_timer", lambda: now[0])
        servermeta.enable_server_cache(ttl_slow=60)
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        values = iter(["1.0.0", "2.0.0"])

        def fetch():
            return next(values)

        assert servermeta.get_or_fetch(key, fetch) == "1.0.0"
        now[0] += 59
        assert servermeta.get_or_fetch(key, fetch) == "1.0.0"
        now[0] += 2
        assert servermeta.get_or_fetch(key, fetch) == "2.0.0"

    def test_mutable_payloads_are_isolated(self):
        servermeta.enable_server_cache()
        key = MetaKey(servermeta.CATEGORY_INFO, GLOBAL, datastack=STACK)

        first = servermeta.get_or_fetch(key, lambda: {"a": {"nested": 1}})
        first["a"]["nested"] = 999
        second = servermeta.get_or_fetch(key, lambda: pytest.fail("refetched"))
        assert second["a"]["nested"] == 1

    def test_reconfiguring_drops_entries(self):
        servermeta.enable_server_cache()
        key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
        servermeta.get_or_fetch(key, lambda: "v")
        assert _cached_entries() == 1
        servermeta.enable_server_cache(ttl_slow=10)
        assert _cached_entries() == 0


class TestTokenScoping:
    def test_keys_ignore_the_token_by_default(self):
        a = servermeta.datastack_key("info", GLOBAL, STACK, {"Authorization": "a"})
        b = servermeta.datastack_key("info", GLOBAL, STACK, {"Authorization": "b"})
        assert a == b

    def test_token_scoped_keys_differ_per_token_without_exposing_it(self):
        servermeta.enable_server_cache(token_scoped=True)
        headers = {"Authorization": "Bearer super-secret"}
        a = servermeta.datastack_key("info", GLOBAL, STACK, headers)
        b = servermeta.datastack_key("info", GLOBAL, STACK, {"Authorization": "x"})
        assert a != b
        assert "super-secret" not in repr(a)

    @responses.activate
    def test_token_scoped_info_is_fetched_per_token(self):
        servermeta.enable_server_cache(token_scoped=True)
        info = responses.add(responses.GET, INFO_URL, json=test_info, status=200)
        _build(token="a" * 32)
        _build(token="a" * 32)
        _build(token="b" * 32)
        assert info.call_count == 2


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
        other = CachedClient(
            STACK, server_address=GLOBAL, auth_token="tok", write_server_cache=False
        )
        assert other.info.info_cache == client.info.info_cache
        assert other.info.info_cache is not client.info.info_cache

    def test_legacy_names_remain_importable(self):
        from caveclient.tools import caching

        assert callable(caching.CachedClient)
        assert callable(caching.stored_info_cache)
        assert caching.server_key("ds", "https://x.test") == caching.server_key(
            "ds", "https://x.test", auth_token="ignored"
        )


class TestMaterializationMetadataKeys:
    """The module-level metadata caches are shared by every client in the
    process, so their keys must carry everything that identifies the answer.
    Mock clients are built before the `responses` context is entered because
    `CAVEclientMock` manages its own, and would switch mocking off on return."""

    @staticmethod
    def _register(rsps, client, datastack, version, payload):
        mapping = dict(client.materialize.default_url_mapping)
        mapping.update(datastack_name=datastack, version=version, table_name="t")
        ep = client.materialize._endpoints
        rsps.add(
            responses.GET,
            ep["versions_metadata"].format_map(mapping),
            json=[
                {
                    "version": version,
                    "time_stamp": "2020-01-01T00:00:00.000000",
                    "expires_on": "2030-01-01T00:00:00.000000",
                    **payload,
                }
            ],
        )
        rsps.add(responses.GET, ep["metadata"].format_map(mapping), json=payload)

    def test_keyed_by_datastack(self):
        from caveclient.tools.testing import CAVEclientMock

        other = dict(datastack_dict, datastack_name="other_stack")
        a = CAVEclientMock(materialization=True, **datastack_dict)
        b = CAVEclientMock(materialization=True, **other)
        a.materialize._version = b.materialize._version = 1

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            self._register(rsps, a, STACK, 1, {"datastack": STACK})
            self._register(rsps, b, "other_stack", 1, {"datastack": "other_stack"})

            assert a.materialize.get_versions_metadata()[0]["datastack"] == STACK
            assert (
                b.materialize.get_versions_metadata()[0]["datastack"] == "other_stack"
            )
            assert a.materialize.get_table_metadata("t")["datastack"] == STACK
            assert b.materialize.get_table_metadata("t")["datastack"] == "other_stack"

    def test_keyed_by_version(self):
        from caveclient.tools.testing import CAVEclientMock

        a = CAVEclientMock(materialization=True, **datastack_dict)
        b = CAVEclientMock(materialization=True, **datastack_dict)
        a.materialize._version = 1
        b.materialize._version = 2

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            self._register(rsps, a, STACK, 1, {"version_seen": 1})
            self._register(rsps, b, STACK, 2, {"version_seen": 2})
            assert a.materialize.get_table_metadata("t")["version_seen"] == 1
            assert b.materialize.get_table_metadata("t")["version_seen"] == 2

    def test_call_shape_does_not_split_the_cache(self):
        from caveclient.tools.testing import CAVEclientMock

        a = CAVEclientMock(materialization=True, **datastack_dict)
        a.materialize._version = 1

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            self._register(rsps, a, STACK, 1, {"id": 1})
            a.materialize.get_table_metadata("t")
            a.materialize.get_table_metadata(
                table_name="t", datastack_name=STACK, version=1, log_warning=False
            )
            a.materialize.get_table_metadata("t", STACK, 1)
            assert len(rsps.calls) == 1


class TestPickling:
    @responses.activate
    def test_client_round_trips_and_stays_lazy(self):
        from caveclient.base import _UNSET

        _stub_info()
        _stub_cg_apiv()
        version = responses.add(
            responses.GET, CG_VERSION_URL, json="2.999.0", status=200
        )
        client = _build(server_cache=True)
        client.chunkedgraph  # built, version not yet looked up

        copy_ = pickle.loads(pickle.dumps(client))

        assert copy_.chunkedgraph._use_server_cache is True
        assert getattr(copy_.chunkedgraph, "_ClientBase__server_version") is _UNSET
        assert version.call_count == 0
        assert copy_.chunkedgraph.server_version == Version("2.999.0")
        assert version.call_count == 1


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_a_forked_child_starts_cold_and_does_not_deadlock():
    import warnings

    servermeta.enable_server_cache()
    key = MetaKey(servermeta.CATEGORY_SERVER_VERSION, LOCAL, extra=("u",))
    assert servermeta.get_or_fetch(key, lambda: "parent") == "parent"

    read_fd, write_fd = os.pipe()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        try:
            os.close(read_fd)
            value = servermeta.get_or_fetch(key, lambda: "child")
            os.write(write_fd, value.encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    _, status = os.waitpid(pid, 0)
    seen = os.read(read_fd, 64).decode()
    os.close(read_fd)
    assert os.WEXITSTATUS(status) == 0
    assert seen == "child"
