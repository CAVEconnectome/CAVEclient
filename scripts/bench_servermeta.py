#!/usr/bin/env python
"""Before/after harness for the server-metadata registry.

NOT a unit test -- lives outside tests/ so `poe test` never collects it.
Run with `poe bench-servermeta` or `uv run python scripts/bench_servermeta.py`.

All HTTP is mocked with `responses`; latency is injected synthetically, so the
numbers isolate CALL COUNT rather than real server behaviour. The 327ms default
latency is taken from the production trace that motivated this work
(3601ms / 11 calls to /info/version).
"""

import argparse
import json
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import responses

from caveclient import CAVEclient, endpoints
from caveclient.tools.testing import (
    TEST_DATASTACK,
    TEST_GLOBAL_SERVER,
    TEST_LOCAL_SERVER,
    api_version_url,
    default_info,
    version_url,
)

try:
    from caveclient import servermeta
except ImportError:  # baseline run, before the registry exists
    servermeta = None

STACK, GLOBAL, LOCAL = TEST_DATASTACK, TEST_GLOBAL_SERVER, TEST_LOCAL_SERVER
INFO = default_info(LOCAL)

COUNTS = Counter()
_LOCK = threading.Lock()

# Services whose version gates a feature, i.e. what a request handler touches.
WARM_SERVICES = ("info", "chunkedgraph", "materialize", "skeleton", "state")


def _delayed(payload, latency):
    def cb(request):
        with _LOCK:
            COUNTS[urlparse(request.url).path] += 1
        if latency:
            time.sleep(latency)
        return (200, {"Content-Type": "application/json"}, json.dumps(payload))

    return cb


def _register(latency):
    def add(url, payload):
        responses.add_callback(
            responses.GET,
            url,
            callback=_delayed(payload, latency),
            content_type="application/json",
        )

    add(
        endpoints.infoservice_endpoints_v2["datastack_info"].format_map(
            {"i_server_address": GLOBAL, "datastack_name": STACK}
        ),
        INFO,
    )
    add(
        version_url(GLOBAL, endpoints.infoservice_common, "i_server_address"), "999.0.0"
    )
    add(
        version_url(
            LOCAL, endpoints.chunkedgraph_endpoints_common, "cg_server_address"
        ),
        "2.999.0",
    )
    add(
        api_version_url(
            LOCAL, endpoints.chunkedgraph_endpoints_common, "cg_server_address"
        ),
        [0, 1],
    )
    add(
        version_url(LOCAL, endpoints.materialization_common, "me_server_address"),
        "999.0.0",
    )
    add(
        api_version_url(LOCAL, endpoints.materialization_common, "me_server_address"),
        [2, 3],
    )
    add(
        version_url(GLOBAL, endpoints.jsonservice_endpoints_v1, "json_server_address"),
        "999.0.0",
    )
    add(
        version_url(
            LOCAL, endpoints.skeletonservice_endpoints_v1, "skeleton_server_address"
        ),
        "999.0.0",
    )
    add(
        api_version_url(
            GLOBAL, endpoints.schema_endpoints_common, "emas_server_address"
        ),
        [1, 2],
    )


def one_page_load(touch_versions=True):
    """Approximate a request handler: build a client, touch the gated services."""
    client = CAVEclient(
        STACK,
        server_address=GLOBAL,
        write_server_cache=False,
        auth_token="bench_token",
    )
    if touch_versions:
        for name in WARM_SERVICES:
            _ = getattr(client, name).server_version
    return client


def _set_cache(enabled):
    if servermeta is None:
        return not enabled  # baseline build can only do the "off" mode
    servermeta.set_server_cache_defaults(enabled=enabled)
    servermeta.get_registry().clear()
    return True


@responses.activate
def run(n, latency, cache, workers, touch_versions=True):
    COUNTS.clear()
    if not _set_cache(cache):
        return None, None
    _register(latency)

    one_page_load(touch_versions)  # discard warm-up: token file / import noise
    COUNTS.clear()
    _set_cache(cache)

    t0 = time.perf_counter()
    if workers == 1:
        for _ in range(n):
            one_page_load(touch_versions)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(lambda _: one_page_load(touch_versions), range(n)))
    wall = time.perf_counter() - t0
    return wall, dict(COUNTS)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, nargs="+", default=[1, 5, 11, 25])
    p.add_argument("--latency-ms", type=float, nargs="+", default=[25, 327])
    p.add_argument("--workers", type=int, nargs="+", default=[1, 8])
    p.add_argument(
        "--no-touch-versions",
        action="store_true",
        help="build clients only; isolates the laziness win from the cache win",
    )
    args = p.parse_args()

    touch = not args.no_touch_versions
    hdr = (
        f"{'mode':>8} {'N':>4} {'lat':>5} {'cache':>6} {'calls':>6} "
        f"{'calls/cl':>9} {'wall_ms':>9} {'ms/client':>10} {'speedup':>8}"
    )
    print(f"touch_versions={touch}  servermeta={'yes' if servermeta else 'ABSENT'}")
    print(hdr)
    print("-" * len(hdr))
    last = {}
    for w in args.workers:
        for lat in args.latency_ms:
            for n in args.n:
                base = None
                for cache in (False, True):
                    wall, counts = run(n, lat / 1000.0, cache, w, touch)
                    if wall is None:
                        continue
                    total = sum(counts.values())
                    if base is None:
                        base = wall
                    mode = "serial" if w == 1 else f"thr:{w}"
                    print(
                        f"{mode:>8} {n:>4} {lat:>5.0f} {str(cache):>6} {total:>6} "
                        f"{total / n:>9.1f} {wall * 1000:>9.1f} "
                        f"{wall / n * 1000:>10.1f} {base / wall:>8.2f}x"
                    )
                    last = counts
                    if cache and servermeta is not None:
                        for path, c in counts.items():
                            if path.endswith(("/version", "/versions")):
                                assert c == 1, f"{path} called {c}x with cache on"
    if last:
        print("\nper-endpoint breakdown (last run):")
        for path, c in sorted(last.items(), key=lambda kv: -kv[1]):
            print(f"  {c:>4}  {path}")


if __name__ == "__main__":
    main()
