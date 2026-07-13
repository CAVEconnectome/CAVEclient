# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CAVEclient is the Python client for CAVE (Connectome Annotation Versioning Engine), a set of microservices for storing and versioning connectomics data, dynamic annotations, metadata, and segmentations. The package wraps each microservice in its own sub-client and exposes them through a single `CAVEclient` facade.

## Common Commands

Development uses [`uv`](https://docs.astral.sh/uv/) plus `poethepoet` tasks defined in `pyproject.toml`. After `uv sync`:

```bash
uvx --from poethepoet poe checks       # doc-build + lint + test (run before PR)
uvx --from poethepoet poe test         # full pytest suite with coverage
uvx --from poethepoet poe lint         # ruff check + ruff format
uvx --from poethepoet poe lint-fix     # autoformat caveclient/ and tests/
uvx --from poethepoet poe doc-build    # build mkdocs site
uvx --from poethepoet poe doc-serve    # serve docs locally
```

Single-test invocation (use `uv run` so the venv resolves):

```bash
uv run pytest tests/test_chunkedgraph.py::test_name -xvs
```

Lint check (the CI gate) is `uv run ruff check . --extend-select I` — the `I` selector enforces import ordering on top of the default ruff rules. Notebooks are excluded from lint.

Version bumps go through `bump-my-version` (configured in `pyproject.toml`); use `uvx --from poethepoet poe dry-bump` to preview.

## Architecture

### Client facade and lazy sub-clients

`CAVEclient(datastack_name=...)` (`caveclient/frameworkclient.py`) uses `__new__` to return one of two concrete classes:

- `CAVEclientGlobal` — when no datastack is given; exposes only global services (`auth`, `info`, `state`, `schema`).
- `CAVEclientFull` — extends global with datastack-bound services (`chunkedgraph`, `annotation`, `materialize`, `skeleton`, `l2cache`).

Each sub-client is a `@property` that builds on first access and caches into `self._<name>`. Sub-clients receive an `over_client` ref back to the facade (accessed inside sub-clients as `self.fc`) so they can share auth, the info cache, the desired resolution, and the datastack name. **When adding a new sub-client, follow this pattern rather than wiring shared state through constructor arguments.**

### Base classes (`caveclient/base.py`)

Three layered bases — `ClientBase`, `ClientBaseWithDataset`, `ClientBaseWithDatastack` — provide:

- the `requests.Session` configured by `session_config.py` (retry/backoff/pool size; user-settable via `set_session_defaults`),
- API version negotiation: each sub-client declares the API versions it supports, queries the server's supported versions, intersects, and picks the max,
- server software version detection used by the `@_check_version_compatibility` decorator to enforce per-method and per-kwarg minimum server versions,
- `BaseEncoder`, a JSON encoder aware of numpy, pandas, Arrow, sets, and dates.

The version-compat decorator is how the client stays usable against older servers — when adding a method that depends on a newer server feature, decorate it rather than branching at call sites.

### Endpoints (`caveclient/endpoints.py`)

URLs are format-string templates, organized per service into:

- `{service}_common` — endpoints shared across versions,
- `{service}_endpoints_v{N}` — one dict per API version,
- `{service}_api_versions` — map of `version_number -> endpoint_dict`.

Each service has a server-address key (e.g. `ae_server_address`, `me_server_address`, `cg_server_address`) that is filled in via `format_map()` per request. Adding an endpoint means adding it to the right version dict, not creating a new file.

### Datastack and auth

- `datastack_lookup.py` caches the `datastack_name -> server_address` mapping to disk so cold-start doesn't always hit the info service. `write_server_cache=False` on `CAVEclient(...)` disables the write.
- `auth.py`'s `AuthClient` reads tokens from `~/.cloudvolume/secrets/cave-secret.json` (or server-specific variants), injects `Authorization: Bearer` headers, and sets a `middle_auth_token` cookie. Interactive token setup is `CAVEclient.setup_token()`.

### Materialization and table tooling

`materializationengine.py` is the largest sub-client and is layered: the low-level methods talk to the server; `tools/table_manager.py` (`TableManager` / `ViewManager`) builds a Pythonic query/filter/join API on top. `tools/stage.py` (`StagedAnnotations`) is the analogous batch-builder for `annotationengine.py`. When adding query features, prefer extending `TableManager` over adding ad-hoc methods to the materialization client.

### Testing

`tests/conftest.py` builds fixtures from `caveclient/tools/testing.py` — `CAVEclientMock` constructs a client wired to a fake server (no network), and `responses` is used to register expected HTTP interactions. Live integration tests live separately in `live_tests/` and are not run by the default `poe test`. When adding tests for a new endpoint, register the responses in the test rather than touching `testing.py` unless the fixture is broadly reusable.

## Python support

Supported: 3.9, 3.10, 3.11, 3.12 (declared in `pyproject.toml` and tested in CI). Optional extra `caveclient[cv]` pulls in `cloud-volume` for imagery/segmentation/skeleton format interop.
