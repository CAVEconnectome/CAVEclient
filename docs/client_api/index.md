---
title: Overview
---

The most common method of interacting with the CAVE Framework is by instantiating a
client (`caveclient.CAVEclient`) and then using that client to interact with various
services. Under the hood, the CAVEclient is a collection of individual clients, which
can be accessed via properties. For example, to access the materialization client,
you can use `client.materialize`, which (up to the exact version) will actually return a
[caveclient.materializationengine.MaterializatonClientV3][] object.

These pages describe the functionality of each of the individual clients, assuming
that you are using the most up-to-date version of each. For more detailed information
on all versions of all clients available, visit the [extended API documentation](../extended_api/index.md).
