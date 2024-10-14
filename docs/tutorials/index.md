---
title: Getting Started
---

AnnotationFramework client is a package for simplifying interactions
with HTML services associated with the CAVE (Connectome Annotation
Versioning Engine).

For a larger introduction to CAVE and its services,
see the main GitHub organization site: [https://github.com/CAVEconnectome]()

## Installation

The CAVEclient can be installed with pip:

```bash
$ pip install caveclient
```

## Assumptions

The code is setup to work flexibly with any deployment of these
services, but you need to specify the server_address if that address is
not <https://globalv1.daf-apis.com/> for each client when initializing
it. Similarly, the clients can query the info service for metadata to
simplify the interaction with a datastack, but you have to specify a
datastack name.
