# Getting Started

AnnotationFramework client is a package for simplifying interactions
with HTML services associated with the CAVE (Connectome Annotation
Versioning Engine), which includes:

- [pychunkedgraph](https://www.github.com/seung-lab/pychunkedgraph)
  (For tracking dynamic segmentations)
- [NeuroglancerJsonServer](https://www.github.com/seung-lab/NeuroglancerJsonServer)
  (For posting/getting neuroglancer json states)
- [AnnotationFrameworkInfoService](https://www.github.com/seung-lab/AnnotationFrameworkInfoService)
  (For storing datastack metadata information)
- [EmAnnotationSchemas](https://www.github.com/seung-lab/EmAnnotationSchemas)
  (For storing an extensible set of schemas for annotating EM data)
- [AnnotatationEngine](https://www.github.com/seung-lab/AnnotationEngine)
  (For storing annotations on EM data)

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
