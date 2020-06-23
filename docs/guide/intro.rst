Getting Started
===============

AnnotationFramework client is a package for simplifying interactions with HTML services associated with the DynamicAnnoationFramework.  Including

- `pychunkedgraph <https://www.github.com/seung-lab/pychunkedgraph>`_ (For tracking dynamic segmentations)
- `NeuroglancerJsonServer <https://www.github.com/seung-lab/NeuroglancerJsonServer>`_ (For posting/getting neuroglancer json states)
- `AnnotationFrameworkInfoService <https://www.github.com/seung-lab/AnnotationFrameworkInfoService>`_ (For storing datastack metadata information)
- `EmAnnotationSchemas <https://www.github.com/seung-lab/EmAnnotationSchemas>`_ (For storing an extensible set of schemas for annotating EM data)
- `AnnotatationEngine <https://www.github.com/seung-lab/AnnotationEngine>`_ (For storing annotations on EM data)

Installation
~~~~~~~~~~~~

The AnnotationFrameworkClient can be installed with pip:

.. code-block:: bash

   $ pip install annotationframeworkclient

Assumptions
~~~~~~~~~~~

the code is setup to work flexibly with any deployment of these services, but you need to specify the server_address if that address is not 
https://www.dynamicannotationframework.com/ for each client when initializing it.  similarly, the clients can query the info service for metadata
to simplify the interaction with a datastack, but you have to specify a datastack name.
