Framework Client: one client for all services
=============================================

The Dynamic Annotation Framework consists of a number of different
services, each with a specific set of tasks that it can perform through
REST endpoints. This module is designed to ease programmatic interaction
with all of the various endpoints. Going forward, we also will be
increasingly using authentication tokens for programmatic access to most
if not all of the services. In order to collect a given server, dataset
name, and user token together into a coherent package that can be used
on multiple endpoints, we will use a FrameworkClient that can build
appropriately configured clients for each of the specific services. Each of the individual services has their own specific documentation as well.

Initializing a FrameworkClient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming that the services are on ``www.dynamicannotationframework.com``
and authentication tokens are either not being used or set up with
default values (see next section), one needs only to specify the dataset
name.

.. code:: ipython3

    from annotationframeworkclient import FrameworkClient
    
    dataset_name = 'pinky100'
    client = FrameworkClient(dataset_name)

Just to confirm that this works, let’s see if we can get the EM image
source from the InfoService. If you get a reasonable looking path,
everything is okay.

.. code:: ipython3

    print(f"The image source is: {client.info.image_source()}")

Accessing specific clients
~~~~~~~~~~~~~~~~~~~~~~~~~~
Each client can be acccessed as a property of the main client. See the documentation at left for the capabilities of each. Assuming your client is named ``client``, the subclients for each service are:

* Authentication Service : ``client.auth``
* AnnotationEngine : ``client.annotation``
* PyChunkedGraph : ``client.chunkedgraph``
* InfoService : ``client.info``
* EM Annotation Schemas : ``client.schemas``
* JSON Neuroglancer State Service : ``client.state``

In addition, there are more complex clients that use multiple services together:

* ImageryClient : Uses cloudvolume and the ChunkedGraph to look up segmentations and imagery together.
* ErstaszMaterialization : Uses cloudvolume and the chunkedgraph to look up segmentations associated with point-like annotations.