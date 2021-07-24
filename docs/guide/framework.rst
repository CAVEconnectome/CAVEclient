CAVEclient: One client for all services
=============================================

The CAVE Framework consists of a number of different
services, each with a specific set of tasks that it can perform through
REST endpoints.
The CAVEclient is designed to ease programmatic interaction
with all of the various endpoints.
In addition, most programmatic access requires the use of authentication tokens.
In order to collect a given server, datastack name, and user token together into a coherent package that can be used
on multiple endpoints, the CAVEclient builds
appropriately configured clients for each of the specific services.
Each of the individual services has their own specific documentation as well.

Global and Local Services
~~~~~~~~~~~~~~~~~~~~~~~~~

There are two categories of data in CAVE: Global and local.
Local services are associated with a single so-called "datastack", which refers to a precise collection of imagery and segmentation data that function together.
For example, EM imagery and a specific pychunkedgraph segmentation would be one datastack, while the same EM imagery but an initial static segmentation would be another.
Datastacks are refered to by a short name, for instance ``pinky100_public_flat_v185``.

Global services are those that are potentially shared across multiple different specific datastacks.
These include the info service, which can describe the properties of all available datastacks,
the authentication service, and the state service that hosts neuroglancer states.
Global services are associated with a particular URL (by default ``http://globalv1.daf-apis.com``),
but not a single datastack.

Initializing a CAVEclient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming that the services are on ``http://globalv1.daf-apis.com``
and authentication tokens are either not being used or set up with
default values (see :doc:`authentication`), a simple CAVEclient that can
only access global services can be initialized:

.. code:: python

    from caveclient import CAVEclient
    
    client = CAVEclient()

Just to confirm that this works, let’s see if we can get the EM image
source from the InfoService.
If you get a list of names of datastacks, all is good. If you have not yet set up an
authentication token or you get an authentication error, look at :ref:`new-token`
for information about how to set up your auth token.

.. code:: python

    client.info.get_datastacks()

If you have a specific datastack you want to use, you can inititialize your CAVEclient with it.
This gives you access to the full range of client functions.

.. code:: python

    client = CAVEclient(datastack_name='my_datastack')
    

Accessing specific clients
~~~~~~~~~~~~~~~~~~~~~~~~~~
Each client can be acccessed as a property of the main client. See the documentation at left for the capabilities of each. Assuming your client is named ``client``, the subclients for each service are:

* Authentication Service : ``client.auth``
* AnnotationEngine : ``client.annotation``
* PyChunkedGraph : ``client.chunkedgraph``
* InfoService : ``client.info``
* EM Annotation Schemas : ``client.schemas``
* JSON Neuroglancer State Service : ``client.state``
