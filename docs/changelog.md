---
title: Changelog
---
## 7.7.3 (April 29, 2025)
- Fixed version endpoint for Info Service
- Version endpoint fix enables the info service to be used to find image mirrors and to request image mirrors for getting datastack info.

## 7.7.2 (April 27, 2025)
- Removed node validation test from SkeletonClient.generate_bulk_skeletons_async() to significantly speed up that function. The tests will are still performed later in the pipeline.

## 7.7.1
- Added root id layer check and node validation check to SkeletonClient.generate_bulk_skeletons_async() to pull such checks as early in the process as possible. They are also performed in the server-side function associated with this function, as well as in the individual skeletonization process performed by the skeletonization workers for each individual root id.

## 7.7.0

## 7.6.3

## 7.6.2 (March 13, 2025)
- Bug fix. SWC skeletons weren't working through the skeleton client's get_bulk_skeletons().

## 7.6.1 (March 12, 2025)
- Verbose flags passed into skeleton client are now passed along to the server.

## 7.6.0 (February, 2025)
- Added tables.find() and views.find().
 Asynchronous skeleton generation and low/high priority skeleton generation (via PubSub).

## 7.5.1 (January 27, 2025)
- Skeleton client get_skeleton(), when called with a 'dict' output format request, now populates the dictionary with numpy arrays instead of Python lists.

## 7.4.3 (December 20, 2024)
- Enabled skeleton client to generate & retrieve the new V4 skeleton format (and all future skeleton versions without necessitating CAVEclient upgrades -- unless the hard-coded default version needs to be continually updated I suppose).

## 7.4.2 (December 18, 2024)
- Eliminated skeleton client skeleton_exists() issue in which passing a large number of root ids would cause a "URI too big" error.

## 7.4.1 (December 17, 2024)
- Enabled skeleton client skeleton_exists() and generate_bulk_skeletons_async() endpoints to accept a either a list or a numpy array of root ids.
- Eliminated skeleton client generate_bulk_skeletons_async() issue in which passing a large number of root ids would cause a "URI too big" error.

## 7.4.0 (December 12, 2024)
annotation module: Added support for "None", "none", etc. to remove notice_text and 
improved documentation on method

## 7.3.2 (December 10, 2024)
Dropped oldest supported numpy requirements and declared python 3.8 support

## 7.3.1 (December 5, 2024)
Improved documentation to annotation.update_table_metadata

## 7.3.0 (December 5, 2024)
Added get_minimal_covering_nodes to chunkedgraph (#274)

## 7.2.1 (December 3, 2024)
Added raw strings to avoid deprecation warning (#275)

## 7.2.0 (December 3, 2024)
Allow get_state_json to accept direct URLs (#271)

## 7.1.0 (December 3, 2024)
Added get_leaves_multiple to chunkedgraph module (#273)

## 7.0.0 (November 20, 2024)

- Simplified skeleton client interface to only accept formats of 'dict' and 'swc'
  Structure of the dictionary returned was also changed compared to previous "json" format, where the dict is flat with only one level of keys.
  Corresponding change made to MeshParty 1.18.0 allows for hydration of MeshParty Skeleton object from this dict format. 

## 6.5.0 (November 15, 2024)

- Added endpoints for bulk skeleton retrieval and querying whether a skeleton exists or not.

## 6.4.1

- Fix networkx deprecation warning.

## 6.4.0 (October 31, 2024)

- Added a CAVEclientMock function to ease the use of fully mocked CAVEclients in testing.
- Fix a bug about use of global server.
- Fix writing of local server secret to be more secure.

## 6.3.0

- Add a convenience function to get a complete L2 feature dataframe.

## 6.2.0

- Fix things so that auth will not error if the secrets directory is read-only.

## 6.1.2

- Fix query filter bug in tables interface.

## 6.1.1

- Fix warnings.

## 6.1.0

- Added support for inequality filters for numerical columns in materialization tables interface.

## 6.0.0 (October 14, 2024)

- Refactored CAVEclient internals away from a factory syntax. Most users should not notice a difference, but code that relied on specific subclient type logic may be affected.

## 5.25.0

- Added ability to suppress table warnings in client.materialize.query_table and similar methods by setting `log_warnings=False`

## 5.20.0 (April 8, 2024)

- Added generalized support for detecting server versions to provide timely exceptions to users
- Used new support to check that chunkegraph has updated version before using spatial bounds kwarg
  on client.chunkedgraph.level2_chunk_graph
- Added support for postign and getting segment properties files to client.state

## 5.18.0

- Added serialization support for pandas.index

## 5.17.3

- Minor documentation typo fix

## 5.17.2

- Bug fixes related to table_manager interface

## 5.17.1

- Bug fixes related to table_manager interface

## 5.17.0

- Fix attrs in dataframe attributes of client.materialize results to remove numpy arrays to allow concatenation of dataframes
- Added getting multiple schemas in one call to improve initialization of table_manager interface of materialization

## 5.16.1

- Bugfix on client.chunkedgrpah.level2_chunk_graph

## 5.16.0

- Added bounding box query to client.chunkedgraph.level2_chunk_graph
- Fix default materialization version client when server not advertising correctly

## 5.15.1 (Jan 18, 2024)

- minor improvements to release process

## 5.15.0 (Jan 18, 2024)

- Improved documentation with types
- Improved testing on more python versions
- Bugfixes for pyton 3.12 compatability

## 5.14.0 (November 24, 2023)

- Made automatic detection of neuroglancer versioning when constructing link shortener links

## 5.13.0 (October 26, 2023)

- Add option to get expired versions to client.materialize.get_versions

## 5.12.1 (October 16, 2023)

- Bugfixes for client.chunkedgraph.get_latest_roots

## 5.12.0 (October 16, 2023)

- Improved logic for client.chunkedgraph.get_latest_roots to work forward or backwards in time

## 5.11.0 (September 19, 2023)

- Added filter_regex_dict options to client.materialize.query_table interface

## 5.10.2 (August 16,2023)

- Fixed pyarrow support for live_live query

## 5.10.1 (August 14,2023)

- Changed random_sample argument to be an integer number of annotations rather than a floating fraction of table
- Added option to live_query

## 5.9.0 (August 14, 2023)

- Added support for native pyarrow deserialization, allowing upgrade to pyarrow version

## 5.8.0

- Allowed int64 root ids to serialize properly
- Added warning that client.materialize.tables interface is in beta

## 5.7.0

- Fix to ensure stop_layer is at least 1
- Added client.chunkedgraph.suggest_latest_roots

## 5.6.0

- Added views to client.materialize.tables interface
- Added optional argument to allow invalid root ids when querying live live, versus creating an exception

## 5.5.1

- documentation fixes on client.materialize.join_query

## 5.5.0

- added methods for different neuroglancer state formats to client.state.

## 5.4.3

- Added 'view' querying options to materialization
- Added client.materialize.tables interface
- Added client.materialize.get_tables_metadata to get all metadata in one call

## 5.2.0

- Added local caching of datastack names > server_address to simplify initialization of clients
  with servers other than global.daf-apis.com.

Cache is saved on a local file ~/.cloudvolume/secrets/cave_datastack_to_server_map.json

Cache will populate the first time caveclient.CAVEclient('my_datastack', server_address="https://my_server.com")
is called. Subsequent calls can then just be caveclient.CAVEclient('my_datastack').

## 5.1.0

- Added get_oldest_timestamp call to chunkedgraph

## 5.0.1

- Fixed bug with desired_resolution being set at the client level
  was being ignored in >5.0.0

## 5.0.0

- Added support for the new CAVE Materialization 3.0 API
  Includes support for the new materialization API, which allows for
  server side conversion of the units of position, and ensures that
  all positions are returned with the same units, even after joins.
- Added support for querying databases that were materialized without merging
  tables together. This will allow for faster materializations.
- Removed support for LiveLive query from the Materialization 2.0 API client.
  Note.. <5.0.0 clients interacting with MaterializationEngine >4.7.0 servers will
  use live live query but will doubly convert the units of position if you ask
  for a desired resolution, as the old client will also do a conversion server side.
- Fixed interaction with api version querying of servers from individual
  clients to work with verify=False. (useful for testing)
- Stored infromation from client about mapping between dataframe and table names
  and original column names.
- Added support for suffixes and select columns to be passed by dictionary rather than list
  making the selection an application of suffixes more explicit when there are collisions
  between column names in joined tables.

---

## Older Upgrade Notes

Change all select_column calls to pass dictionaries rather than lists.
Change all suffix calls to pass dictionaries rather than lists.
Advocate for your server administrator to upgrade to MaterializationEngine 4.7.0 or later,
so you can use the new MaterializationEngine 3.0 API and client.
