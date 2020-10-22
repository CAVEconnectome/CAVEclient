# Changelog

## [Unreleased]

### Added
- **JSONStateService**: Neuroglancer URL can be specified for the client under the property `ngl_url`.
For a FrameworkClient with a datastack name, the value is set using the `viewer_site` field from the info client.

### Changed

- **JSONStateService**: In `build_neuroglancer_url`, if `ngl_url` is None the url will be pulled from the default client value.
If there is the default value is None, only the URL to the JSON file will be returned.

## [2.0.1] - 2020-10-20

### Fixed
- **AuthClient** : Token creation and setting is more robust. Directories are created if not previously present.

## [2.0.0]

### Added
- First release of the unified FrameworkClient and system-wide authentication.