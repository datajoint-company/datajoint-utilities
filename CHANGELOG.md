# Changelog

Observes [Semantic Versioning](https://semver.org/spec/v2.0.0.html) standard and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) convention.


## [0.7.0] - 2025-10-31

### Added
- PopulateHandler now supports per-table notification configuration with dynamic table addition
- DataJointWorker integration with notification system:
  - Optional notifiers parameter to enable notifications
  - Per-table notification control via `notify_on` parameter in `add_step()` and `__call__()`
  - Automatic DEBUG log level management for PopulateHandler visibility

## [0.6.2] - 2025-06-04

- Fix - bugfix data copy with DataJoint's latest `topo_sort()`

## [0.6.1] - 2025-05-10

- Fix - improve "idle cycles" logic for DataJointWorker


## [0.6.0] - 2025-04-09

### Added
- New stale jobs handling functionality in worker:
  - Time-based stale job detection with configurable timeout
  - Flexible action options: mark as error, remove, or query without modification
  - Improved connection-based stale job detection
  - Backward compatibility with existing code

## [0.5.3] - 2025-04-04

- Fix - use `Connection` object argument when creating schemas to access jobs table

## [0.5.2] - 2025-03-06

- Added - allow `Connection` object argument to `get_workflow_operation_overview` in worker

## [0.5.1] - 2024-11-15

- Added - apply restrictions to `get_key_source_count` in worker

## [0.5.0] - 2024-11-08

### Added

- Started CHANGELOG
- Install with `pyproject.toml`


[0.0.0]: https://github.com/datajoint-company/datajoint-utilities/releases/tag/0.5.0
