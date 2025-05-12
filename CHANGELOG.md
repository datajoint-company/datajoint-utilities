# Changelog

Observes [Semantic Versioning](https://semver.org/spec/v2.0.0.html) standard and [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) convention.

## [0.6.0] - 2024-04-09

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
