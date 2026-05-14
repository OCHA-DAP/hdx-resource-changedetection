# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed

- `check_urls()` now creates asyncio tasks in chunks of `TASK_CHUNK_SIZE` (500) rather than all upfront, so the event-loop task count is bounded regardless of the total number of resources.
- `ZIP_SIZE_THRESHOLD` raised from 75 MB to 100 MB to reflect 2 GB server memory; worst-case in-memory ZIP buffer across 13 concurrent connections is now ~1.3 GB, leaving ~300 MB headroom after OS/interpreter overhead.

### Fixed

- `get_async_crc_sum()` now validates the HTTP 206 status on the central-directory range request, preventing a full file body from being read into memory if the server ignores the Range header.
- Netloc extraction in `dataset_processor.get_distributed_resources_to_check()` and the `netlocs` test fixture now correctly reads the URL from index 2 of the resource tuple (was incorrectly using index 0 after the tuple layout changed).
- `existing_hash` parameter of `Retrieval.fetch()` now has type annotation `str | None` to match its actual callers.
- `test_retrieval_large.py` now passes a bare hostname (`"data.worldpop.org"`) to `Retrieval` instead of a full URL, consistent with how all other tests supply netlocs.

## [0.0.1] - 2025-02-26

### Added

-
