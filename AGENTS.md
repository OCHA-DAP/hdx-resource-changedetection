# HDX Resource Change Detection — Agent Guidelines

## Project Overview

This project monitors resources on the [Humanitarian Data Exchange (HDX)](https://data.humdata.org) platform. It periodically downloads resources, computes checksums, compares against stored metadata, and updates HDX datasets when changes are detected (broken links, new hashes, sizes, etc.).

## Tech Stack

- **Python 3.10+** (CI runs on 3.13/3.14)
- **aiohttp** — async HTTP downloads
- **hdx-python-api**, **hdx-python-pipelineutils**, **hdx-python-utilities** — HDX platform integration
- **redis** — distributed task queue
- **tenacity** — retry logic with exponential backoff
- **uv** — package management
- **Hatchling + hatch-vcs** — build backend; version derived from git tags
- **Ruff** — linting and formatting (replaces black/isort/flake8)
- **pytest**, **pytest-asyncio**, **pytest-check**, **pytest-mock**, **pytest-cov** — testing

## Repository Layout

```
src/hdx/resource/changedetection/
    cli.py                  # CLI entry point via HDX facade()
    __main__.py             # python -m entry point
    config.py               # Logging init, constants
    dataset_processor.py    # Fetches/filters HDX datasets and resources
    retrieval.py            # Async HTTP downloads, hashing, ETag handling
    retrieval_utilities.py  # File signature/MIME-type validation helpers
    results.py              # Compares new vs stored metadata; marks broken links
    dataset_updater.py      # Writes updated resource metadata back to HDX
    task_manager.py         # Redis-backed distributed task coordination
    utilities.py            # Status reporting, CSV export, HTTP error classification
    tenacity_custom_wait.py # Custom backoff strategy for 429 responses
    name_generator.py       # Random human-readable instance IDs
tests/
    conftest.py             # Session-scoped fixtures (config, URLs, netlocs)
    test_retrieval.py
    test_dataset_processor.py
    test_results.py
    test_task_manager.py
    test_retrieval_large.py
    test_retrieval_fallback.py
```

## Commands

```bash
# Install (development)
uv sync

# Run tests
pytest

# Run tests including Redis-dependent tests (requires Redis on localhost)
pytest --run-redis

# Lint and format check
hatch fmt --check
# or
uv run ruff format --check && uv run ruff check

# Auto-fix lint/format issues
uv run ruff format && uv run ruff check --fix

# Build package
hatch build

# Run the tool
python -m hdx.resource.changedetection
# or, if installed
hdx-resource-changedetection
```

## Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format. Add an entry under `[Unreleased]` (creating the section if it doesn't exist) whenever you make a change that falls into one of these categories:

- **Added** — new features or capabilities
- **Changed** — behaviour changes, architectural refactors, or notable performance improvements
- **Fixed** — bug fixes
- **Removed** — removed features or code

Do not add entries for pure style/formatting fixes, comment updates, or test-only changes that don't affect production behaviour.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix. Unrelated changes obscure the intent of the fix and complicate review and blame.

## Key Conventions

### Code Style
- **Ruff** is the sole formatter/linter. Do not introduce black, isort, or flake8 separately.
- Line length limit is not enforced (E501 is ignored).
- Use modern Python type hint syntax: `list[str]`, `dict[str, int]`, `str | None` (not `List`, `Dict`, `Optional`).

### Async Patterns
- All HTTP work is async via `aiohttp`. Do not use `requests` in async contexts.
- `AsyncLimiter` instances must be created per event loop — never reused across loops.
- Stream large files rather than loading them fully into memory.

### File Size Strategy (retrieval.py)
The retrieval strategy switches based on file size:
- **< 31 MB**: full download + MD5 hash
- **31 MB – 419 MB**: CRC-32 computation (no full download)
- **> 419 MB**: ETag-only (no hashing)

Thresholds are constants in `retrieval.py`. Do not hardcode magic numbers elsewhere.

### Resource Tuple Layout
Resources are stored as tuples in this exact order:
```
(dataset_id, resource_id, url, format, hash, size, last_modified, broken_link)
```
Result tuples from retrieval:
```
(size, last_modified, etag, final_hash, sig_match, mime_match, size_match, http_status, status_code)
```

### Custom Status Codes (utilities.py)
| Code | Meaning |
|------|---------|
| `0` | OK |
| `-1` | MIME type mismatch |
| `-2` | File signature mismatch |
| `-3` | Size mismatch |
| `-10` | `ClientResponseError` |
| `-11` | Unknown error |

### Error Handling
- Server errors (5xx) retry with exponential backoff (up to 3 attempts via tenacity).
- 429 responses use a custom wait strategy from `tenacity_custom_wait.py`.
- Client errors (4xx, excluding 429) do not retry; broken-link flag is set on the resource.

### HDX Integration
- Configuration is read from `.hdx_configuration.yaml` and `.useragents.yaml` at runtime.
- CLI arguments are parsed by `hdx.facades.infer_arguments.facade()`.
- Do not hardcode HDX API keys or site URLs.

### Redis / Distributed Tasks
- `task_manager.py` uses WATCH/MULTI/EXEC for atomic task locking.
- Stuck tasks reset automatically after 24 hours.
- Instance IDs are generated as `color-adjective-animal` strings.

### Logging
- Initialise logging only via `config.init_logging()`.
- `LOG_FILE_PATH` and `LOG_LEVEL` environment variables are respected.
- Structured error details go to `errors.log`.

### Testing
- Use `pytest-check` for non-blocking assertions (multiple failures per test).
- Mark Redis-dependent tests with `@pytest.mark.needs_redis`; they are skipped unless `--run-redis` is passed.
- Session-scoped fixtures are preferred for heavy resources (HDX Configuration, URL lists).
- Tests mix real HTTP URLs (GitHub, HDX) with mocked responses — keep live-network tests clearly separated.

### Pre-commit Hooks
Pre-commit uses a non-standard config location. After cloning, install hooks with:
```bash
pre-commit install
```
If hooks do not run, check `.pre-commit-config.yaml` and manually update `.git/hooks/pre-commit` as described in README.

The `uv-lock` and `pip-compile` hooks auto-regenerate `requirements.txt` and `requirements-test.txt` from `pyproject.toml` — do not edit those files by hand.

### Versioning
Version is derived automatically from git tags via `hatch-vcs`. Do not set a version manually in `pyproject.toml` or `_version.py`.

### Docker
- Multi-stage build: builder installs deps, runtime image is lean.
- Git must be present in the builder stage for `hatch-vcs` versioning.
- `src/` and `tests/` directories are excluded from the final image.
- The entrypoint script (`docker/entrypoint.sh`) performs environment variable substitution.
