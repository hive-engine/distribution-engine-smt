# Changelog

This document details the significant changes between the `main` and `dev` branches.

## 🚀 Features & Improvements

- **Modernized Python Support:** The codebase has been updated to support modern Python versions (>=3.13) and now uses `pyproject.toml` for dependency management.
- **Replaced Core Libraries:**
  - Replaced the unmaintained `beem` library with `hive-nectar` for all Hive RPC interactions.
  - Replaced `steemengine` with `nectarengine` for interacting with the Hive Engine sidechain.
- **Performance Enhancements:**
  - **Batch Block Processing:** Introduced batch processing for both mainnet and sidechain blocks, which can be enabled via the `enable_hive_bulk_blocks` and `enable_engine_bulk_blocks` settings in `config.json`. This significantly improves initial sync speed.
  - **Improved Streaming Logic:** Refined block streaming with more robust timestamp tracking and error handling.
- **API Enhancements:**
  - **Enhanced `/state` Endpoint:** The `/state` endpoint now provides more detailed streaming status, including information about the sidechain sync progress.
  - **Case Normalization:** All API endpoints now consistently handle `token`, `author`, and `permlink` parameters in a case-insensitive manner.
  - **Simplified Vote Fetching:** The logic for fetching votes has been optimized to reduce redundant calls.
- **New Tooling:**
  - **`update_token_config.py`:** A new script has been added to allow for automatically updating the token configuration from a remote API endpoint.

## 🔧 Refactoring & Code Quality

- **Code Modernization:**
  - Removed Python 2 compatibility code and `__future__` imports.
  - Replaced `datetime.utcnow()` with timezone-aware `datetime.now(timezone.utc)` for more accurate timestamp handling.
  - Removed the `python-dateutil` dependency in favor of the standard library's `datetime` module.
  - Stream scripts (`stream_blocks.py` and `stream_engine_sidechain_blocks.py`) have been refactored into dedicated Processor classes (`HiveStreamProcessor` and `EngineStreamProcessor`) for improved modularity and maintainability.
- **Standardized Scripts:** All `run-*.sh` scripts have been updated for better compatibility and consistency.
- **Shell Script Updates:** Reverted shell scripts to use the original `main` branch paths and standardized the Python interpreter to `python3`.
- **Improved Logging:** Logging has been updated to be more consistent and provide better insights into the application's behavior.

## 🐛 Bug Fixes

- **Timestamp Handling:** Corrected timestamp handling to be timezone-aware, preventing potential issues with block processing and payout calculations.
- **Gunicorn Configuration:** The production API server script now correctly configures a temporary directory for Gunicorn workers.

