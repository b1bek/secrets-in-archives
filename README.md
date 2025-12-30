# Secrets in Archives

A pipeline to process archives (zip, tar, rar) from Cloudflare R2, scan them for secrets using TruffleHog, and store verified results in a PostgreSQL database.

## Features

- **Automated Pipeline**: Downloads, extracts, scans, and cleans up archives from R2.
- **Smart Resume**: Tracks file hashes (ETag) to skip already processed files and resume interrupted downloads.
- **Verified Secret Scanning**: Uses [TruffleHog](https://github.com/trufflesecurity/trufflehog) to find *verified* secrets (active credentials).
- **Deduplication**: Stores unique secrets in PostgreSQL to avoid duplicates.
- **Modular Design**: Clean Python project structure managed by `uv`.

## Setup

1.  **Prerequisites**:
    *   Python 3.10+
    *   [uv](https://github.com/astral-sh/uv) (for dependency management)
    *   PostgreSQL database
    *   TruffleHog installed and in your PATH

2.  **Installation**:
    ```bash
    uv sync
    source .venv/bin/activate
    ```

3.  **Configuration**:
    Create a `.env` file based on `.env.example`:
    ```env
    R2_ENDPOINT_URL=https://<account_id>.r2.cloudflarestorage.com
    R2_ACCESS_KEY_ID=<your_access_key>
    R2_SECRET_ACCESS_KEY=<your_secret_key>
    R2_BUCKET_NAME=<your_bucket_name>
    DATABASE_URL=postgresql://user:password@localhost:5432/dbname
    ```

## Usage

Run the pipeline:
```bash
uv run python main.py
```

## Project Structure

- `src/`: Source code modules (config, db, r2, archive, scanner).
- `main.py`: Entry point and pipeline orchestration.
- `temp_processing/`: Temporary directory for file operations (automatically cleaned up).

## Database Schema

The pipeline automatically creates the following tables:
- `scan_results`: Stores found secrets (Detector Name, Raw Secret, Full JSON).
- `processed_files`: Tracks file hashes (ETags) to manage resume capability.
