# Fork-specific Context

This is **devilpreet/Daft**, a personal maintained fork of Eventual-Inc/Daft.

- **Fork owner:** `devilpreet` (GitHub account)
- **Team fork:** `anshulgoel27/Daft` — pull spatial join, delta lake, spill-to-disk, and SQL function commits from here occasionally
- **Primary branch:** `feature/avro-tar-support` (33+ commits ahead of upstream/main)
- **Key additions over upstream:**
  - `daft.read_avro` / `daft.write_avro` — Avro file I/O (src/daft-avro/)
  - `daft.read_avro_tar()` — streaming Avro from `.tar.gz` archives (daft/io/avro_tar/)
  - Spatial join (daft.functions.spatial), delta lake merge, spill-to-disk, geo functions
- **Wheel builds happen on GitHub Actions only** — never run `make build-whl` locally
- **Avro tests after any avro change:**
  ```
  DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_avro.py tests/io/test_avro_write.py tests/io/test_avro_tar.py"
  ```
- **Remotes:** `origin` → devilpreet/Daft, `upstream` → Eventual-Inc/Daft, `anshulgoel27` → anshulgoel27/Daft
- **Python env:** `uv pip install` (not pip). venv at `.venv`.

---

# Resources

- https://docs.daft.ai for the user-facing API docs
- CONTRIBUTING.md for detailed development process
- https://github.com/Eventual-Inc/Daft for issues, discussions, and PRs

# Dev Workflow

1. [Once] Set up Python environment and install dependencies: `make .venv`
2. [Optional] Activate .venv: `source .venv/bin/activate`. Not necessary with Makefile commands.
3. If Rust code is modified, rebuild: `make build`
4. Run tests. See [Testing Details](#testing-details).

# Testing Details

- `make test` runs tests in `tests/` directory. Uses `pytest` under the hood.
  - Must set `DAFT_RUNNER` environment variable to `ray` or `native` to run the tests with the corresponding runner.
    - Start with `DAFT_RUNNER=native` unless testing Ray or distributed code.
  - `make test EXTRA_ARGS="..."` passes additional arguments to `pytest`.
    - `make test EXTRA_ARGS="-v tests/dataframe/test_select.py"` runs the test in the given file.
    - `make test EXTRA_ARGS="-v tests/dataframe/test_select.py::test_select_dataframe"` runs the given test method.
  - Default `integration`, `benchmark`, and `hypothesis` tests are disabled. Best to run on CI.
- `make doctests` runs doctests in `daft/` directory. Tests docstrings in Daft APIs.

# PR Conventions

- Titles: Conventional Commits format; enforced by `.github/workflows/pr-labeller.yml`.
- Descriptions: follow `.github/pull_request_template.md`.
