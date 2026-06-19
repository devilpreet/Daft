This is a fork of Eventual-Inc/Daft (`devilpreet/Daft`). Key additions over upstream:

- `daft.read_avro` / `daft.write_avro` — Avro file I/O (`src/daft-avro/`, `daft/io/_avro.py`)
- `daft.read_avro_tar()` — streaming Avro records from `.tar.gz` archives (`daft/io/avro_tar/`)
- Spatial join (`daft/functions/spatial.py`), delta lake merge, spill-to-disk, geo SQL functions

**Build rules:**
- Never suggest running `make build-whl` locally — wheels are built on GitHub Actions (aarch64 manylinux_2_28 only)
- Use `uv pip install` not `pip install` for this venv (managed by uv at `.venv/`)
- To rebuild the Rust extension locally: `maturin develop` (dev profile, not `--uv`)

**Testing avro changes:**
```bash
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_avro.py tests/io/test_avro_write.py tests/io/test_avro_tar.py"
```

**Git remotes:**
- `origin` → devilpreet/Daft (push here)
- `upstream` → Eventual-Inc/Daft (pull upstream changes)
- `anshulgoel27` → anshulgoel27/Daft (team fork — spatial/delta/spill features)
