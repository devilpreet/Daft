# avro-tar skill

Domain knowledge for working with `daft.read_avro_tar` — streaming Avro records from `.tar.gz` archives.

---

## API Reference

```python
daft.read_avro_tar(
    path: str | list[str],
    io_config: IOConfig | None = None,
    column_names: list[str] | None = None,
    file_path_column: str | None = None,
) -> DataFrame
```

| Parameter | Description |
|---|---|
| `path` | Path, glob (`*.tar.gz`), or list of paths. Supports `s3://`, `gs://`, `abfs://`, local. |
| `io_config` | `daft.io.IOConfig` with S3/GCS/Azure credentials. |
| `column_names` | Projection pushdown — read only these columns. |
| `file_path_column` | Add a column with the source `.tar.gz` path. |

Each `.tar.gz` becomes one Daft task. Archives are distributed across Ray workers automatically.
All `.avro` files across all archives must share the same schema; schema is inferred from the first file.

---

## Architecture

```
daft.read_avro_tar(paths)
  └─ AvroTarSource (DataSource)          daft/io/avro_tar/_avro_tar.py
       └─ get_tasks()
            └─ AvroTarSourceTask (per tar.gz file)
                 └─ execute()
                      ├─ streams bytes from object store (via io_config)
                      ├─ decompresses tar.gz in memory
                      └─ calls Rust avro reader (src/daft-avro/src/read.rs)
                           └─ returns RecordBatch → Daft DataFrame partition
```

Key files:
- `daft/io/avro_tar/_avro_tar.py` — Python DataSource + task implementation
- `daft/io/_avro.py` — `read_avro` / `write_avro` (single-file Avro)
- `src/daft-avro/src/read.rs` — Rust Avro reader (Apache Avro spec)
- `daft/__init__.py` — exports `read_avro`, `read_avro_tar`, `write_avro`

---

## Common Patterns

### Read from S3
```python
from daft.io import IOConfig, S3Config
import daft

io_config = IOConfig(s3=S3Config(region="us-east-1"))
df = daft.read_avro_tar("s3://my-bucket/data/*.tar.gz", io_config=io_config)
df.show()
```

### Column projection (read only needed columns)
```python
df = daft.read_avro_tar(
    "s3://my-bucket/data/*.tar.gz",
    io_config=io_config,
    column_names=["id", "event_type", "timestamp"],
)
```

### Add source file path column
```python
df = daft.read_avro_tar(
    paths,
    io_config=io_config,
    file_path_column="source_archive",
)
```

### Convert to Parquet (batch job pattern)
```python
df = daft.read_avro_tar("s3://in-bucket/raw/*.tar.gz", io_config=io_config)
df.write_parquet("s3://out-bucket/converted/")
```

---

## Test Patterns

Test helpers are in `tests/io/test_avro_tar.py`:

```python
def make_avro_bytes(data: dict, schema_name: str = "record") -> bytes:
    """Produce valid Avro OCF bytes from a dict of column-lists using daft itself."""
    df = daft.from_pydict(data)
    with tempfile.TemporaryDirectory() as tmpdir:
        df.write_avro(tmpdir, compression="null", write_mode="overwrite")
        avro_files = [f for f in os.listdir(tmpdir) if f.endswith(".avro")]
        with open(os.path.join(tmpdir, avro_files[0]), "rb") as fh:
            return fh.read()

def make_tar_gz(avro_contents: list[tuple[str, bytes]]) -> bytes:
    """Pack (name, bytes) pairs into a tar.gz in memory."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in avro_contents:
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()
```

Typical test structure:
```python
def test_something(tmp_path):
    data = {"id": [1, 2, 3], "val": ["a", "b", "c"]}
    avro_bytes = make_avro_bytes(data)
    tar_bytes = make_tar_gz([("part0.avro", avro_bytes)])

    tar_path = tmp_path / "test.tar.gz"
    tar_path.write_bytes(tar_bytes)

    df = daft.read_avro_tar(str(tar_path))
    result = df.sort("id").to_pydict()
    assert result == data
```

Run avro tests:
```bash
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_avro.py tests/io/test_avro_write.py tests/io/test_avro_tar.py"
```

---

## Debugging

Enable verbose logging:
```python
import logging
logging.getLogger("daft.io.avro_tar").setLevel(logging.DEBUG)
```

Check Rust avro reader directly:
```bash
cargo test -p daft-avro
```
