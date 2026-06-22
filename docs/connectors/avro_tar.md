# Reading Avro Files from tar.gz Archives

Daft provides [`daft.read_avro_tar()`][daft.io.avro_tar.read_avro_tar] to read Avro files
packaged inside `.tar.gz` (or `.tgz`) archives.  This is a common pattern in data
pipelines that batch-export many small Avro files into a single compressed archive for
efficient object-store transfer.

Each archive may contain multiple `.avro` members.  All members are read and concatenated
into a single DataFrame.  Archives are distributed across workers when using the Ray
runner, giving one task per `.tar.gz` file.

## Installation

The fast in-memory read path requires **fastavro** (no temp-file I/O):

```bash
pip install "daft[avro]"
# or, if you already have daft installed:
pip install fastavro
```

Without fastavro, Daft falls back to writing each Avro member to a temporary file and
reading it via the built-in Rust Avro reader.  Functionally identical, just slower.

## Basic Usage

=== "Local Files"

    ```python
    import daft

    df = daft.read_avro_tar("/data/batch/*.tar.gz")
    df.show()
    ```

=== "S3"

    ```python
    import daft
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(s3=S3Config(region="us-east-1"))
    df = daft.read_avro_tar("s3://my-bucket/exports/*.tar.gz", io_config=io_config)
    df.show()
    ```

=== "GCS"

    ```python
    import daft
    from daft.io import GCSConfig, IOConfig

    io_config = IOConfig(gcs=GCSConfig(project_id="my-project"))
    df = daft.read_avro_tar("gs://my-bucket/exports/*.tar.gz", io_config=io_config)
    df.show()
    ```

=== "List of paths"

    ```python
    import daft

    paths = ["/data/batch_2024_01.tar.gz", "/data/batch_2024_02.tar.gz"]
    df = daft.read_avro_tar(paths)
    df.show()
    ```

## Convert to Parquet

The typical use-case is a one-off conversion pipeline:

```python
import daft
from daft.io import IOConfig, S3Config

io_config = IOConfig(s3=S3Config(region="us-east-1"))

df = daft.read_avro_tar("s3://my-bucket/avro-exports/*.tar.gz", io_config=io_config)
df.write_parquet("s3://my-bucket/parquet-output/", io_config=io_config)
```

With the Ray runner this is fully distributed — each `.tar.gz` file is processed by a
separate worker in parallel:

```python
import daft

daft.context.set_runner_ray()  # or DAFT_RUNNER=ray in your environment

df = daft.read_avro_tar("s3://my-bucket/avro-exports/*.tar.gz", io_config=io_config)
df.write_parquet("s3://my-bucket/parquet-output/", io_config=io_config)
```

## Column Projection

Read only a subset of columns to reduce memory usage:

```python
df = daft.read_avro_tar(
    "s3://my-bucket/exports/*.tar.gz",
    io_config=io_config,
    column_names=["user_id", "event_type", "timestamp"],
)
```

## Source File Path Column

Attach the source archive path as an extra column — useful for debugging or partitioning
downstream:

```python
df = daft.read_avro_tar(
    "s3://my-bucket/exports/*.tar.gz",
    io_config=io_config,
    file_path_column="source_archive",
)
df.show()
# ╭─────────────┬──────────────────────────────────────╮
# │ user_id     │ source_archive                       │
# ╞═════════════╪══════════════════════════════════════╡
# │ 1001        │ s3://my-bucket/exports/batch_01.tar… │
# ╰─────────────┴──────────────────────────────────────╯
```

## Schema Inference

The schema is inferred automatically from the first `.avro` member in the first archive.
All `.avro` files across all archives are assumed to share the same schema.

```python
df = daft.read_avro_tar("s3://my-bucket/exports/*.tar.gz", io_config=io_config)
print(df.schema())
```

## Performance Notes

| Tip | Effect |
|-----|--------|
| Install `fastavro` (`pip install daft[avro]`) | Eliminates temp-file I/O per member; 1.5–2× faster |
| Use Ray runner (`DAFT_RUNNER=ray`) | Processes each `.tar.gz` in parallel across workers |
| Use `column_names=` projection | Reduces memory per task; skips unused column decoding |
| Increase S3 connections (`S3Config(max_connections=64)`) | Reduces download latency when many small archives |

## Limitations

- All `.avro` files in all archives must share the same schema (schema is inferred from
  the first member of the first archive).
- One Daft task is created per `.tar.gz` file.  If you have very few large archives and
  many workers, consider splitting archives upstream so the number of archives ≥ number
  of workers.
- The entire archive is downloaded before any members are read (streaming tar extraction
  requires sequential access).
