# bdo-transcripciones-etl

Lambda that loads BDO SAC transcripciones (Genesys/Empatia bot conversations)
from the landing bucket, applies the `bdo_sac_structure.json` contract, and
writes a Hive-partitioned parquet dataset to the refined bucket -- then
builds the per-conversation payload a Step Function uses to call Genesys'
survey API for every conversation in the batch.

- **Source**: `s3://augusta-nexa-dev-providers-landing/external/datanexa/transacciones/empatia/transcripciones/BDO/*.json`
- **Target**: `s3://augusta-nexa-dev-refined/transacciones/empatia/transcripciones/cliente_prefijo=BDO/operacion_prefijo=SAC/year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet`
- **Contract**: `s3://augusta-nexa-dev-resources/contracts/transacciones/empatia/transcripciones/bdo_sac_structure.json`

> The contract's `output_append.prefix_pattern` (`transacciones/empatia/transcripciones`)
> is what the Lambda actually writes to -- it's the source of truth per the
> task description ("the field `output_append` shows the path where the
> parquet table is gonna be stored"). This differs slightly in segment order
> from the prefix mentioned in the original ask
> (`transacciones/transcripciones/empatia/`); if the refined-bucket layout
> should instead follow that literal path, change `output_append.prefix_pattern`
> in the contract file -- the Lambda reads it dynamically, no code change needed.

## What it does

1. Reads `bdo_sac_structure.json` from the resources bucket -- this drives
   everything else: which bucket/prefix to read, how to rename each column,
   and where to write the result.
2. Lists every `*.json` object under the contract's `source.prefix_pattern`
   and loads them. Individual unreadable files (empty objects, corrupted
   uploads, anything that fails `json.loads`) are **skipped, not fatal**
   (`s3_utils.read_json_files`): a warning is logged with the S3 key, the
   rest of the batch still gets processed and written, and the bad file is
   left in place in the landing bucket (not deleted) for investigation.
   The Lambda's return value includes `skipped_files` (the list of keys
   that failed) alongside `processed_files` (the count that succeeded).
3. For each record: renames fields per `columns[].source_column -> name`,
   stamps constant columns (`cliente_prefijo`, `operacion_prefijo`,
   `caso_uso_id`, `canal`, `origen`), casts types (`string`/`integer`/
   `timestamp`/`json`), then applies `transformations` (trim, uppercase,
   remove_accents) in the order they appear in the contract.
4. Writes the result as Hive-partitioned parquet to the refined bucket,
   via **DuckDB** (see `parquet_io.py`):
   `<prefix>/<col_1>=<value_1>/<col_2>=<value_2>/.../year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet`
   -- e.g. `.../cliente_prefijo=BDO/operacion_prefijo=SAC/year=2026/month=08/day=18/transcripciones_20260818T153045Z.parquet`.
   `partition_cols` (`cliente_prefijo`, `operacion_prefijo` per the
   contract's `output.iceberg.partition_by`) and `year=`/`month=`/`day=`
   are all real Hive-style key=value segments; the date is always the
   **processing date** (UTC, when the Lambda runs) -- not any field in the
   data, and not contract-configurable. Partition columns are excluded
   from each file's own schema (already encoded in the path) -- DuckDB's
   native `PARTITION_BY` doesn't offer that, so this groups the rows by
   distinct partition-value combinations manually (`SELECT DISTINCT ...`,
   then one `COPY ... WHERE ...` per group) instead of a single
   `PARTITION_BY` copy. Rows are staged to a local temp NDJSON file first
   (DuckDB's `read_json_auto` needs a real file), and dedup happens once,
   before splitting into groups, via a `QUALIFY ROW_NUMBER() OVER (...)`
   window function. No pandas/numpy anywhere in this pipeline -- `pyarrow`
   (build kept failing in the CI image) and then `fastparquet`+`cramjam`
   (worked, but still pandas-based) were both tried and dropped in favor
   of DuckDB, which needs neither.
5. **Deletes the source JSON objects** from the landing bucket once the
   parquet write succeeds.
6. Collects every `conversacion_id` (`genesys_cloud_id`) seen in the batch,
   resolves `params/genesys/api/core.json -> unitary -> params/genesys/api/unitary.json`,
   and returns one entry per conversation with `{conversationId}` filled
   into `/api/v2/quality/conversations/{conversationId}/surveys`. This
   return value **is** the Step Function payload -- it isn't written back to
   S3, since it's meant to feed the next state (e.g. a `Map` state calling
   the survey endpoint once per conversation).

## Not implemented (flagged rather than guessed)

- **`hash`/`encrypt` column flags in the contract**: `bdo_sac_structure.json`
  marks some columns `hash: true`/`encrypt: true` (and has a top-level
  `encryption` block with a KMS key alias), but this Lambda doesn't act on
  either flag -- sensitive columns (`consumidor_id`, `consumidor_nombre`,
  `consumidor_apellido`, `mensajes`) are written to parquet as plain values.
  A KMS/AES-GCM implementation existed at one point and was deliberately
  removed as unneeded scope; if hashing/encryption is required later, it'd
  need to be reintroduced in `transform.py`.
- **Iceberg / DynamoDB dual-write** (`output.mode: ICEBERG_DYNAMO` in the
  contract): out of scope for this Lambda, which writes plain Hive-partitioned
  parquet as the task described. A separate `iceberg-pipeline-refined` repo
  in this workspace already does an Iceberg MERGE via Glue for a related
  pipeline, if that's the pattern to extend instead.
- **Full request context for the survey call** (`headers`/`base_url`/
  `Authorization` in the `result_for_step_function.json` sample): those
  fields come from Genesys auth/config that wasn't part of any of the
  provided contract files, so they're left for whatever state/Lambda in the
  Step Function already owns Genesys auth to merge in. `request_context`
  here only carries what `unitary.json` actually defines
  (`url`, `method`, `type`, `path`, `result_data`).

## Layout

`src/` is a real Python package (`src/__init__.py`), imported as `src.*` --
Lambda's Handler is `src.main.handler`, and every module uses absolute
`src.`-prefixed imports for its siblings (e.g. `from src.contract import
Contract`), never bare/relative ones. This matches how the function is
actually deployed: the packaged zip has `src/` as a real subfolder at its
root (not flattened), because the real deployment for this Lambda goes
through a separate Jenkins pipeline, not `sam build`/`sam deploy` --
`template.yaml` documents the equivalent Handler/env/IAM contract, it
isn't the literal build mechanism.

```
src/
  __init__.py              # makes src/ an actual package
  main.py                 # orchestrates the below; `handler(event, context)` entrypoint
  config.py                # env vars -> Settings, logical->real bucket name resolution
  contract.py               # loads bdo_sac_structure.json, resolves core.json -> unitary.json
  s3_utils.py                # list/read/delete JSON, generic S3 helpers
  transform.py                 # record -> row mapping (rename/cast/transform), pandas-free
  parquet_io.py                 # rows -> Hive-partitioned parquet on S3, via DuckDB
  step_function.py                # conversation ids -> Step Function payload
test/
  fixtures/            # copies of the provided contract/config/sample files
  test_*.py            # pytest unit tests (moto-mocked S3, no real AWS calls) -- import from
                        # src.* too, e.g. `from src.main import handler`
template.yaml         # AWS SAM deployment definition (reference; see note above)
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ENV_PREFIX` | `augusta-nexa-dev-` | Prepended to the contract's logical bucket names (`providers-landing`, `refined`, `resources`) to get the real bucket name. |
| `RESOURCES_BUCKET` | `${ENV_PREFIX}resources` | Where the contract and Genesys API params live. |
| `CONTRACT_KEY` | `contracts/transacciones/empatia/transcripciones/bdo_sac_structure.json` | Contract object key. |
| `CORE_CONFIG_KEY` | `params/genesys/api/core.json` | Resolved to find the `unitary.json` path. |

## Running the tests

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt   # .venv/bin/pip on macOS/Linux
.venv/Scripts/python -m pytest -v
```

All mocked (moto for S3) -- no AWS credentials or network access needed.
Covers: contract/bucket resolution, column renaming/defaults/type casting,
transformation order (trim -> uppercase -> remove_accents), JSON column
serialization (the `mensajes` column), DuckDB-based partitioning/dedup/
timestamp-casting (`test_parquet_io.py`), S3 list/read/delete, skipping
unreadable source files without failing the batch, Step Function URL
templating, and a full end-to-end `src.main.handler` run against seeded
fixture files.

## Formatting & linting

```bash
.venv/Scripts/python -m black src/ test/     # format
.venv/Scripts/python -m flake8 src/ test/    # lint (line length/config in .flake8)
```

## Deploying

```bash
sam build --use-container   # duckdb ships a compiled native extension; container
                              # build matches it to the Lambda runtime
sam deploy --guided
```

`template.yaml` grants the function: read+list on the landing/resources
buckets, delete on the landing bucket, and put on the refined bucket.

`requirements.txt` is for local dev/tests (includes `pytest`/`moto`/
`black`/etc. via `requirements-dev.txt`). The CI packaging step should
instead install from [requirements-lambda.txt](requirements-lambda.txt) --
production dependencies only, no dev/test tooling. That distinction is what
actually matters for Lambda's 250 MB unzipped deployment-package limit:
measured locally, `duckdb`+`boto3`+`botocore` together are only ~66 MB
(`duckdb` ~37 MB replaced the earlier `pandas`+`numpy`+`fastparquet`+
`cramjam` stack, which was ~112 MB by itself), comfortably under the cap.
`moto` alone is ~42 MB and the full dev toolchain (`pytest`, `pylint`,
`pre-commit`, `black`, `flake8`, `isort`, `boto3-stubs`, ...) adds up to
~75 MB more -- accidentally bundling that dev group is what actually risks
tripping the limit, not the production dependencies.
