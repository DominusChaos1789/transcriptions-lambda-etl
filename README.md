# bdo-transcripciones-etl

Lambda that loads BDO SAC transcripciones (Genesys/Empatia bot conversations)
from the landing bucket, applies the `transcripcion.json` contract, computes
its `technical_columns` (audit/EAV metadata), and writes **two** parquet
outputs to the refined bucket -- `output_core` (business + core audit
columns, one row per conversation) and `output_atts` (EAV: one row per
business column per conversation) -- then builds the per-conversation
payload a Step Function uses to call Genesys' survey API.

- **Source**: `s3://augusta-nexa-dev-providers-landing/external/datanexa/transacciones/empatia/transcripciones/BDO/*.json`
- **Contract**: `s3://augusta-nexa-dev-resources/contracts/transacciones/empatia/transcripciones/bdo/sac/transcripcion.json`
- **output_core**: `s3://augusta-nexa-dev-refined/transacciones/parquet/transcripciones_core/cliente_prefijo=BDO/operacion_prefijo=SAC/year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet`
- **output_atts**: `s3://augusta-nexa-dev-refined/transacciones/parquet/transcripciones_atts/cliente_prefijo=BDO/operacion_prefijo=SAC/year=YYYY/month=MM/day=DD/transcripciones_<timestamp>.parquet`

This replaced an earlier, simpler contract shape (`bdo_sac_structure.json`,
single `output_append` target, no technical columns) -- that shape is no
longer supported by this Lambda at all.

## What it does

1. Reads `transcripcion.json` from the resources bucket -- this drives
   everything else: which bucket/prefix to read, how to rename each
   column, how to compute the `technical_columns`, and where both outputs
   get written.
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
   remove_accents) in the order they appear in the contract (`transform.py`).
4. Deduplicates the resulting rows per `deduplication` (drops repeated
   `interaccion_id`, keeping the row with the latest `interaccion_fecha_fin`
   -- `transform.dedup_rows`, plain Python, not SQL). This has to happen
   before technical-column computation/EAV pivoting, once, on the business
   rows: `output_atts` rows don't carry the dedup key column (`interaccion_id`)
   at all, so there's no way to dedup after pivoting.
5. Computes every `technical_columns` entry for each surviving row
   (`technical_columns.py`) and splits into the two output row-sets
   (`build_output_rows`):
   - **output_core**: each business row plus whatever technical columns
     `output_core.technical_col_keep` lists (audit/lineage columns like
     `ejecucion_id`, `registro_id`, `registro_hash`, `base_id`, `cargue_id`,
     `cargue_fecha`, ...).
   - **output_atts**: an EAV pivot -- for each business row, one output row
     *per business column* (`len(contract.columns)` rows per source
     record), carrying `output_atts.technical_col_keep`'s columns plus
     `atributo_nombre`/`atributo_tipo`/`atributo_posicion`/`atributo_valor`
     (the column's name/declared type/position/value, value always
     stringified). Sensitive columns (`consumidor_id`, etc.) are pivoted in
     plaintext -- encryption was removed from this pipeline earlier and
     hasn't been reintroduced.

   Each `technical_columns` entry has a `calculus_type` describing how its
   value is derived:
   | calculus_type | behavior |
   |---|---|
   | `execution_id` | the Lambda's own `context.aws_request_id` (falls back to a fresh UUID when there's no real Lambda context, e.g. local runs) |
   | `uniqe` | `sha256` of a fresh random UUID -- unique per record, **not** reproducible across reruns of the same source data |
   | `concatetation` | `sha256(delimiter.join(resolved column values))` |
   | `field_mapping` | first non-null value resolved from `columns`, checking in order: the business row, the contract's own top-level fields (e.g. `client_prefix`), a bracketed nested contract reference (`output_core[output_name_file]`), another already-computed technical column, or the sentinel `"No apply"` (always skipped -> null) |
   | `file_name` | the source S3 object's filename |
   | `file_landing_date` | the **processing date** (UTC, when the Lambda runs, not any field in the source data) -- columns whose name ends in `_id` get the date-only (`yyyy-mm-dd`) form, others get the full ISO datetime |
   | `column_name`/`column_type_data`/`column_value`/`column_index_position_map` | not computed by `compute_technical_columns` at all -- these are the four EAV fields, populated directly while pivoting `output_atts` rows |

   `technical_columns` entries have no `type` field in the contract JSON
   itself (only `calculus_type`/`description`/`columns`/`delimiter`) -- the
   SQL type each one gets cast to on write is the Lambda's own fixed
   knowledge (`contract._TECHNICAL_COLUMN_TYPES`), defaulting to `string`
   for anything unlisted.
6. Writes both row-sets as Hive-partitioned parquet to the refined bucket,
   via **DuckDB** (`parquet_io.write_hive_parquet`, called once per output):
   `<prefix>/<col_1>=<value_1>/<col_2>=<value_2>/.../year=YYYY/month=MM/day=DD/<output_name_file>_<timestamp>.parquet`.
   `partition_cols` (`cliente_prefijo`, `operacion_prefijo` per the
   contract's `output.iceberg.partition_by`, shared by both outputs) and
   `year=`/`month=`/`day=` are all real Hive-style key=value segments.
   Partition columns are excluded from each file's own schema (already
   encoded in the path) -- DuckDB's native `PARTITION_BY` doesn't offer
   that, so this groups the rows by distinct partition-value combinations
   manually (`SELECT DISTINCT ...`, then one `COPY ... WHERE ...` per
   group) instead of a single `PARTITION_BY` copy. Rows are staged to a
   local temp NDJSON file first (DuckDB's `read_json_auto` needs a real
   file). Every column present in the rows is explicitly `CAST` to the SQL
   type matching its contract-declared `type` (`string`->`VARCHAR`,
   `integer`->`BIGINT`, `timestamp`->`TIMESTAMP`, `json`->`VARCHAR`)
   instead of trusting DuckDB's `read_json_auto` type inference -- that
   inference over-eagerly promotes UUID-*shaped* strings (like
   `conversacion_id`, which holds `genesys_cloud_id` values) to DuckDB's
   native `UUID` logical type, which most Parquet readers besides DuckDB
   itself don't understand (AWS S3 Select fails outright with
   `Unsupported Parquet type UUID` on it). No pandas/numpy anywhere in
   this pipeline -- `pyarrow` (build kept failing in the CI image) and
   then `fastparquet`+`cramjam` (worked, but still pandas-based) were both
   tried and dropped in favor of DuckDB, which needs neither.
7. **Deletes the source JSON objects** from the landing bucket once both
   writes succeed.
8. Collects every `conversacion_id` (`genesys_cloud_id`) seen in the batch,
   resolves `params/genesys/api/core.json -> unitary -> params/genesys/api/unitary.json`,
   and returns one entry per conversation with `{conversationId}` filled
   into `/api/v2/quality/conversations/{conversationId}/surveys`. This
   return value **is** the Step Function payload -- it isn't written back to
   S3, since it's meant to feed the next state (e.g. a `Map` state calling
   the survey endpoint once per conversation).

## Known gaps in the contract itself (flagged, not silently guessed)

`transcripcion.json` has a couple of inconsistencies between
`technical_col_keep` (what each output claims to carry) and
`technical_columns` (what's actually defined):

- **`archivo_fecha_id`** is listed in `output_atts.technical_col_keep` but
  had no `technical_columns` entry at all. Added one (`calculus_type:
  file_landing_date`, mirroring `cargue_fecha_id`) to both the test
  fixture and this note -- **the real contract file in S3 needs the same
  addition**, or `archivo_fecha_id` will come back `null` in `output_atts`.
- **`gestion_tipo`** and **`gestion_canal`** are listed in
  `output_core.technical_col_keep` but have no `technical_columns` entry
  and no obvious source field in the contract's top-level metadata
  (unlike `archivo_fecha_id`, there wasn't a clear "mirror this other
  column" pattern to follow). Currently these just come back `null` in
  `output_core` -- `contract.technical_column_types` gives them a safe
  `string` SQL type so the write doesn't fail, but the *values* need a
  real `technical_columns` definition added to the contract before
  they'll be populated.
- **`parquet_nombre`**'s `field_mapping` resolves to
  `output_core[output_name_file]` (`"transcripciones"`, the literal
  string) per the contract's own definition -- worth double-checking
  against your actual intended behavior, since a sample output row shown
  during development had a hash-like value there instead of the literal
  filename prefix, which this implementation doesn't currently produce.

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
  __init__.py                # makes src/ an actual package
  main.py                   # orchestrates the below; `handler(event, context)` entrypoint
  config.py                   # env vars -> Settings, logical->real bucket name resolution
  contract.py                   # loads transcripcion.json, resolves core.json -> unitary.json
  s3_utils.py                     # list/read/delete JSON, generic S3 helpers
  transform.py                      # record -> row mapping (rename/cast/transform) + dedup_rows, pandas-free
  technical_columns.py                # technical_columns calculus engine + output_core/output_atts row-building
  parquet_io.py                         # rows -> Hive-partitioned parquet on S3, via DuckDB
  step_function.py                        # conversation ids -> Step Function payload
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
| `CONTRACT_KEY` | `contracts/transacciones/empatia/transcripciones/bdo/sac/transcripcion.json` | Contract object key. |
| `CORE_CONFIG_KEY` | `params/genesys/api/core.json` | Resolved to find the `unitary.json` path. |

## Running the tests

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt   # .venv/bin/pip on macOS/Linux
.venv/Scripts/python -m pytest -v
```

All mocked (moto for S3) -- no AWS credentials or network access needed.
Covers: contract/bucket resolution, column renaming/defaults/type casting,
transformation order (trim -> uppercase -> remove_accents), dedup
(`test_transform.py`), the technical_columns calculus engine and
output_core/output_atts row-building (`test_technical_columns.py`),
DuckDB-based partitioning/type-casting (`test_parquet_io.py`), S3
list/read/delete, skipping unreadable source files without failing the
batch, Step Function URL templating, and a full end-to-end
`src.main.handler` run against seeded fixture files.

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
buckets, delete on the landing bucket, and put on the refined bucket (the
single `refined` bucket permission covers both `output_core` and
`output_atts`, since they're just different prefixes within it).

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
