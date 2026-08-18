# bdo-transcripciones-etl

Lambda that loads BDO SAC transcripciones (Genesys/Empatia bot conversations)
from the landing bucket, applies the `bdo_sac_structure.json` contract, and
writes a Hive-partitioned parquet dataset to the refined bucket -- then
builds the per-conversation payload a Step Function uses to call Genesys'
survey API for every conversation in the batch.

- **Source**: `s3://augusta-nexa-dev-providers-landing/external/datanexa/transacciones/empatia/transcripciones/BDO/*.json`
- **Target**: `s3://augusta-nexa-dev-refined/transacciones/empatia/transcripciones/cliente_prefijo=BDO/operacion_prefijo=SAC/*.parquet`
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
   and loads them.
3. For each record: renames fields per `columns[].source_column -> name`,
   stamps constant columns (`cliente_prefijo`, `operacion_prefijo`,
   `caso_uso_id`, `canal`, `origen`), casts types (`string`/`integer`/
   `timestamp`/`json`), then applies `transformations` (trim, uppercase,
   remove_accents) in the order they appear in the contract.
4. Deduplicates per `deduplication` (drops repeated `interaccion_id`,
   keeping the row with the latest `interaccion_fecha_fin`).
5. Writes the result as Hive-partitioned parquet
   (`cliente_prefijo=.../operacion_prefijo=.../part-<uuid>.parquet`) to the
   refined bucket, via `fastparquet` (see `parquet_io.py`). `pyarrow` was
   the original choice but its build kept failing in the CI image (Python
   3.13, no prebuilt wheel available there, and its source build needs the
   Apache Arrow C++ library preinstalled). `fastparquet` still isn't
   dependency-free -- it needs `cramjam` for compression -- but has had
   better wheel coverage across recent Python versions in practice.
6. **Deletes the source JSON objects** from the landing bucket once the
   parquet write succeeds.
7. Collects every `conversacion_id` (`genesys_cloud_id`) seen in the batch,
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

Flat module layout (not a package) so the Lambda's Handler setting is simply
`main.handler` -- `CodeUri: src/` puts these directly at the zip root:

```
src/
  main.py                 # orchestrates the below; `handler(event, context)` entrypoint
  config.py                # env vars -> Settings, logical->real bucket name resolution
  contract.py               # loads bdo_sac_structure.json, resolves core.json -> unitary.json
  s3_utils.py                # list/read/delete JSON, generic S3 helpers
  transform.py                 # record -> row mapping, transformations, dedup
  parquet_io.py                 # DataFrame -> Hive-partitioned parquet on S3
  step_function.py                # conversation ids -> Step Function payload
test/
  fixtures/            # copies of the provided contract/config/sample files
  test_*.py            # pytest unit tests (moto-mocked S3, no real AWS calls)
template.yaml         # AWS SAM deployment definition
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
serialization (the `mensajes` column), deduplication, S3 list/read/delete,
Step Function URL templating, and a full end-to-end `main.handler` run
against seeded fixture files.

## Formatting & linting

```bash
.venv/Scripts/python -m black src/ test/     # format
.venv/Scripts/python -m flake8 src/ test/    # lint (line length/config in .flake8)
```

## Deploying

```bash
sam build --use-container   # fastparquet's cramjam dependency ships native extensions;
                              # container build matches them to the Lambda runtime
sam deploy --guided
```

`template.yaml` grants the function: read+list on the landing/resources
buckets, delete on the landing bucket, and put on the refined bucket.

`requirements.txt` is for local dev/tests. The CI packaging step should
instead install from [requirements-lambda.txt](requirements-lambda.txt),
which deliberately excludes `boto3` (already provided by the Lambda Python
runtime) to help stay under Lambda's 250 MB unzipped deployment-package
limit.
