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
4. Applies `encryption`/`hash` per contract: columns marked `hash: true` get
   a `<name>_hash` (SHA-256) column; columns marked `encrypt: true` are
   AES-GCM encrypted with a KMS-generated data key (one data key per
   invocation, reused across rows; the wrapped/encrypted copy of that key is
   stored per-row in `<name>_edk` so any row is independently decryptable
   later). The plaintext value is never written to the output.
5. Deduplicates per `deduplication` (drops repeated `interaccion_id`,
   keeping the row with the latest `interaccion_fecha_fin`).
6. Writes the result as Hive-partitioned parquet
   (`cliente_prefijo=.../operacion_prefijo=.../part-<uuid>.parquet`) to the
   refined bucket.
7. **Deletes the source JSON objects** from the landing bucket once the
   parquet write succeeds.
8. Collects every `conversacion_id` (`genesys_cloud_id`) seen in the batch,
   resolves `params/genesys/api/core.json -> unitary -> params/genesys/api/unitary.json`,
   and returns one entry per conversation with `{conversationId}` filled
   into `/api/v2/quality/conversations/{conversationId}/surveys`. This
   return value **is** the Step Function payload -- it isn't written back to
   S3, since it's meant to feed the next state (e.g. a `Map` state calling
   the survey endpoint once per conversation).

## Not implemented (flagged rather than guessed)

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

```
src/bdo_transcripciones_etl/
  config.py        # env vars -> Settings, logical->real bucket name resolution
  contract.py       # loads bdo_sac_structure.json, resolves core.json -> unitary.json
  s3_utils.py        # list/read/delete JSON, generic S3 helpers
  security.py         # sha256 hashing + KMS-envelope AES-GCM encryption
  transform.py          # record -> row mapping, transformations, dedup
  parquet_io.py          # DataFrame -> Hive-partitioned parquet on S3
  step_function.py         # conversation ids -> Step Function payload
  handler.py                 # orchestrates the above; lambda_handler entrypoint
tests/
  fixtures/            # copies of the provided contract/config/sample files
  test_*.py            # pytest unit tests (moto-mocked S3/KMS, no real AWS calls)
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

20 unit tests, all mocked (moto for S3/KMS, a local fake KMS client for the
encryption round-trip tests) -- no AWS credentials or network access needed.
Covers: contract/bucket resolution, column renaming/defaults/type casting,
transformation order (trim -> uppercase -> remove_accents), hashing,
encrypt/decrypt round-trips (including the `mensajes` JSON column),
deduplication, S3 list/read/delete, Step Function URL templating, and a
full end-to-end `lambda_handler` run against seeded fixture files.

## Deploying

```bash
sam build --use-container   # pyarrow ships native extensions; container build
                              # matches them to the Lambda runtime
sam deploy --guided
```

`template.yaml` grants the function: read+list on the landing/resources
buckets, delete on the landing bucket, put on the refined bucket, and
`kms:GenerateDataKey`/`kms:Decrypt` scoped to the `alias/data-sensitive` key
(tighten the KMS resource ARN for production instead of `"*"` with an alias
condition).
