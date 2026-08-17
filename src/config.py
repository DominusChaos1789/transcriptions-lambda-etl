"""Environment-driven configuration for the BDO transcripciones ETL Lambda."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Contract buckets/resources are declared with short logical names inside
    # bdo_sac_structure.json (e.g. "providers-landing", "refined", "resources").
    # Actual bucket names are that logical name prefixed with ENV_PREFIX.
    env_prefix: str
    resources_bucket: str
    contract_key: str
    core_config_key: str
    # Column-level KMS encryption is skipped for now (ENABLE_ENCRYPTION unset
    # or "false") -- sensitive columns are written as plaintext, hashing
    # still applies. Flip this env var once KMS/cryptography wiring is ready.
    enable_encryption: bool

    def resolve_bucket(self, logical_name: str) -> str:
        """Turn a contract's short bucket name (e.g. "refined") into the
        real bucket name (e.g. "augusta-nexa-dev-refined")."""
        if logical_name.startswith(self.env_prefix):
            return logical_name
        return f"{self.env_prefix}{logical_name}"


def load_settings() -> Settings:
    env_prefix = os.environ.get("ENV_PREFIX", "augusta-nexa-dev-")
    resources_bucket = os.environ.get("RESOURCES_BUCKET", f"{env_prefix}resources")
    contract_key = os.environ.get(
        "CONTRACT_KEY",
        "contracts/transacciones/empatia/transcripciones/bdo_sac_structure.json",
    )
    core_config_key = os.environ.get("CORE_CONFIG_KEY", "params/genesys/api/core.json")
    enable_encryption = os.environ.get("ENABLE_ENCRYPTION", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    return Settings(
        env_prefix=env_prefix,
        resources_bucket=resources_bucket,
        contract_key=contract_key,
        core_config_key=core_config_key,
        enable_encryption=enable_encryption,
    )
