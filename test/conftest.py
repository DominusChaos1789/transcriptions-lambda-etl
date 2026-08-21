import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES_DIR = Path(__file__).parent / "fixtures"

ENV_PREFIX = "augusta-nexa-dev-"
RESOURCES_BUCKET = f"{ENV_PREFIX}resources"
LANDING_BUCKET = f"{ENV_PREFIX}providers-landing"
REFINED_BUCKET = f"{ENV_PREFIX}refined"

CONTRACT_KEY = "contracts/transacciones/empatia/transcripciones/bdo/sac/transcripcion.json"
CORE_CONFIG_KEY = "params/genesys/api/core.json"
UNITARY_KEY = "params/genesys/api/unitary.json"
SOURCE_PREFIX = "external/datanexa/transacciones/empatia/transcripciones/BDO"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("ENV_PREFIX", ENV_PREFIX)
    monkeypatch.setenv("RESOURCES_BUCKET", RESOURCES_BUCKET)
    monkeypatch.setenv("CONTRACT_KEY", CONTRACT_KEY)
    monkeypatch.setenv("CORE_CONFIG_KEY", CORE_CONFIG_KEY)


@pytest.fixture
def aws(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for bucket in (RESOURCES_BUCKET, LANDING_BUCKET, REFINED_BUCKET):
            s3.create_bucket(Bucket=bucket)

        s3.put_object(
            Bucket=RESOURCES_BUCKET,
            Key=CONTRACT_KEY,
            Body=(FIXTURES_DIR / "transcripcion.json").read_bytes(),
        )
        s3.put_object(
            Bucket=RESOURCES_BUCKET,
            Key=CORE_CONFIG_KEY,
            Body=(FIXTURES_DIR / "core.json").read_bytes(),
        )
        s3.put_object(
            Bucket=RESOURCES_BUCKET,
            Key=UNITARY_KEY,
            Body=(FIXTURES_DIR / "unitary.json").read_bytes(),
        )

        yield {"s3": s3}


@pytest.fixture
def seeded_source_files(aws):
    s3 = aws["s3"]
    keys = []
    for name in ("sample_transcription_1.json", "sample_transcription_2.json"):
        key = f"{SOURCE_PREFIX}/{name}"
        s3.put_object(Bucket=LANDING_BUCKET, Key=key, Body=(FIXTURES_DIR / name).read_bytes())
        keys.append(key)
    return keys
