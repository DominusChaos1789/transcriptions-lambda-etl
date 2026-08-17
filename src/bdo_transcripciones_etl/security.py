"""Column-level security per the contract's `encryption` block: sha256
hashing for `hash: true` columns, and KMS-envelope AES-GCM encryption for
`encrypt: true` columns.

One data key is generated per Lambda invocation (per batch of files) and
reused for every row/column that needs encrypting -- the encrypted copy of
that data key is stored alongside each value so any row can be decrypted
independently later using only the KMS key, without needing the batch's
other rows.
"""
import base64
import os
from dataclasses import dataclass
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def hash_value(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass
class DataKey:
    plaintext: bytes
    encrypted_b64: str
    key_alias: str


class EncryptionService:
    """Wraps a single KMS data key for the lifetime of one batch run."""

    def __init__(self, kms_client, key_alias: str):
        self._kms = kms_client
        self.key_alias = key_alias
        self._data_key: Optional[DataKey] = None

    def _ensure_data_key(self) -> DataKey:
        if self._data_key is None:
            response = self._kms.generate_data_key(KeyId=self.key_alias, KeySpec="AES_256")
            self._data_key = DataKey(
                plaintext=response["Plaintext"],
                encrypted_b64=base64.b64encode(response["CiphertextBlob"]).decode("ascii"),
                key_alias=self.key_alias,
            )
        return self._data_key

    def encrypt(self, plaintext_value: str, aad: str) -> dict:
        """Returns {"ciphertext": <b64 nonce+ct>, "encrypted_data_key": <b64>}."""
        key = self._ensure_data_key()
        aesgcm = AESGCM(key.plaintext)
        nonce = os.urandom(12)
        ct = aesgcm.encrypt(nonce, plaintext_value.encode("utf-8"), aad.encode("utf-8"))
        return {
            "ciphertext": base64.b64encode(nonce + ct).decode("ascii"),
            "encrypted_data_key": key.encrypted_b64,
        }

    def decrypt(self, envelope: dict, aad: str) -> str:
        encrypted_data_key = base64.b64decode(envelope["encrypted_data_key"])
        plaintext_key = self._kms.decrypt(CiphertextBlob=encrypted_data_key)["Plaintext"]
        aesgcm = AESGCM(plaintext_key)
        raw = base64.b64decode(envelope["ciphertext"])
        nonce, ct = raw[:12], raw[12:]
        return aesgcm.decrypt(nonce, ct, aad.encode("utf-8")).decode("utf-8")


class NoOpEncryptionService:
    """Used when the contract's encryption block is disabled or KMS isn't
    reachable in a given environment (e.g. local/dry-run). Values pass
    through unchanged -- callers should not rely on this for real PII."""

    def encrypt(self, plaintext_value: str, aad: str) -> dict:
        return {"ciphertext": plaintext_value, "encrypted_data_key": None}
