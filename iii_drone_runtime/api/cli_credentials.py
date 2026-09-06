"""Fail-closed authentication against receiver-derived machine verifiers."""

from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
import re


IDENTITY = re.compile(r"^[a-f0-9]{64}$")
TOKEN = re.compile(r"^[A-Za-z0-9_-]{43,128}$")


class RuntimeCliCredentialVerifier:
    def __init__(self, path: Path, *, require_root_owner: bool) -> None:
        self.path = path
        self.require_root_owner = require_root_owner

    def authenticate(self, token: str | None) -> str:
        if token is None or TOKEN.fullmatch(token) is None:
            raise RuntimeError("missing or invalid CLI machine credential")
        value = self._load()
        observed = hashlib.sha256(token.encode("ascii")).hexdigest()
        matches = [
            item["machine_id"]
            for item in value["clients"]
            if hmac.compare_digest(item["token_sha256"], observed)
        ]
        if len(matches) != 1:
            raise RuntimeError("missing or invalid CLI machine credential")
        return matches[0]

    def validate(self) -> None:
        """Authenticate the complete verifier store without exposing its values."""

        self._load()

    def _load(self) -> dict:
        if self.path.is_symlink() or not self.path.is_file():
            raise RuntimeError("Runtime API machine verifier store is unavailable")
        metadata = self.path.stat(follow_symlinks=False)
        if metadata.st_mode & 0o022:
            raise RuntimeError("Runtime API machine verifier store is writable")
        if self.require_root_owner and metadata.st_uid != 0:
            raise RuntimeError("Runtime API machine verifier store is not root-owned")
        try:
            raw = self.path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"cannot read Runtime API machine verifier store: {exc}"
            ) from exc
        canonical = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        if not isinstance(value, dict) or raw != canonical + b"\n":
            raise RuntimeError(
                "Runtime API machine verifier store is not canonical JSON"
            )
        if (
            set(value)
            != {
                "schema",
                "verifier_id",
                "access_id",
                "generation",
                "clients",
            }
            or value.get("schema") != "iii.runtime-api-client-verifiers/v1"
        ):
            raise RuntimeError("Runtime API machine verifier store is malformed")
        expected_id = hashlib.sha256(
            json.dumps(
                {key: item for key, item in value.items() if key != "verifier_id"},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        if value.get("verifier_id") != expected_id:
            raise RuntimeError("Runtime API machine verifier identity mismatch")
        if (
            not IDENTITY.fullmatch(str(value.get("access_id", "")))
            or not isinstance(value.get("generation"), int)
            or isinstance(value.get("generation"), bool)
            or value["generation"] < 0
        ):
            raise RuntimeError("Runtime API machine verifier metadata is malformed")
        clients = value.get("clients")
        if not isinstance(clients, list) or not clients:
            raise RuntimeError("Runtime API has no active machine credential")
        identities: set[str] = set()
        verifiers: set[str] = set()
        for item in clients:
            if not isinstance(item, dict) or set(item) != {
                "machine_id",
                "label",
                "token_sha256",
            }:
                raise RuntimeError("Runtime API machine verifier entry is malformed")
            if not IDENTITY.fullmatch(
                str(item["machine_id"])
            ) or not IDENTITY.fullmatch(str(item["token_sha256"])):
                raise RuntimeError("Runtime API machine verifier entry is invalid")
            if not isinstance(item["label"], str) or not item["label"]:
                raise RuntimeError("Runtime API machine label is invalid")
            if item["machine_id"] in identities or item["token_sha256"] in verifiers:
                raise RuntimeError("Runtime API machine verifier entry is duplicated")
            identities.add(item["machine_id"])
            verifiers.add(item["token_sha256"])
        self._verify_access_state(value)
        return value

    def _verify_access_state(self, verifiers: dict) -> None:
        path = self.path.with_name("access-state.json")
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("Runtime API access state is unavailable")
        metadata = path.stat(follow_symlinks=False)
        if metadata.st_mode & 0o022:
            raise RuntimeError("Runtime API access state is writable")
        if self.require_root_owner and metadata.st_uid != 0:
            raise RuntimeError("Runtime API access state is not root-owned")
        try:
            raw = path.read_bytes()
            state = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read Runtime API access state: {exc}") from exc
        canonical = json.dumps(
            state, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        if not isinstance(state, dict) or raw != canonical + b"\n":
            raise RuntimeError("Runtime API access state is not canonical JSON")
        if (
            set(state) != {"schema", "access_id", "generation", "clients"}
            or state.get("schema") != "iii.receiver-access-state/v2"
            or not isinstance(state.get("clients"), dict)
            or not isinstance(state.get("generation"), int)
            or isinstance(state.get("generation"), bool)
            or state["generation"] < 0
        ):
            raise RuntimeError("Runtime API access state is malformed")
        expected_id = hashlib.sha256(
            json.dumps(
                {key: item for key, item in state.items() if key != "access_id"},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        if state.get("access_id") != expected_id:
            raise RuntimeError("Runtime API access-state identity mismatch")
        if (
            state["access_id"] != verifiers["access_id"]
            or state["generation"] != verifiers["generation"]
        ):
            raise RuntimeError("Runtime API verifier projection is stale")


__all__ = ["RuntimeCliCredentialVerifier"]
