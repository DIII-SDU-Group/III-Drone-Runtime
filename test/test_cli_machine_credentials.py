from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.cli_credentials import RuntimeCliCredentialVerifier


def _canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _write(path: Path, clients: list[tuple[str, str, str]]) -> None:
    access_state = {
        "schema": "iii.receiver-access-state/v2",
        "access_id": "0" * 64,
        "generation": 1,
        "clients": {machine_id: {"state": "active"} for machine_id, _, _ in clients},
    }
    access_state["access_id"] = hashlib.sha256(
        _canonical(
            {key: item for key, item in access_state.items() if key != "access_id"}
        )
    ).hexdigest()
    access_path = path.with_name("access-state.json")
    access_path.write_bytes(_canonical(access_state) + b"\n")
    access_path.chmod(0o640)
    value = {
        "schema": "iii.runtime-api-client-verifiers/v1",
        "verifier_id": "0" * 64,
        "access_id": access_state["access_id"],
        "generation": 1,
        "clients": [
            {
                "machine_id": machine_id,
                "label": label,
                "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
            }
            for machine_id, label, token in clients
        ],
    }
    value["verifier_id"] = hashlib.sha256(
        _canonical({key: item for key, item in value.items() if key != "verifier_id"})
    ).hexdigest()
    path.write_bytes(_canonical(value) + b"\n")
    path.chmod(0o640)


def test_independent_machine_tokens_authorize_and_revoke_without_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime-verifiers.json"
    token_a = "A" * 43
    token_b = "B" * 43
    machine_a = "1" * 64
    machine_b = "2" * 64
    _write(path, [(machine_a, "provisioning", token_a), (machine_b, "gc", token_b)])
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="runtime-test",
                profile="sim",
                system_id="system-test",
                browser_password="browser-secret",
                cli_credentials_path=str(path),
            )
        )
    )
    assert (
        client.get("/cli/readiness", headers={"X-III-CLI-Token": token_a}).status_code
        == 200
    )
    assert (
        client.get("/cli/readiness", headers={"X-III-CLI-Token": token_b}).status_code
        == 200
    )

    _write(path, [(machine_b, "gc", token_b)])
    assert (
        client.get("/cli/readiness", headers={"X-III-CLI-Token": token_a}).status_code
        == 401
    )
    assert (
        client.get("/cli/readiness", headers={"X-III-CLI-Token": token_b}).status_code
        == 200
    )


def test_machine_verifier_store_tampering_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "runtime-verifiers.json"
    token = "C" * 43
    _write(path, [("3" * 64, "gc", token)])
    verifier = RuntimeCliCredentialVerifier(path, require_root_owner=False)
    assert verifier.authenticate(token) == "3" * 64
    path.write_bytes(path.read_bytes().replace(b'"generation":1', b'"generation":2'))
    try:
        verifier.authenticate(token)
    except RuntimeError as exc:
        assert "identity mismatch" in str(exc)
    else:
        raise AssertionError("tampered Runtime verifier store was accepted")


def test_stale_machine_verifier_projection_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "runtime-verifiers.json"
    token = "D" * 43
    _write(path, [("4" * 64, "gc", token)])
    verifier = RuntimeCliCredentialVerifier(path, require_root_owner=False)
    assert verifier.authenticate(token) == "4" * 64
    access_path = tmp_path / "access-state.json"
    state = json.loads(access_path.read_text())
    state["generation"] = 2
    state["access_id"] = hashlib.sha256(
        _canonical({key: item for key, item in state.items() if key != "access_id"})
    ).hexdigest()
    access_path.write_bytes(_canonical(state) + b"\n")

    try:
        verifier.authenticate(token)
    except RuntimeError as exc:
        assert "projection is stale" in str(exc)
    else:
        raise AssertionError("stale Runtime verifier projection was accepted")


def test_real_app_refuses_missing_machine_verifier_store_at_startup(
    tmp_path: Path,
) -> None:
    settings = RuntimeApiSettings(
        runtime_id="iii-aircraft-runtime",
        runtime_name="III Aircraft Runtime",
        profile="real",
        system_id="iii-aircraft",
        browser_password="field-browser-secret",
        cli_credentials_path=str(tmp_path / "missing-verifiers.json"),
        release_id="a" * 64,
    )

    try:
        create_app(settings=settings)
    except RuntimeError as exc:
        assert "verifier store is unavailable" in str(exc)
    else:
        raise AssertionError("real Runtime API accepted a missing verifier store")


def test_real_app_refuses_manually_constructed_development_identity() -> None:
    settings = RuntimeApiSettings(
        runtime_id="iii-runtime",
        runtime_name="Unsafe Runtime",
        profile="real",
        system_id="iii-drone",
        browser_password="dev-password",
        cli_credentials_path="/not-used-before-settings-validation",
        release_id="a" * 64,
    )

    try:
        create_app(settings=settings)
    except RuntimeError as exc:
        assert "III_RUNTIME_API_BROWSER_PASSWORD" in str(exc)
        assert "III_RUNTIME_API_ID" in str(exc)
        assert "III_RUNTIME_API_SYSTEM_ID" in str(exc)
    else:
        raise AssertionError("real Runtime API accepted manually injected dev settings")


@pytest.mark.skipif(
    os.geteuid() != 0, reason="root ownership is a production invariant"
)
def test_real_app_accepts_root_owned_receiver_credential_projection(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime-verifiers.json"
    _write(path, [("5" * 64, "commissioned-gc", "E" * 43)])
    settings = RuntimeApiSettings(
        runtime_id="iii-aircraft-runtime",
        runtime_name="III Aircraft Runtime",
        profile="real",
        system_id="iii-aircraft",
        browser_password="field-browser-secret",
        cli_credentials_path=str(path),
        release_id="a" * 64,
    )

    create_app(settings=settings)
