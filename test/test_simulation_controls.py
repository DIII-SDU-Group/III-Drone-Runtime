import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.cli_credentials import RuntimeCliCredentialVerifier
from iii_drone_runtime.api.simulation import (
    SimulationRuntimeController,
    _parse_status_output,
)


BROWSER_PASSWORD = "test-browser-secret"


class _FakeSimulationTools:
    def __init__(self):
        self.calls = []

    def status(self):
        self.calls.append("status")
        return {
            "tmux_session": "running",
            "simulation_process_groups": ["100"],
            "px4_gazebo": "running",
            "gazebo_transport": "available",
            "qgroundcontrol": "not_controlled",
        }

    def start_backend(self):
        self.calls.append("start_backend")
        return {
            "tmux_session": "running",
            "simulation_process_groups": ["100"],
            "px4_gazebo": "running",
            "gazebo_transport": "available",
            "qgroundcontrol": "not_controlled",
        }

    def stop_backend(self):
        self.calls.append("stop_backend")
        return {
            "tmux_session": "stopped",
            "simulation_process_groups": [],
            "px4_gazebo": "stopped",
            "gazebo_transport": "unavailable",
            "qgroundcontrol": "not_controlled",
        }


def _write_real_credentials(root: Path) -> Path:
    canonical = lambda value: json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode()
    access = {
        "schema": "iii.receiver-access-state/v2",
        "access_id": "0" * 64,
        "generation": 1,
        "clients": {"1" * 64: {"state": "active"}},
    }
    access["access_id"] = hashlib.sha256(
        canonical({key: value for key, value in access.items() if key != "access_id"})
    ).hexdigest()
    (root / "access-state.json").write_bytes(canonical(access) + b"\n")
    verifier = {
        "schema": "iii.runtime-api-client-verifiers/v1",
        "verifier_id": "0" * 64,
        "access_id": access["access_id"],
        "generation": 1,
        "clients": [
            {
                "machine_id": "1" * 64,
                "label": "test-client",
                "token_sha256": hashlib.sha256(b"A" * 43).hexdigest(),
            }
        ],
    }
    verifier["verifier_id"] = hashlib.sha256(
        canonical(
            {key: value for key, value in verifier.items() if key != "verifier_id"}
        )
    ).hexdigest()
    path = root / "runtime-verifiers.json"
    path.write_bytes(canonical(verifier) + b"\n")
    path.chmod(0o640)
    return path


def _client(
    profile: str, tools: _FakeSimulationTools, credential_root: Path | None = None
) -> TestClient:
    real = profile in {"real", "opti_track"}
    credential_path = (
        _write_real_credentials(credential_root)
        if real and credential_root is not None
        else None
    )
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="iii-aircraft-runtime" if real else "test-runtime",
                runtime_name="Test Runtime",
                profile=profile,
                system_id="iii-aircraft" if real else "test-system",
                browser_password=BROWSER_PASSWORD,
                cli_token="cli-secret",
                cli_credentials_path=(
                    str(credential_path) if credential_path is not None else None
                ),
                release_id="a" * 64 if real else None,
            ),
            simulation_controller=SimulationRuntimeController(
                profile=profile, adapter=tools
            ),
            # This suite isolates the simulation-profile gate. Receiver clock
            # rejection has its own contract tests and otherwise intercepts every
            # real-profile mutation before the simulation controller is reached.
            clock_gate_provider=lambda: None,
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": BROWSER_PASSWORD}).json()[
        "session_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_simulation_status_is_exposed_in_simulation_domain():
    tools = _FakeSimulationTools()
    client = _client("sim", tools)

    response = client.get("/simulation/status", headers=_headers(client))

    assert response.status_code == 200
    payload = response.json()
    assert payload["result"]["enabled"] is True
    assert payload["simulation"]["profile"] == "sim"
    assert payload["simulation"]["px4_gazebo_status"] == "running"
    assert payload["simulation"]["latest"]["status"]["gazebo_transport"] == "available"


def test_simulation_start_and_stop_are_available_only_in_sim_profile():
    tools = _FakeSimulationTools()
    client = _client("sim", tools)
    headers = _headers(client)

    start = client.post("/simulation/backend/start", headers=headers).json()
    stop = client.post("/simulation/backend/stop", headers=headers).json()

    assert start["result"]["ok"] is True
    assert start["result"]["status"]["qgroundcontrol"] == "not_controlled"
    assert stop["simulation"]["px4_gazebo_status"] == "stopped"
    assert tools.calls == ["start_backend", "stop_backend"]


def test_simulation_controls_are_disabled_in_real_profile_without_calling_tools(
    tmp_path: Path,
    monkeypatch,
):
    # This test exercises profile gating, not the separately covered on-aircraft
    # ownership policy.  Keep it runnable by the normal devcontainer user while
    # still constructing a real-profile application.
    monkeypatch.setattr(RuntimeCliCredentialVerifier, "validate", lambda self: None)
    tools = _FakeSimulationTools()
    client = _client("real", tools, tmp_path)
    headers = _headers(client)

    status = client.get("/simulation/status", headers=headers).json()
    start = client.post("/simulation/backend/start", headers=headers).json()

    assert status["result"]["enabled"] is False
    assert status["result"]["disabled_reason"]
    assert start["result"]["ok"] is False
    assert start["result"]["status"]["qgroundcontrol"] == "not_controlled"
    assert tools.calls == []


def test_simulation_status_parser_maps_px4_gazebo_without_qgc_control():
    parsed = _parse_status_output(
        "\n".join(
            [
                "tmux_session: running",
                "simulation_process_groups: 111 222",
                "gazebo_transport: available",
                "qgroundcontrol: running",
                "px4_instance_state: lock_or_socket_present",
            ]
        )
    )

    assert parsed["px4_gazebo"] == "running"
    assert parsed["simulation_process_groups"] == ["111", "222"]
    assert parsed["qgroundcontrol"] == "running"
