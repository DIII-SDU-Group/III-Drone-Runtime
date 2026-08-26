from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.simulation import SimulationRuntimeController, _parse_status_output


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


def _client(profile: str, tools: _FakeSimulationTools) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                profile=profile,
                browser_password="secret",
                cli_token="cli-secret",
            ),
            simulation_controller=SimulationRuntimeController(profile=profile, adapter=tools),
            # This suite isolates the simulation-profile gate. Receiver clock
            # rejection has its own contract tests and otherwise intercepts every
            # real-profile mutation before the simulation controller is reached.
            clock_gate_provider=lambda: None,
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
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


def test_simulation_controls_are_disabled_in_real_profile_without_calling_tools():
    tools = _FakeSimulationTools()
    client = _client("real", tools)
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
