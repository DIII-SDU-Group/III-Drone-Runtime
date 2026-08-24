import threading
import time

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.logs import LogSource, LogSourceProvider


def _client(provider: LogSourceProvider) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            log_provider=provider,
        )
    )


def _token(client: TestClient) -> str:
    return client.post("/session/login", json={"password": "secret"}).json()["session_token"]


def test_websocket_log_follow_emits_source_metadata(tmp_path):
    log_file = tmp_path / "daemon.log"
    log_file.write_text("one\ntwo\n", encoding="utf-8")
    client = _client(LogSourceProvider([LogSource("daemon", "Daemon", "file", log_file)]))
    token = _token(client)

    with client.websocket_connect(f"/logs/follow/daemon?token={token}") as websocket:
        first = websocket.receive_json()
        second = websocket.receive_json()

    assert first == {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "one"}
    assert second == {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "two"}


def test_websocket_all_logs_follow_includes_source_labels(tmp_path):
    daemon_log = tmp_path / "daemon.log"
    runtime_log = tmp_path / "runtime.log"
    daemon_log.write_text("d\n", encoding="utf-8")
    runtime_log.write_text("r\n", encoding="utf-8")
    client = _client(
        LogSourceProvider(
            [
                LogSource("daemon", "Daemon", "file", daemon_log),
                LogSource("runtime_api", "Runtime API", "file", runtime_log),
            ]
        )
    )
    token = _token(client)

    with client.websocket_connect(f"/logs/follow/all?token={token}") as websocket:
        first = websocket.receive_json()
        second = websocket.receive_json()

    assert first["source_id"] == "daemon"
    assert first["source_label"] == "Daemon"
    assert second["source_id"] == "runtime_api"
    assert second["source_label"] == "Runtime API"


def test_websocket_log_follow_streams_appended_lines(tmp_path):
    log_file = tmp_path / "daemon.log"
    log_file.write_text("initial\n", encoding="utf-8")
    client = _client(LogSourceProvider([LogSource("daemon", "Daemon", "file", log_file)]))
    token = _token(client)

    def append_line() -> None:
        time.sleep(0.2)
        with log_file.open("a", encoding="utf-8") as stream:
            stream.write("next\n")

    with client.websocket_connect(f"/logs/follow/daemon?token={token}") as websocket:
        assert websocket.receive_json()["line"] == "initial"
        writer = threading.Thread(target=append_line)
        writer.start()
        assert websocket.receive_json()["line"] == "next"
        writer.join(timeout=1)


def test_websocket_log_follow_rejects_invalid_token(tmp_path):
    log_file = tmp_path / "daemon.log"
    log_file.write_text("one\n", encoding="utf-8")
    client = _client(LogSourceProvider([LogSource("daemon", "Daemon", "file", log_file)]))

    try:
        with client.websocket_connect("/logs/follow/daemon?token=wrong"):
            raise AssertionError("expected websocket rejection")
    except WebSocketDisconnect:
        pass
