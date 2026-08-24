import json
import socketserver
import threading
from types import SimpleNamespace

from iii_drone_runtime.daemon.client import DaemonClient


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        request = json.loads(self.rfile.readline().decode("utf-8"))
        response = {"ok": True, "result": {"echo": request}}
        self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))


def test_daemon_client_request_round_trip(tmp_path, monkeypatch):
    socket_path = tmp_path / "daemon.sock"
    monkeypatch.setenv("III_SYSTEM_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("III_SYSTEM_DAEMON_SOCKET", str(socket_path))
    monkeypatch.delenv("III_SYSTEM_DAEMON_REQUEST_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("III_SYSTEM_DAEMON_CLIENT_TIMEOUT_SEC", raising=False)

    server = socketserver.ThreadingUnixStreamServer(str(socket_path), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        client = DaemonClient()
        result = client._request({"command": "status"})
        assert result["echo"] == {"command": "status", "daemon_timeout_sec": client.request_timeout_sec}
        assert client.ping()
        assert client.service_start("micro_ros_agent")["echo"] == {
            "command": "service_start",
            "service_id": "micro_ros_agent",
            "daemon_timeout_sec": client.request_timeout_sec,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


def test_ensure_running_starts_systemd_daemon_service(tmp_path, monkeypatch):
    socket_path = tmp_path / "daemon.sock"
    log_path = tmp_path / "daemon.log"
    monkeypatch.setenv("III_SYSTEM_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("III_SYSTEM_DAEMON_SOCKET", str(socket_path))
    monkeypatch.setenv("III_SYSTEM_DAEMON_LOG", str(log_path))
    monkeypatch.setenv("III_SYSTEMD_DAEMON_SERVICE", "test-iii-daemon.service")

    client = DaemonClient()
    calls = []
    ping_results = iter([False, True])

    monkeypatch.setattr(client, "ping", lambda: next(ping_results))
    monkeypatch.setattr(client, "_assert_systemd_available", lambda: None)

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        socket_path.touch()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("iii_drone_runtime.daemon.client.subprocess.run", fake_run)

    client.ensure_running(timeout_seconds=0.2)

    assert calls
    assert calls[0][0][-3:] == ["systemctl", "start", "test-iii-daemon.service"]
