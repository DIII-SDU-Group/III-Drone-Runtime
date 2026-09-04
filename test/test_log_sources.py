from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.logs import LogSource, LogSourceProvider


def test_fake_log_sources_list_tail_download_and_all_view(tmp_path):
    daemon_log = tmp_path / "daemon.log"
    runtime_log = tmp_path / "runtime.log"
    daemon_log.write_text("d1\nd2\nd3\n", encoding="utf-8")
    runtime_log.write_text("r1\nr2\n", encoding="utf-8")
    provider = LogSourceProvider(
        [
            LogSource("daemon", "Daemon", "file", daemon_log),
            LogSource("runtime_api", "Runtime API", "file", runtime_log),
        ]
    )

    assert [source.source_id for source in provider.list_sources()] == ["daemon", "runtime_api", "all"]
    assert provider.tail("daemon", lines=2) == [
        {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "d2"},
        {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "d3"},
    ]
    assert provider.tail("all", lines=3) == [
        {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "d3"},
        {"source_id": "runtime_api", "source_label": "Runtime API", "kind": "file", "line": "r1"},
        {"source_id": "runtime_api", "source_label": "Runtime API", "kind": "file", "line": "r2"},
    ]
    assert "[runtime_api] r2" in provider.download("all")


def test_runtime_api_log_rest_tail_and_download(tmp_path):
    daemon_log = tmp_path / "daemon.log"
    daemon_log.write_text("line1\nline2\n", encoding="utf-8")
    provider = LogSourceProvider([LogSource("daemon", "Daemon", "file", daemon_log)])
    client = TestClient(
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
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    sources = client.get("/logs/sources", headers=headers).json()["sources"]
    tail = client.get("/logs/daemon/tail?lines=1", headers=headers).json()["lines"]
    download = client.get("/logs/daemon/download", headers=headers).json()["content"]

    assert {source["source_id"] for source in sources} == {"daemon", "all"}
    assert tail == [{"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "line2"}]
    assert "[daemon] line1" in download


def test_runtime_api_cli_log_tail_uses_cli_token(tmp_path):
    log_file = tmp_path / "daemon.log"
    log_file.write_text("line1\nline2\n", encoding="utf-8")
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            log_provider=LogSourceProvider([LogSource("daemon", "Daemon", "file", log_file)]),
        )
    )

    assert client.get("/cli/logs/daemon/tail?lines=1").status_code == 401
    response = client.get("/cli/logs/daemon/tail?lines=1", headers={"X-III-CLI-Token": "cli-secret"})

    assert response.status_code == 200
    assert response.json()["lines"] == [
        {"source_id": "daemon", "source_label": "Daemon", "kind": "file", "line": "line2"}
    ]


def test_entity_directory_tail_prefers_current_run_log(tmp_path):
    stale = tmp_path / "older.log"
    current = tmp_path / "current.log"
    stale.write_text("stale\n", encoding="utf-8")
    current.write_text("one\ntwo\n", encoding="utf-8")

    rows = LogSourceProvider([]).tail_directory(
        "configuration_server", tmp_path, lines=1
    )

    assert rows == [
        {
            "source_id": "configuration_server",
            "source_label": "configuration_server",
            "kind": "entity",
            "line": "two",
        }
    ]
