import pytest
from fastapi.testclient import TestClient
from iii_drone_contracts import API_VERSION

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mdns import (
    RuntimeApiAdvertiser,
    runtime_api_advertisement_properties,
)


def test_runtime_api_settings_reads_complete_environment(monkeypatch):
    monkeypatch.setenv("III_RUNTIME_API_ID", "runtime-1")
    monkeypatch.setenv("III_RUNTIME_API_NAME", "Runtime One")
    monkeypatch.setenv("III_RUNTIME_API_PROFILE", "sim")
    monkeypatch.setenv("III_RUNTIME_API_HOST", "127.0.0.1")
    monkeypatch.setenv("III_RUNTIME_API_PORT", "9876")
    monkeypatch.setenv("III_RUNTIME_API_MDNS_ENABLED", "1")
    monkeypatch.setenv("III_RUNTIME_API_MDNS_INSTANCE", "Runtime One API")
    monkeypatch.setenv("III_RUNTIME_API_MDNS_HOST", "runtime-one.local")
    monkeypatch.setenv("III_RUNTIME_API_SYSTEM_ID", "drone-1")
    monkeypatch.setenv("III_RUNTIME_API_BROWSER_PASSWORD", "browser-secret")
    monkeypatch.setenv("III_RUNTIME_API_CLI_TOKEN", "cli-secret")
    monkeypatch.setenv("III_RUNTIME_API_HEARTBEAT_INTERVAL_SEC", "3")
    monkeypatch.setenv("III_RUNTIME_API_SESSION_LEASE_TIMEOUT_SEC", "11")
    monkeypatch.setenv("III_RUNTIME_API_PX4_MAVLINK_ENDPOINT", "udp://:14550")
    monkeypatch.setenv("III_RUNTIME_API_PX4_ENABLED", "0")
    monkeypatch.setenv("III_RUNTIME_API_LOG_DIR", "/var/log/iii-runtime-api")

    settings = RuntimeApiSettings.from_env()

    assert settings.runtime_id == "runtime-1"
    assert settings.runtime_name == "Runtime One"
    assert settings.profile == "sim"
    assert settings.host == "127.0.0.1"
    assert settings.port == 9876
    assert settings.mdns_enabled is True
    assert settings.mdns_instance_name == "Runtime One API"
    assert settings.mdns_advertise_host == "runtime-one.local"
    assert settings.system_id == "drone-1"
    assert settings.browser_password == "browser-secret"
    assert settings.cli_token == "cli-secret"
    assert settings.heartbeat_interval_seconds == 3
    assert settings.lease_timeout_seconds == 11
    assert settings.px4_mavlink_endpoint == "udp://:14550"
    assert settings.px4_command_transport_enabled is False
    assert settings.log_dir == "/var/log/iii-runtime-api"


def test_real_profile_requires_runtime_api_secrets(monkeypatch):
    monkeypatch.setenv("III_RUNTIME_API_PROFILE", "real")
    monkeypatch.delenv("III_RUNTIME_API_BROWSER_PASSWORD", raising=False)
    monkeypatch.delenv("III_RUNTIME_API_CLI_TOKEN", raising=False)
    monkeypatch.delenv("III_RUNTIME_API_CREDENTIALS_PATH", raising=False)

    with pytest.raises(RuntimeError, match="III_RUNTIME_API_BROWSER_PASSWORD"):
        RuntimeApiSettings.from_env()


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("III_RUNTIME_API_BROWSER_PASSWORD", "dev-password"),
        ("III_RUNTIME_API_BROWSER_PASSWORD", "too-short"),
        ("III_RUNTIME_API_CLI_TOKEN", "any-shared-token-is-forbidden"),
        ("III_RUNTIME_API_ID", "iii-runtime"),
        ("III_RUNTIME_API_SYSTEM_ID", "iii-drone"),
        ("III_RELEASE_ID", "not-a-release-id"),
    ],
)
def test_real_profile_rejects_development_credentials_and_default_identity(
    monkeypatch, variable, value
):
    monkeypatch.setenv("III_RUNTIME_API_PROFILE", "real")
    monkeypatch.setenv("III_RUNTIME_API_BROWSER_PASSWORD", "field-browser-secret")
    monkeypatch.delenv("III_RUNTIME_API_CLI_TOKEN", raising=False)
    monkeypatch.setenv(
        "III_RUNTIME_API_CREDENTIALS_PATH",
        "/var/lib/iii/deployment/runtime-api-client-verifiers.json",
    )
    monkeypatch.setenv("III_RUNTIME_API_ID", "iii-aircraft-runtime")
    monkeypatch.setenv("III_RUNTIME_API_SYSTEM_ID", "iii-aircraft")
    monkeypatch.setenv("III_RELEASE_ID", "a" * 64)
    monkeypatch.setenv(variable, value)

    with pytest.raises(RuntimeError, match=variable):
        RuntimeApiSettings.from_env()


def test_real_profile_accepts_unique_identity_and_non_development_credentials(
    monkeypatch,
):
    monkeypatch.setenv("III_RUNTIME_API_PROFILE", "real")
    monkeypatch.setenv("III_RUNTIME_API_BROWSER_PASSWORD", "field-browser-secret")
    monkeypatch.delenv("III_RUNTIME_API_CLI_TOKEN", raising=False)
    monkeypatch.setenv(
        "III_RUNTIME_API_CREDENTIALS_PATH",
        "/var/lib/iii/deployment/runtime-api-client-verifiers.json",
    )
    monkeypatch.setenv("III_RUNTIME_API_ID", "iii-aircraft-runtime")
    monkeypatch.setenv("III_RUNTIME_API_SYSTEM_ID", "iii-aircraft")
    monkeypatch.setenv("III_RELEASE_ID", "a" * 64)

    settings = RuntimeApiSettings.from_env()

    assert settings.profile == "real"
    assert settings.runtime_id == "iii-aircraft-runtime"
    assert settings.system_id == "iii-aircraft"
    assert settings.release_id == "a" * 64
    assert settings.cli_credentials_path.endswith("runtime-api-client-verifiers.json")


def test_identity_exposes_configured_system_id():
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="runtime-1",
            runtime_name="Runtime One",
            profile="sim",
            system_id="drone-1",
            browser_password="secret",
            cli_token="cli-secret",
        )
    )

    response = TestClient(app).get("/identity")

    assert response.json()["host_label"] == "drone-1"


class _FakeAdvertiser:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


def test_mdns_advertisement_starts_and_stops_with_app_lifecycle():
    advertiser = _FakeAdvertiser()
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="runtime-1",
            runtime_name="Runtime One",
            profile="sim",
            system_id="drone-1",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        mdns_advertiser=advertiser,
    )

    with TestClient(app):
        assert advertiser.started is True
        assert advertiser.stopped is False

    assert advertiser.stopped is True


class _FakeServiceInfo:
    def __init__(self, service_type, name, *, addresses, port, properties, server):
        self.service_type = service_type
        self.name = name
        self.addresses = addresses
        self.port = port
        self.properties = properties
        self.server = server


class _FakeZeroconf:
    def __init__(self):
        self.registered = None
        self.unregistered = None
        self.closed = False

    def register_service(self, service_info) -> None:
        self.registered = service_info

    def unregister_service(self, service_info) -> None:
        self.unregistered = service_info

    def close(self) -> None:
        self.closed = True


def test_runtime_api_advertiser_registers_expected_mdns_metadata():
    zeroconf = _FakeZeroconf()
    advertiser = RuntimeApiAdvertiser(
        runtime_id="runtime-1",
        runtime_name="Runtime One",
        profile="sim",
        bind_host="0.0.0.0",
        port=8765,
        instance_name="Runtime One API",
        system_id="drone-1",
        zeroconf_factory=lambda: zeroconf,
        service_info_factory=_FakeServiceInfo,
        host_resolver=lambda bind_host, advertise_host: "192.168.1.10",
        server_name_resolver=lambda: "runtime-one.local.",
    )

    advertiser.start()

    assert zeroconf.registered is not None
    assert zeroconf.registered.service_type == "_iii-runtime-api._tcp.local."
    assert zeroconf.registered.name == "Runtime One API._iii-runtime-api._tcp.local."
    assert zeroconf.registered.port == 8765
    assert zeroconf.registered.server == "runtime-one.local."
    assert zeroconf.registered.properties == {
        "runtime_id": "runtime-1",
        "runtime_name": "Runtime One",
        "name": "Runtime One",
        "system_id": "drone-1",
        "host": "192.168.1.10",
        "port": "8765",
        "api_version": API_VERSION,
        "scheme": "http",
        "profile": "sim",
    }

    advertiser.stop()

    assert zeroconf.unregistered is zeroconf.registered
    assert zeroconf.closed is True


def test_identity_matches_advertised_metadata_and_does_not_expose_operational_state():
    settings = RuntimeApiSettings(
        runtime_id="runtime-1",
        runtime_name="Runtime One",
        profile="sim",
        system_id="drone-1",
        host="127.0.0.1",
        port=8765,
        browser_password="secret",
        cli_token="cli-secret",
    )
    app = create_app(settings=settings)
    identity_response = TestClient(app).get("/identity")
    properties = runtime_api_advertisement_properties(
        runtime_id=settings.runtime_id,
        runtime_name=settings.runtime_name,
        profile=settings.profile,
        system_id=settings.system_id,
        host=settings.host,
        port=settings.port,
    )

    identity = identity_response.json()
    assert identity["runtime_id"] == properties["runtime_id"]
    assert identity["runtime_name"] == properties["runtime_name"]
    assert identity["profile"] == properties["profile"]
    assert identity["host_label"] == properties["system_id"]
    assert identity["compatibility"]["api_version"] == properties["api_version"]
    assert "system" not in identity
    assert "vehicle" not in identity
    assert "browser_password" not in identity
    assert TestClient(app).get("/runtime/status").status_code == 401
