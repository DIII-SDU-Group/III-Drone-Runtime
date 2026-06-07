from pathlib import Path
import subprocess


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def test_runtime_api_systemd_unit_is_separate_and_non_required():
    unit = (WORKSPACE_ROOT / "tools/systemd/iii-runtime-api.service").read_text(encoding="utf-8")
    main = (WORKSPACE_ROOT / "src/III-Drone-Runtime/iii_drone_runtime/api/main.py").read_text(encoding="utf-8")

    assert "Description=III runtime API" in unit
    assert "Wants=iii-system-daemon.service" in unit
    assert "Requires=iii-system-daemon.service" not in unit
    assert "Environment=III_RUNTIME_API_PROFILE=sim" in unit
    assert "Environment=III_RUNTIME_API_MDNS_ENABLED=1" in unit
    assert 'Environment="III_RUNTIME_API_MDNS_INSTANCE=III Runtime API Devcontainer"' in unit
    assert "EnvironmentFile=-/home/iii/ws/.config/iii-runtime-api.env" in unit
    assert "ExecStart=" in unit
    assert "iii_drone_runtime.api.main" in unit
    assert 'if __name__ == "__main__"' in main


def test_runtime_api_install_script_dry_run():
    result = subprocess.run(
        [str(WORKSPACE_ROOT / "scripts/systemd/install_runtime_api_service.sh"), "--dry-run"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Would install" in result.stdout
    assert "iii-runtime-api.service" in result.stdout


def test_devcontainer_post_start_installs_runtime_api_service():
    post_start = (WORKSPACE_ROOT / ".devcontainer/post_start.sh").read_text(encoding="utf-8")

    assert "pip3 install -r ./requirements.txt" in post_start
    assert "./scripts/systemd/install_runtime_api_service.sh" in post_start
