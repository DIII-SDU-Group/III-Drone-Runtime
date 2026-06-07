from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_runtime_sources_do_not_import_core_math():
    offenders = []
    for source in (PACKAGE_ROOT / "iii_drone_runtime").rglob("*.py"):
        text = source.read_text(encoding="utf-8")
        if "iii_drone_core" in text:
            offenders.append(str(source.relative_to(PACKAGE_ROOT)))

    assert offenders == []


def test_runtime_sources_do_not_import_mcp_reference_package():
    offenders = []
    for source in (PACKAGE_ROOT / "iii_drone_runtime").rglob("*.py"):
        text = source.read_text(encoding="utf-8")
        if "iii_drone_mcp" in text:
            offenders.append(str(source.relative_to(PACKAGE_ROOT)))

    assert offenders == []
