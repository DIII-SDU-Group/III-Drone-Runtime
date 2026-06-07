import sys
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_SRC = PACKAGE_ROOT.parent

for candidate in [PACKAGE_ROOT, WORKSPACE_SRC / "III-Drone-Contracts"]:
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))
