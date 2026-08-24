"""Console entrypoint for iii-runtime-api."""

from __future__ import annotations

import uvicorn

from .app import RuntimeApiSettings, create_app


def main() -> int:
    settings = RuntimeApiSettings.from_env()
    uvicorn.run(create_app(settings=settings), host=settings.host, port=settings.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
