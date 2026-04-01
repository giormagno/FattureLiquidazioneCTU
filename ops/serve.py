import os
import sys
from pathlib import Path

from waitress import serve

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app import app


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def main():
    host = os.getenv("WAITRESS_HOST", os.getenv("APP_HOST", "127.0.0.1")).strip() or "127.0.0.1"
    port = env_int("WAITRESS_PORT", env_int("APP_PORT", 8000))
    threads = env_int("WAITRESS_THREADS", 4)
    connection_limit = env_int("WAITRESS_CONNECTION_LIMIT", 100)

    app.logger.info(
        "Avvio Waitress host=%s port=%s threads=%s connection_limit=%s",
        host,
        port,
        threads,
        connection_limit,
    )
    serve(
        app,
        host=host,
        port=port,
        threads=threads,
        connection_limit=connection_limit,
    )


if __name__ == "__main__":
    main()
