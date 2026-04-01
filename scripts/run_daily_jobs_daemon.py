import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNNER = PROJECT_ROOT / "scripts" / "run_daily_jobs.py"


def load_local_env():
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def parse_run_at(value: str) -> tuple[int, int]:
    raw = (value or "").strip()
    try:
        hour_raw, minute_raw = raw.split(":", 1)
        hour = int(hour_raw)
        minute = int(minute_raw)
    except (ValueError, AttributeError):
        raise ValueError("JOB_DAEMON_RUN_AT deve essere nel formato HH:MM") from None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("JOB_DAEMON_RUN_AT non valido")
    return hour, minute


def main():
    load_local_env()
    configure_logging()

    timezone_name = os.getenv("JOB_TIMEZONE", "Europe/Rome").strip() or "Europe/Rome"
    run_at = os.getenv("JOB_DAEMON_RUN_AT", "20:00").strip() or "20:00"
    poll_seconds = max(int(os.getenv("JOB_DAEMON_POLL_SECONDS", "60")), 15)
    target_hour, target_minute = parse_run_at(run_at)
    timezone = ZoneInfo(timezone_name)
    last_attempt_date = None

    logging.info(
        "Scheduler job avviato timezone=%s run_at=%s poll_seconds=%s",
        timezone_name,
        run_at,
        poll_seconds,
    )

    while True:
        now = datetime.now(timezone)
        current_date = now.date()
        current_tuple = (now.hour, now.minute)
        target_tuple = (target_hour, target_minute)

        if current_tuple >= target_tuple and last_attempt_date != current_date:
            logging.info("Finestra giornaliera raggiunta, eseguo run_daily_jobs.py")
            result = subprocess.run(
                [sys.executable, str(RUNNER)],
                cwd=PROJECT_ROOT,
                check=False,
            )
            last_attempt_date = current_date
            if result.returncode == 0:
                logging.info("Job giornalieri completati con successo")
            else:
                logging.error("Job giornalieri terminati con codice %s", result.returncode)

        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
