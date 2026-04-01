import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import app, get_connection, init_storage, now_iso  # noqa: E402

LOG_DIR = PROJECT_ROOT / "logs"
JOB_LOG_PATH = LOG_DIR / "jobs.log"


def configure_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(JOB_LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Esegue i job schedulati giornalieri.")
    parser.add_argument(
        "--job",
        choices=["daily_invoice_summary", "daily_payment_summary", "all"],
        default="all",
        help="Seleziona un job specifico oppure tutti.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forza l'esecuzione anche se il job risulta gia' eseguito oggi.",
    )
    return parser.parse_args()


def ensure_job_row(conn, job_code: str):
    conn.execute(
        """
        INSERT OR IGNORE INTO scheduled_jobs (job_code, last_run_at, last_status, last_message, updated_at)
        VALUES (?, '', '', '', ?)
        """,
        (job_code, now_iso()),
    )


def get_job_state(conn, job_code: str):
    ensure_job_row(conn, job_code)
    return conn.execute(
        """
        SELECT job_code, last_run_at, last_status, last_message, updated_at
        FROM scheduled_jobs
        WHERE job_code = ?
        """,
        (job_code,),
    ).fetchone()


def set_job_state(conn, job_code: str, status: str, message: str):
    conn.execute(
        """
        UPDATE scheduled_jobs
        SET last_run_at = ?, last_status = ?, last_message = ?, updated_at = ?
        WHERE job_code = ?
        """,
        (now_iso(), status, message, now_iso(), job_code),
    )


def already_ran_today(last_run_at: str) -> bool:
    raw = (last_run_at or "").strip()
    if not raw:
        return False
    try:
        return datetime.fromisoformat(raw).date() == date.today()
    except ValueError:
        return False


def run_daily_invoice_summary(conn):
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM invoices
        WHERE sent = 1
        """
    ).fetchone()
    total = int(row["total"]) if row else 0
    return (
        "SUCCESS",
        f"Job infrastrutturale eseguito. Fatture con flag invio attivo attualmente rilevate: {total}.",
    )


def run_daily_payment_summary(conn):
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM invoices
        WHERE trim(coalesce(receipt_date, '')) <> ''
        """
    ).fetchone()
    total = int(row["total"]) if row else 0
    return (
        "SUCCESS",
        f"Job infrastrutturale eseguito. Fatture con data incasso valorizzata attualmente rilevate: {total}.",
    )


JOB_RUNNERS = {
    "daily_invoice_summary": run_daily_invoice_summary,
    "daily_payment_summary": run_daily_payment_summary,
}


def run_job(conn, job_code: str, force: bool):
    state = get_job_state(conn, job_code)
    if not force and already_ran_today(state["last_run_at"]):
        message = f"Job {job_code} gia' eseguito oggi; uso --force per rieseguirlo."
        logging.info(message)
        set_job_state(conn, job_code, "SKIPPED", message)
        return

    runner = JOB_RUNNERS[job_code]
    status, message = runner(conn)
    logging.info("%s: %s", job_code, message)
    set_job_state(conn, job_code, status, message)


def main():
    args = parse_args()
    configure_logging()
    init_storage()

    selected_jobs = list(JOB_RUNNERS.keys()) if args.job == "all" else [args.job]
    logging.info("Avvio job giornalieri: %s", ", ".join(selected_jobs))

    with get_connection() as conn:
        for job_code in selected_jobs:
            run_job(conn, job_code, args.force)
        conn.commit()

    app.logger.info("Job giornalieri completati")


if __name__ == "__main__":
    main()
