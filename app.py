import base64
import calendar
import io
import json
import logging
import os
import random
import re
import sqlite3
import string
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from xml.sax.saxutils import escape

import pdfplumber
from flask import Flask, flash, g, jsonify, redirect, render_template, request, send_file, session, url_for
from lxml import etree
from playwright.sync_api import sync_playwright
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
XSL_PATH = BASE_DIR / "FoglioStileAssoSoftware.xsl"

NS = "http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2"
NS_MAP = {"ns2": NS}

MONTH_OPTIONS = [
    (1, "Gennaio"),
    (2, "Febbraio"),
    (3, "Marzo"),
    (4, "Aprile"),
    (5, "Maggio"),
    (6, "Giugno"),
    (7, "Luglio"),
    (8, "Agosto"),
    (9, "Settembre"),
    (10, "Ottobre"),
    (11, "Novembre"),
    (12, "Dicembre"),
]
ARCHIVE_MONTH_OPTIONS = [("", "Tutti i mesi")] + [(str(value), label) for value, label in MONTH_OPTIONS]

INVOICE_PROFILE_DEFAULTS = {
    "transmitter_country_code": "IT",
    "transmitter_id_code": "BTTSVN49H70A662N",
    "transmission_format": "FPA12",
    "recipient_code": "JK1HU4",
    "issuer_vat_country": "IT",
    "issuer_vat_code": "06928550729",
    "issuer_tax_code": "BTTSVN49H70A662N",
    "issuer_first_name": "SILVANA",
    "issuer_last_name": "BITETTO",
    "issuer_tax_regime": "RF01",
    "issuer_address": "VIA GOFFREDO MAMELI",
    "issuer_street_number": "25",
    "issuer_zip": "70126",
    "issuer_city": "BARI",
    "issuer_province": "BA",
    "issuer_country": "IT",
    "client_tax_code": "80018350720",
    "client_name": "Ministero della Giustizia - Tribunale (Giudice Unico di Primo Grado) di Bari",
    "client_address": "Piazza E. De Nicola",
    "client_street_number": "41",
    "client_zip": "70100",
    "client_city": "BARI",
    "client_province": "BA",
    "client_country": "IT",
    "payment_beneficiary": "Bitetto Silvana",
    "payment_terms_code": "TP02",
    "payment_method_code": "MP05",
    "payment_bank_name": "UNICREDIT",
    "payment_iban": "IT66Y0200804025000005106331",
    "document_type": "TD06",
    "currency": "EUR",
    "vat_rate": "22.00",
    "withholding_type": "RT01",
    "withholding_rate": "20.00",
    "withholding_payment_reason": "A",
    "vat_collection_method": "I",
    "default_payer": "INPS",
    "description_main_template": "DECRETO n.{numero_rg}-{anno_rg} R.G. - il pagamento avviene dalla parte individuata dal provv.to del Giudice ({pagante})",
    "description_expense_template": "Rimborsi spese decreto n.{numero_rg}-{anno_rg} R.G.",
    "attachment_description_template": "DECRETO n.{numero_rg}-{anno_rg} R.G.",
}
INVOICE_PROFILE_TEMPLATE_FIELDS = {"numero_rg", "anno_rg", "pagante"}
INVOICE_PROFILE_UPPER_FIELDS = {
    "transmitter_country_code",
    "transmitter_id_code",
    "transmission_format",
    "recipient_code",
    "issuer_vat_country",
    "issuer_vat_code",
    "issuer_tax_code",
    "issuer_tax_regime",
    "issuer_zip",
    "issuer_city",
    "issuer_province",
    "issuer_country",
    "client_tax_code",
    "client_zip",
    "client_city",
    "client_province",
    "client_country",
    "payment_terms_code",
    "payment_method_code",
    "payment_iban",
    "document_type",
    "currency",
    "withholding_type",
    "withholding_payment_reason",
    "vat_collection_method",
    "default_payer",
}
API_QUOTA_PERIOD_OPTIONS = [
    ("day", "Giornaliero rolling"),
    ("week", "Settimanale rolling"),
    ("month", "Mensile rolling"),
    ("year", "Annuale rolling"),
]
API_QUOTA_PERIOD_LABELS = dict(API_QUOTA_PERIOD_OPTIONS)
API_QUOTA_WINDOW_LABELS = {
    "day": "ultime 24 ore",
    "week": "ultimi 7 giorni",
    "month": "ultimi 30 giorni",
    "year": "ultimi 365 giorni",
}
API_QUOTA_WINDOW_DELTAS = {
    "day": timedelta(days=1),
    "week": timedelta(days=7),
    "month": timedelta(days=30),
    "year": timedelta(days=365),
}


# ---------------------------------------------------------------------------
# Environment and storage bootstrap
# ---------------------------------------------------------------------------

def load_local_env():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_local_env()

STORAGE_DIR = Path(os.getenv("APP_STORAGE_DIR", str(BASE_DIR / "storage"))).expanduser()
XML_OUTPUT_DIR = STORAGE_DIR / "xml"
PDF_OUTPUT_DIR = STORAGE_DIR / "pdf"
DECREE_OUTPUT_DIR = STORAGE_DIR / "decreti"
DB_PATH = STORAGE_DIR / "fatture.db"
LOG_DIR = Path(os.getenv("APP_LOG_DIR", str(BASE_DIR / "logs"))).expanduser()
LOG_PATH = LOG_DIR / "app.log"


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def env_csv(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


APP_ENV = (os.getenv("APP_ENV") or "development").strip().lower()
APP_DEBUG = env_bool("APP_DEBUG", APP_ENV != "production")
APP_HOST = os.getenv("APP_HOST", "127.0.0.1").strip() or "127.0.0.1"
APP_PORT = env_int("APP_PORT", 5000)
APP_BASE_URL = os.getenv("APP_BASE_URL", f"http://{APP_HOST}:{APP_PORT}").strip()
APP_SECRET_KEY = os.getenv("APP_SECRET_KEY", "dev-insecure-secret-key-change-me")
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
BOOTSTRAP_MASTER_USERNAME = os.getenv("APP_MASTER_USERNAME", "master").strip() or "master"
BOOTSTRAP_MASTER_EMAIL = os.getenv("APP_MASTER_EMAIL", "").strip()
BOOTSTRAP_MASTER_PASSWORD = os.getenv("APP_MASTER_PASSWORD", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY_OPENAI", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

app.config["SECRET_KEY"] = APP_SECRET_KEY
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = APP_BASE_URL.lower().startswith("https://")


@app.after_request
def add_no_cache_headers(response):
    if request.method == "GET" and response.mimetype in {"text/html", "application/json"}:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def migrate_invoices_schema(conn: sqlite3.Connection):
    columns = table_columns(conn, "invoices")
    if not columns:
        conn.execute(
            """
            CREATE TABLE invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                owner_user_id INTEGER NOT NULL DEFAULT 0,
                created_by_user_id INTEGER NOT NULL DEFAULT 0,
                invoice_number TEXT NOT NULL,
                invoice_date TEXT NOT NULL,
                rg_number TEXT NOT NULL DEFAULT '',
                rg_year TEXT NOT NULL DEFAULT '',
                payer TEXT NOT NULL DEFAULT 'INPS',
                compenso TEXT NOT NULL DEFAULT '0.00',
                expense_reimbursement TEXT NOT NULL DEFAULT '0.00',
                taxable_amount TEXT NOT NULL DEFAULT '0.00',
                vat_amount TEXT NOT NULL DEFAULT '0.00',
                withholding_amount TEXT NOT NULL DEFAULT '0.00',
                total_amount TEXT NOT NULL DEFAULT '0.00',
                payment_amount TEXT NOT NULL DEFAULT '0.00',
                payment_date TEXT NOT NULL DEFAULT '',
                signed INTEGER NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                locked INTEGER NOT NULL DEFAULT 0,
                receipt_date TEXT NOT NULL DEFAULT '',
                xml_path TEXT NOT NULL DEFAULT '',
                pdf_path TEXT NOT NULL DEFAULT '',
                decree_path TEXT NOT NULL DEFAULT '',
                decree_source_filename TEXT NOT NULL DEFAULT '',
                filename_base TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                extraction_method TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(owner_user_id, invoice_number, invoice_date)
            )
            """
        )
        return

    needs_rebuild = "owner_user_id" not in columns or "created_by_user_id" not in columns
    if not needs_rebuild:
        if "locked" not in columns:
            conn.execute(
                """
                ALTER TABLE invoices
                ADD COLUMN locked INTEGER NOT NULL DEFAULT 0
                """
            )
        return

    conn.execute("ALTER TABLE invoices RENAME TO invoices_legacy")
    legacy_columns = table_columns(conn, "invoices_legacy")
    has_locked = "locked" in legacy_columns

    conn.execute(
        """
        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL DEFAULT 0,
            created_by_user_id INTEGER NOT NULL DEFAULT 0,
            invoice_number TEXT NOT NULL,
            invoice_date TEXT NOT NULL,
            rg_number TEXT NOT NULL DEFAULT '',
            rg_year TEXT NOT NULL DEFAULT '',
            payer TEXT NOT NULL DEFAULT 'INPS',
            compenso TEXT NOT NULL DEFAULT '0.00',
            expense_reimbursement TEXT NOT NULL DEFAULT '0.00',
            taxable_amount TEXT NOT NULL DEFAULT '0.00',
            vat_amount TEXT NOT NULL DEFAULT '0.00',
            withholding_amount TEXT NOT NULL DEFAULT '0.00',
            total_amount TEXT NOT NULL DEFAULT '0.00',
            payment_amount TEXT NOT NULL DEFAULT '0.00',
            payment_date TEXT NOT NULL DEFAULT '',
            signed INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            locked INTEGER NOT NULL DEFAULT 0,
            receipt_date TEXT NOT NULL DEFAULT '',
            xml_path TEXT NOT NULL DEFAULT '',
            pdf_path TEXT NOT NULL DEFAULT '',
            decree_path TEXT NOT NULL DEFAULT '',
            decree_source_filename TEXT NOT NULL DEFAULT '',
            filename_base TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            extraction_method TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(owner_user_id, invoice_number, invoice_date)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO invoices (
            id, source, owner_user_id, created_by_user_id, invoice_number, invoice_date,
            rg_number, rg_year, payer, compenso, expense_reimbursement, taxable_amount,
            vat_amount, withholding_amount, total_amount, payment_amount, payment_date,
            signed, sent, locked, receipt_date, xml_path, pdf_path, decree_path,
            decree_source_filename, filename_base, notes, extraction_method, created_at, updated_at
        )
        SELECT
            id, source, 0, 0, invoice_number, invoice_date,
            rg_number, rg_year, payer, compenso, expense_reimbursement, taxable_amount,
            vat_amount, withholding_amount, total_amount, payment_amount, payment_date,
            signed, sent, {"locked" if has_locked else "0"}, receipt_date, xml_path, pdf_path, decree_path,
            decree_source_filename, filename_base, notes, extraction_method, created_at, updated_at
        FROM invoices_legacy
        """
    )
    conn.execute("DROP TABLE invoices_legacy")


def init_storage():
    for folder in (STORAGE_DIR, XML_OUTPUT_DIR, PDF_OUTPUT_DIR, DECREE_OUTPUT_DIR, LOG_DIR):
        folder.mkdir(parents=True, exist_ok=True)

    bootstrap_timestamp = datetime.now().isoformat(timespec="seconds")
    with get_connection() as conn:
        migrate_invoices_schema(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL DEFAULT '' UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_permissions (
                user_id INTEGER PRIMARY KEY,
                can_import_xml_history INTEGER NOT NULL DEFAULT 0,
                can_insert_archive_manual INTEGER NOT NULL DEFAULT 0,
                can_manage_invoice_flags INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_invoice_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                profile_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_api_quotas (
                user_id INTEGER PRIMARY KEY,
                quota_value INTEGER NOT NULL DEFAULT 0,
                quota_period TEXT NOT NULL DEFAULT 'week',
                is_unlimited INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                created_by_user_id INTEGER NOT NULL DEFAULT 0,
                invoice_id INTEGER,
                event_type TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_invoices_owner_date
            ON invoices(owner_user_id, invoice_date)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_invoices_created_by
            ON invoices(created_by_user_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_role
            ON users(role)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_usage_events_user_created_at
            ON usage_events(user_id, created_at)
            """
        )
        default_profile_json = json.dumps(INVOICE_PROFILE_DEFAULTS)
        conn.execute(
            """
            INSERT OR IGNORE INTO user_permissions (
                user_id, can_import_xml_history, can_insert_archive_manual,
                can_manage_invoice_flags, updated_at
            )
            SELECT id, 0, 0, 0, ?
            FROM users
            """,
            (bootstrap_timestamp,),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO user_invoice_profiles (
                user_id, profile_json, created_at, updated_at
            )
            SELECT id, ?, ?, ?
            FROM users
            """,
            (default_profile_json, bootstrap_timestamp, bootstrap_timestamp),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO user_api_quotas (
                user_id, quota_value, quota_period, is_unlimited, created_at, updated_at
            )
            SELECT
                id,
                0,
                'week',
                1,
                ?,
                ?
            FROM users
            """,
            (bootstrap_timestamp, bootstrap_timestamp),
        )
        conn.execute(
            """
            UPDATE user_api_quotas
            SET is_unlimited = 1,
                updated_at = ?
            WHERE user_id IN (
                SELECT id
                FROM users
                WHERE role = 'master'
            )
            """,
            (bootstrap_timestamp,),
        )
        owner_columns = table_columns(conn, "invoices")
        if "owner_user_id" in owner_columns:
            conn.execute(
                """
                UPDATE invoices
                SET owner_user_id = 0
                WHERE owner_user_id IS NULL
                """
            )
            conn.execute(
                """
                UPDATE invoices
                SET created_by_user_id = owner_user_id
                WHERE created_by_user_id IS NULL OR created_by_user_id = 0
                """
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                job_code TEXT PRIMARY KEY,
                last_run_at TEXT NOT NULL DEFAULT '',
                last_status TEXT NOT NULL DEFAULT '',
                last_message TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            UPDATE invoices
            SET decree_source_filename = trim(substr(notes, 12)),
                updated_at = ?
            WHERE source = 'imported'
              AND trim(coalesce(decree_source_filename, '')) = ''
              AND notes LIKE 'Import XML:%'
            """,
            (bootstrap_timestamp,),
        )


init_storage()


def configure_logging():
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=1_048_576,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)

    app.logger.handlers.clear()
    app.logger.setLevel(level)
    app.logger.addHandler(file_handler)
    app.logger.addHandler(stream_handler)
    app.logger.propagate = False

    if APP_ENV == "production" and APP_SECRET_KEY == "dev-insecure-secret-key-change-me":
        app.logger.warning(
            "APP_SECRET_KEY non configurata: usa un valore forte prima del deploy reale."
        )


configure_logging()
app.logger.info("Bootstrap applicazione completato in modalita' %s", APP_ENV)


# ---------------------------------------------------------------------------
# Authentication helpers
# ---------------------------------------------------------------------------

def users_exist() -> bool:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return bool(row["total"])


def get_user_by_id(user_id: int | None):
    if not user_id:
        return None
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT u.*, p.can_import_xml_history, p.can_insert_archive_manual, p.can_manage_invoice_flags
            FROM users u
            LEFT JOIN user_permissions p ON p.user_id = u.id
            WHERE u.id = ?
            """,
            (user_id,),
        ).fetchone()


def get_user_by_login(login_value: str):
    login_value = (login_value or "").strip()
    if not login_value:
        return None
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT u.*, p.can_import_xml_history, p.can_insert_archive_manual, p.can_manage_invoice_flags
            FROM users u
            LEFT JOIN user_permissions p ON p.user_id = u.id
            WHERE lower(u.username) = lower(?)
               OR lower(u.email) = lower(?)
            """,
            (login_value, login_value),
        ).fetchone()


def list_users():
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT u.*, p.can_import_xml_history, p.can_insert_archive_manual, p.can_manage_invoice_flags
            FROM users u
            LEFT JOIN user_permissions p ON p.user_id = u.id
            ORDER BY
                CASE WHEN u.role = 'master' THEN 0 ELSE 1 END,
                lower(u.username)
            """
        ).fetchall()


def list_active_users(include_master: bool = True):
    rows = list_users()
    if include_master:
        return [row for row in rows if row["is_active"]]
    return [row for row in rows if row["is_active"] and row["role"] != "master"]


def merge_invoice_profile(profile_data: dict | None) -> dict:
    profile = dict(INVOICE_PROFILE_DEFAULTS)
    if profile_data:
        for key, value in profile_data.items():
            if key in profile:
                profile[key] = str(value or "").strip()
    return profile


def normalize_invoice_profile_form(form_data) -> dict:
    profile = {}
    for field_name, default_value in INVOICE_PROFILE_DEFAULTS.items():
        raw_value = str(form_data.get(field_name, default_value) or "").strip()
        if field_name in INVOICE_PROFILE_UPPER_FIELDS:
            raw_value = raw_value.upper()
        profile[field_name] = raw_value
    return profile


def validate_invoice_profile_templates(profile: dict):
    formatter = string.Formatter()
    template_fields = [
        "description_main_template",
        "description_expense_template",
        "attachment_description_template",
    ]
    for field_name in template_fields:
        raw_template = profile[field_name]
        if not raw_template:
            raise ValueError(f"Inserisci un valore per {field_name}")
        try:
            parsed_chunks = list(formatter.parse(raw_template))
        except ValueError as exc:
            raise ValueError(f"Template non valido per {field_name}: {exc}") from exc

        for _, placeholder_name, _, _ in parsed_chunks:
            if placeholder_name and placeholder_name not in INVOICE_PROFILE_TEMPLATE_FIELDS:
                raise ValueError(
                    f"Placeholder non supportato in {field_name}: {placeholder_name}"
                )


def validate_invoice_profile(profile: dict) -> dict:
    required_fields = {
        "transmitter_country_code": "Paese trasmittente",
        "transmitter_id_code": "Codice trasmittente",
        "transmission_format": "Formato trasmissione",
        "recipient_code": "Codice destinatario",
        "issuer_vat_country": "Paese partita IVA emittente",
        "issuer_vat_code": "Partita IVA emittente",
        "issuer_tax_code": "Codice fiscale emittente",
        "issuer_first_name": "Nome emittente",
        "issuer_last_name": "Cognome emittente",
        "issuer_tax_regime": "Regime fiscale emittente",
        "issuer_address": "Indirizzo emittente",
        "issuer_zip": "CAP emittente",
        "issuer_city": "Comune emittente",
        "issuer_country": "Nazione emittente",
        "client_tax_code": "Codice fiscale cliente",
        "client_name": "Denominazione cliente",
        "client_address": "Indirizzo cliente",
        "client_zip": "CAP cliente",
        "client_city": "Comune cliente",
        "client_country": "Nazione cliente",
        "payment_beneficiary": "Beneficiario",
        "payment_terms_code": "Condizioni pagamento",
        "payment_method_code": "Modalita pagamento",
        "payment_bank_name": "Istituto finanziario",
        "payment_iban": "IBAN",
        "document_type": "Tipo documento",
        "currency": "Divisa",
        "vat_rate": "Aliquota IVA",
        "withholding_type": "Tipo ritenuta",
        "withholding_rate": "Aliquota ritenuta",
        "withholding_payment_reason": "Causale pagamento ritenuta",
        "vat_collection_method": "Esigibilita IVA",
        "default_payer": "Pagante di default",
    }
    for field_name, label in required_fields.items():
        if not profile.get(field_name, "").strip():
            raise ValueError(f"Campo obbligatorio mancante: {label}")

    for rate_field, label in {
        "vat_rate": "Aliquota IVA",
        "withholding_rate": "Aliquota ritenuta",
    }.items():
        try:
            parse_decimal(profile[rate_field])
        except Exception as exc:
            raise ValueError(f"Valore non valido per {label}") from exc

    validate_invoice_profile_templates(profile)
    return profile


def ensure_invoice_profile(conn: sqlite3.Connection, user_id: int, timestamp: str | None = None):
    now_value = timestamp or datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT OR IGNORE INTO user_invoice_profiles (
            user_id, profile_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        (user_id, json.dumps(INVOICE_PROFILE_DEFAULTS), now_value, now_value),
    )


def get_invoice_profile(user_id: int) -> dict:
    with get_connection() as conn:
        ensure_invoice_profile(conn, user_id)
        row = conn.execute(
            """
            SELECT profile_json, created_at, updated_at
            FROM user_invoice_profiles
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
    profile_data = {}
    if row and row["profile_json"]:
        try:
            profile_data = json.loads(row["profile_json"])
        except json.JSONDecodeError:
            profile_data = {}
    profile = merge_invoice_profile(profile_data)
    profile["user_id"] = user_id
    profile["created_at"] = row["created_at"] if row else ""
    profile["updated_at"] = row["updated_at"] if row else ""
    return profile


def get_invoice_profile_map(user_ids: list[int]) -> dict[str, dict]:
    unique_ids = []
    seen = set()
    for user_id in user_ids:
        if user_id in seen:
            continue
        seen.add(user_id)
        unique_ids.append(user_id)
    return {str(user_id): get_invoice_profile(user_id) for user_id in unique_ids}


def build_owner_profile_defaults(user_rows) -> dict[str, dict]:
    profile_map = get_invoice_profile_map([row["id"] for row in user_rows])
    return {
        user_id: {
            "default_payer": profile["default_payer"],
            "vat_rate": profile["vat_rate"],
            "withholding_rate": profile["withholding_rate"],
        }
        for user_id, profile in profile_map.items()
    }


def update_invoice_profile(user_id: int, profile: dict):
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        ensure_invoice_profile(conn, user_id)
        conn.execute(
            """
            UPDATE user_invoice_profiles
            SET profile_json = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (json.dumps(profile), now_iso(), user_id),
        )


def render_invoice_profile_template(template_value: str, context: dict, field_name: str) -> str:
    try:
        return template_value.format(**context)
    except KeyError as exc:
        raise ValueError(f"Placeholder non supportato in {field_name}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"Template non valido per {field_name}: {exc}") from exc


def normalize_api_quota_form(form_data, role: str = "user") -> dict:
    quota_period = str(form_data.get("quota_period", "week") or "week").strip().lower()
    quota_raw = str(form_data.get("quota_value", "") or "").strip()
    quota_value = parse_int(quota_raw, 0) if quota_raw else 0
    is_unlimited = bool(form_data.get("is_unlimited"))
    if (role or "").strip().lower() == "master":
        is_unlimited = True
    return {
        "quota_value": quota_value,
        "quota_period": quota_period,
        "is_unlimited": is_unlimited,
    }


def validate_api_quota_payload(payload: dict, role: str = "user") -> dict:
    quota_period = (payload.get("quota_period") or "week").strip().lower()
    if quota_period not in API_QUOTA_PERIOD_LABELS:
        raise ValueError("Periodo quota non valido")

    role = (role or "user").strip().lower()
    is_unlimited = bool(payload.get("is_unlimited")) or role == "master"
    quota_value = parse_int(payload.get("quota_value"), 0)
    if quota_value < 0:
        raise ValueError("Il numero di utilizzi non puo' essere negativo")
    if not is_unlimited and quota_value <= 0:
        raise ValueError("Inserisci un numero di utilizzi maggiore di zero")

    return {
        "quota_value": quota_value,
        "quota_period": quota_period,
        "is_unlimited": is_unlimited,
    }


def ensure_user_api_quota(conn: sqlite3.Connection, user_id: int, timestamp: str | None = None):
    now_value = timestamp or datetime.now().isoformat(timespec="seconds")
    user = conn.execute(
        "SELECT id, role FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not user:
        raise ValueError("Utente non trovato")

    conn.execute(
        """
        INSERT OR IGNORE INTO user_api_quotas (
            user_id, quota_value, quota_period, is_unlimited, created_at, updated_at
        ) VALUES (?, 0, 'week', 1, ?, ?)
        """,
        (user_id, now_value, now_value),
    )
    if user["role"] == "master":
        conn.execute(
            """
            UPDATE user_api_quotas
            SET is_unlimited = 1, updated_at = ?
            WHERE user_id = ?
            """,
            (now_value, user_id),
        )


def apply_user_api_quota(
    conn: sqlite3.Connection,
    user_id: int,
    quota_value: int,
    quota_period: str,
    is_unlimited: bool,
    timestamp: str | None = None,
):
    now_value = timestamp or datetime.now().isoformat(timespec="seconds")
    ensure_user_api_quota(conn, user_id, now_value)
    user = conn.execute(
        "SELECT id, role FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not user:
        raise ValueError("Utente non trovato")
    effective_unlimited = bool(is_unlimited) or user["role"] == "master"
    conn.execute(
        """
        UPDATE user_api_quotas
        SET quota_value = ?, quota_period = ?, is_unlimited = ?, updated_at = ?
        WHERE user_id = ?
        """,
        (
            max(int(quota_value), 0),
            quota_period,
            1 if effective_unlimited else 0,
            now_value,
            user_id,
        ),
    )


def quota_window_start(reference_dt: datetime, quota_period: str) -> datetime:
    delta = API_QUOTA_WINDOW_DELTAS.get(quota_period, API_QUOTA_WINDOW_DELTAS["week"])
    return reference_dt - delta


def count_api_usage(user_id: int, quota_period: str, reference_dt: datetime | None = None) -> int:
    current_dt = reference_dt or datetime.now()
    start_dt = quota_window_start(current_dt, quota_period)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM usage_events
            WHERE user_id = ?
              AND event_type = 'openai_extract_success'
              AND created_at >= ?
            """,
            (user_id, start_dt.isoformat(timespec="seconds")),
        ).fetchone()
    return int(row["total"] if row else 0)


def get_user_api_quota_summary(user_id: int, reference_dt: datetime | None = None) -> dict:
    current_dt = reference_dt or datetime.now()
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, username, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        ensure_user_api_quota(conn, user_id, current_dt.isoformat(timespec="seconds"))
        quota_row = conn.execute(
            """
            SELECT quota_value, quota_period, is_unlimited
            FROM user_api_quotas
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()

    quota_period = (
        str(quota_row["quota_period"]).strip().lower()
        if quota_row and quota_row["quota_period"]
        else "week"
    )
    if quota_period not in API_QUOTA_PERIOD_LABELS:
        quota_period = "week"
    quota_value = int(quota_row["quota_value"] if quota_row else 0)
    is_unlimited = bool(quota_row["is_unlimited"] if quota_row else 1) or user["role"] == "master"
    used_count = count_api_usage(user_id, quota_period, current_dt)
    remaining_count = None if is_unlimited else max(quota_value - used_count, 0)
    is_exhausted = False if is_unlimited else used_count >= quota_value
    return {
        "user_id": user_id,
        "username": user["username"],
        "user_role": user["role"],
        "quota_value": quota_value,
        "quota_period": quota_period,
        "quota_period_label": API_QUOTA_PERIOD_LABELS[quota_period],
        "window_label": API_QUOTA_WINDOW_LABELS[quota_period],
        "is_unlimited": is_unlimited,
        "used_count": used_count,
        "remaining_count": remaining_count,
        "is_exhausted": is_exhausted,
    }


def get_api_quota_summary_map(user_rows) -> dict[int, dict]:
    return {row["id"]: get_user_api_quota_summary(row["id"]) for row in user_rows}


def update_user_api_quota(user_id: int, quota_config: dict):
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        validated = validate_api_quota_payload(quota_config, user["role"])
        apply_user_api_quota(
            conn,
            user_id,
            validated["quota_value"],
            validated["quota_period"],
            validated["is_unlimited"],
            now_iso(),
        )


def record_api_usage_event(
    user_id: int,
    created_by_user_id: int,
    event_type: str = "openai_extract_success",
    invoice_id: int | None = None,
    meta: dict | None = None,
):
    with get_connection() as conn:
        ensure_user_api_quota(conn, user_id)
        conn.execute(
            """
            INSERT INTO usage_events (
                user_id, created_by_user_id, invoice_id, event_type, meta_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                created_by_user_id,
                invoice_id,
                event_type,
                json.dumps(meta or {}, ensure_ascii=True),
                now_iso(),
            ),
        )


def ensure_user_permissions(conn: sqlite3.Connection, user_id: int):
    conn.execute(
        """
        INSERT OR IGNORE INTO user_permissions (
            user_id, can_import_xml_history, can_insert_archive_manual,
            can_manage_invoice_flags, updated_at
        ) VALUES (?, 0, 0, 0, ?)
        """,
        (user_id, now_iso()),
    )


def assign_legacy_invoices_to_master(master_user_id: int):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE invoices
            SET owner_user_id = ?, created_by_user_id = ?
            WHERE owner_user_id = 0 OR created_by_user_id = 0
            """,
            (master_user_id, master_user_id),
        )


def create_user_account(
    username: str,
    email: str,
    password: str,
    role: str = "user",
    is_active: bool = True,
    permissions: dict | None = None,
    api_quota: dict | None = None,
) -> int:
    username = (username or "").strip()
    email = (email or "").strip().lower()
    password = password or ""
    role = (role or "user").strip().lower()

    if role not in {"master", "user"}:
        raise ValueError("Ruolo non valido")
    if not username:
        raise ValueError("Inserisci uno username")
    if len(password) < 8:
        raise ValueError("La password deve contenere almeno 8 caratteri")
    if not email or "@" not in email:
        raise ValueError("Inserisci una email valida")

    now = now_iso()
    with get_connection() as conn:
        if role == "master":
            existing_master = conn.execute(
                "SELECT id FROM users WHERE role = 'master'"
            ).fetchone()
            if existing_master:
                raise ValueError("Esiste gia' un account master")

        try:
            cursor = conn.execute(
                """
                INSERT INTO users (
                    username, email, password_hash, role, is_active,
                    created_at, updated_at, last_login_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, '')
                """,
                (
                    username,
                    email,
                    generate_password_hash(password),
                    role,
                    1 if is_active else 0,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("Username o email gia' presenti") from exc

        user_id = cursor.lastrowid
        ensure_user_permissions(conn, user_id)
        ensure_invoice_profile(conn, user_id, now)
        ensure_user_api_quota(conn, user_id, now)
        if permissions:
            conn.execute(
                """
                UPDATE user_permissions
                SET can_import_xml_history = ?,
                    can_insert_archive_manual = ?,
                    can_manage_invoice_flags = ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (
                    1 if permissions.get("can_import_xml_history") else 0,
                    1 if permissions.get("can_insert_archive_manual") else 0,
                    1 if permissions.get("can_manage_invoice_flags") else 0,
                    now,
                    user_id,
                ),
            )
        if api_quota:
            validated_quota = validate_api_quota_payload(api_quota, role)
            apply_user_api_quota(
                conn,
                user_id,
                validated_quota["quota_value"],
                validated_quota["quota_period"],
                validated_quota["is_unlimited"],
                now,
            )

    if role == "master":
        assign_legacy_invoices_to_master(user_id)
    return user_id


def update_user_active(user_id: int, is_active: bool):
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        if user["role"] == "master":
            raise ValueError("L'account master non puo' essere disattivato")
        conn.execute(
            """
            UPDATE users
            SET is_active = ?, updated_at = ?
            WHERE id = ?
            """,
            (1 if is_active else 0, now_iso(), user_id),
        )


def reset_user_password(user_id: int, password: str):
    if len(password or "") < 8:
        raise ValueError("La password deve contenere almeno 8 caratteri")
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        conn.execute(
            """
            UPDATE users
            SET password_hash = ?, updated_at = ?
            WHERE id = ?
            """,
            (generate_password_hash(password), now_iso(), user_id),
        )


def update_user_permissions(user_id: int, permissions: dict):
    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, role FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not user:
            raise ValueError("Utente non trovato")
        if user["role"] == "master":
            raise ValueError("I permessi extra non si applicano al master")
        ensure_user_permissions(conn, user_id)
        conn.execute(
            """
            UPDATE user_permissions
            SET can_import_xml_history = ?,
                can_insert_archive_manual = ?,
                can_manage_invoice_flags = ?,
                updated_at = ?
            WHERE user_id = ?
            """,
            (
                1 if permissions.get("can_import_xml_history") else 0,
                1 if permissions.get("can_insert_archive_manual") else 0,
                1 if permissions.get("can_manage_invoice_flags") else 0,
                now_iso(),
                user_id,
            ),
        )


def login_user(user_row: sqlite3.Row):
    session["user_id"] = user_row["id"]
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET last_login_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now_iso(), now_iso(), user_row["id"]),
        )


def logout_user():
    session.pop("user_id", None)


def current_user():
    return getattr(g, "current_user", None)


def is_master_user(user_row: sqlite3.Row | None) -> bool:
    return bool(user_row and user_row["role"] == "master")


def can_import_xml(user_row: sqlite3.Row | None) -> bool:
    return bool(user_row and (is_master_user(user_row) or user_row["can_import_xml_history"]))


def can_insert_archive_manual(user_row: sqlite3.Row | None) -> bool:
    return bool(user_row and (is_master_user(user_row) or user_row["can_insert_archive_manual"]))


def can_manage_invoice_flags(user_row: sqlite3.Row | None) -> bool:
    return bool(user_row and (is_master_user(user_row) or user_row["can_manage_invoice_flags"]))


def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapped


def master_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        user = current_user()
        if not user:
            return redirect(url_for("login", next=request.path))
        if not is_master_user(user):
            flash("Area riservata all'account master.", "error")
            return redirect(url_for("index"))
        return view_func(*args, **kwargs)

    return wrapped


def should_bypass_auth() -> bool:
    if request.path.startswith("/static/"):
        return True
    return request.endpoint in {
        "healthz",
        "login",
        "setup_master",
        "logout",
    }


@app.before_request
def load_current_user():
    g.current_user = get_user_by_id(session.get("user_id"))

    if request.endpoint == "healthz":
        return None

    if not users_exist():
        if request.endpoint != "setup_master":
            return redirect(url_for("setup_master"))
        return None

    if should_bypass_auth():
        return None

    if not g.current_user:
        return redirect(url_for("login", next=request.path))

    if not g.current_user["is_active"]:
        logout_user()
        flash("Account disattivato. Contatta il master.", "error")
        return redirect(url_for("login"))

    return None


@app.context_processor
def inject_auth_context():
    user = current_user()
    return {
        "current_user": user,
        "is_master_user": is_master_user(user),
        "can_import_xml_user": can_import_xml(user),
        "can_insert_archive_manual_user": can_insert_archive_manual(user),
        "can_manage_invoice_flags_user": can_manage_invoice_flags(user),
        "users_bootstrapped": users_exist(),
    }


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_iso() -> str:
    return date.today().isoformat()


def format_display_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        return date.fromisoformat(raw).strftime("%d/%m/%Y")
    except ValueError:
        return raw


def random_progressivo(length=5):
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fmt(value: Decimal) -> str:
    return str(quantize_money(value))


def format_euro(value) -> str:
    amount = parse_decimal(value)
    return f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def parse_decimal(value, default="0.00") -> Decimal:
    if isinstance(value, Decimal):
        return quantize_money(value)

    if value is None:
        return quantize_money(Decimal(default))

    if isinstance(value, (int, float)):
        return quantize_money(Decimal(str(value)))

    raw = str(value).strip()
    if not raw:
        return quantize_money(Decimal(default))

    raw = raw.replace("€", "").replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")

    try:
        return quantize_money(Decimal(raw))
    except InvalidOperation:
        return quantize_money(Decimal(default))


def normalize_money_string(value) -> str:
    return fmt(parse_decimal(value))


def nome_file_base(numero_fattura: str, data_fattura: str) -> str:
    anno_2 = data_fattura[2:4]
    try:
        num = int(numero_fattura.split("/")[0].split("-")[0])
        return f"IT06928550729_{anno_2}{num:03d}"
    except (ValueError, IndexError):
        safe = re.sub(r"[^A-Za-z0-9_-]+", "-", numero_fattura).strip("-")
        return f"IT06928550729_{anno_2}_{safe or 'fattura'}"


def split_invoice_number(value: str):
    match = re.match(r"^\s*(\d+)(.*)$", (value or "").strip())
    if not match:
        return None
    number = int(match.group(1))
    suffix = match.group(2) or ""
    return number, suffix


def get_suggested_invoice_number(owner_user_id: int | None = None, default="1/e") -> str:
    best_number = 0
    best_suffix = "/e"
    best_id = -1

    with get_connection() as conn:
        if owner_user_id is None:
            rows = conn.execute("SELECT id, invoice_number FROM invoices").fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, invoice_number
                FROM invoices
                WHERE owner_user_id = ?
                """,
                (owner_user_id,),
            ).fetchall()

    for row in rows:
        parts = split_invoice_number(row["invoice_number"])
        if not parts:
            continue
        number, suffix = parts
        if number > best_number or (number == best_number and row["id"] > best_id):
            best_number = number
            best_suffix = suffix or "/e"
            best_id = row["id"]

    if best_number <= 0:
        return default
    return f"{best_number + 1}{best_suffix or '/e'}"


def parse_int(value, default=None):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def resolve_owner_user_id(raw_owner_value=None) -> int:
    user = current_user()
    if not user:
        raise ValueError("Utente non autenticato")
    if is_master_user(user):
        owner_id = parse_int(raw_owner_value, user["id"])
        owner = get_user_by_id(owner_id)
        if not owner:
            raise ValueError("Utente proprietario non trovato")
        return owner["id"]
    return user["id"]


def can_access_invoice_row(user_row: sqlite3.Row | None, invoice_row: sqlite3.Row | None) -> bool:
    if not user_row or not invoice_row:
        return False
    if is_master_user(user_row):
        return True
    return invoice_row["owner_user_id"] == user_row["id"]


def get_accessible_invoice_row(invoice_id: int):
    row = get_invoice_row(invoice_id)
    if not row:
        return None
    return row if can_access_invoice_row(current_user(), row) else None


def invoice_sort_key(row: sqlite3.Row):
    parts = split_invoice_number(row["invoice_number"])
    if parts:
        number, suffix = parts
        return (number, suffix.casefold(), row["invoice_date"], row["id"])
    return (10**9, (row["invoice_number"] or "").casefold(), row["invoice_date"], row["id"])


def calculate_totals(
    compenso: Decimal,
    rimborsi_spese: Decimal,
    vat_rate: str | Decimal = "22.00",
    withholding_rate: str | Decimal = "20.00",
) -> dict:
    imponibile = quantize_money(compenso + rimborsi_spese)
    vat_decimal = parse_decimal(vat_rate) / Decimal("100")
    withholding_decimal = parse_decimal(withholding_rate) / Decimal("100")
    iva = quantize_money(imponibile * vat_decimal)
    # Assunzione operativa: i rimborsi spese confluiscono nell'imponibile IVA,
    # ma la ritenuta si applica solo al compenso professionale.
    ritenuta = quantize_money(compenso * withholding_decimal)
    totale_documento = quantize_money(imponibile + iva)
    importo_pagamento = quantize_money(totale_documento - ritenuta)
    return {
        "compenso": quantize_money(compenso),
        "rimborsi_spese": quantize_money(rimborsi_spese),
        "imponibile": imponibile,
        "iva": iva,
        "ritenuta": ritenuta,
        "totale_documento": totale_documento,
        "importo_pagamento": importo_pagamento,
    }


def ensure_iso_date(value: str, field_name: str, required=True) -> str:
    raw = (value or "").strip()
    if not raw:
        if required:
            raise ValueError(f"Campo obbligatorio mancante: {field_name}")
        return ""

    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise ValueError(f"Data non valida per {field_name}") from exc


def validate_year(value: str, field_name: str, required=True) -> str:
    raw = (value or "").strip()
    if not raw:
        if required:
            raise ValueError(f"Campo obbligatorio mancante: {field_name}")
        return ""
    if not re.fullmatch(r"\d{4}", raw):
        raise ValueError(f"Anno non valido: {field_name}")
    return raw


def parse_bool(value) -> int:
    if isinstance(value, bool):
        return int(value)
    return int(str(value).strip().lower() in {"1", "true", "on", "yes"})


def is_truthy(value) -> bool:
    return bool(parse_bool(value))


def original_import_filename(value: str) -> str:
    return Path(value or "").name.strip()


def save_bytes(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def stored_path(raw_path: str) -> Path | None:
    raw = (raw_path or "").strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.exists() else None


def response_output_text(payload: dict) -> str:
    direct_text = payload.get("output_text")
    if direct_text:
        return direct_text

    texts = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            content_type = content.get("type")
            if content_type == "output_text":
                texts.append(content.get("text", ""))
            elif content_type == "refusal":
                refusal_text = content.get("refusal") or "Richiesta rifiutata dal modello"
                raise RuntimeError(refusal_text)

    return "".join(texts).strip()


def parse_json_output(text: str) -> dict:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if not match:
            raise
        return json.loads(match.group(0))


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_pdf_text(pdf_bytes: bytes) -> str:
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


def extract_payer(text: str) -> str:
    match = re.search(
        r"(?i:a carico della parte resistente)\s+([A-Z][A-Z.]+(?:\s+[A-Z][A-Z.]+){0,4})",
        text,
    )
    if match:
        return re.sub(r"[\.\s]+", "", match.group(1)).upper()

    match = re.search(
        r"(?i:all)[^A-Z]{1,3}([A-Z][A-Z.]+(?:\s+[A-Z][A-Z.]+){0,4})\s+(?i:di pagare)",
        text,
    )
    if match:
        return re.sub(r"[\.\s]+", "", match.group(1)).upper()

    match = re.search(
        r"ISTITUTO NAZIONALE DELLA PREVIDENZA SOCIALE|I\.N\.P\.S\.|INPS",
        text,
        re.IGNORECASE,
    )
    if match:
        return "INPS"

    return "INPS"


def extract_local_data(text: str) -> dict:
    numero_rg = ""
    anno_rg = ""

    rg_patterns = [
        r"N\.\s*R\.G\.?\s*(\d+)\s*/\s*(\d{4})",
        r"numero d[’']ordine\s+(\d+)\s+dell['’]?anno\s+(\d{4})\s+di\s+R\.G\.",
    ]
    for pattern in rg_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            numero_rg = match.group(1)
            anno_rg = match.group(2)
            break

    compenso = Decimal("0.00")
    rimborsi_spese = Decimal("0.00")

    match_compenso = re.search(
        r"somma di(?:\s+euro|\s*€)?\s*([\d.,]+)(?=[\s\S]{0,100}?(?:a titolo di onorario|per onorario|a titolo di compenso|per compenso))",
        text,
        re.IGNORECASE,
    )
    if not match_compenso:
        match_compenso = re.search(
            r"([\d.,]+)(?=[\s\S]{0,40}?(?:a titolo di onorario|per onorario|a titolo di compenso|per compenso))",
            text,
            re.IGNORECASE,
        )
    if match_compenso:
        compenso = parse_decimal(match_compenso.group(1))

    match_spese = re.search(
        r"(?:ed|e)\s*[€E]\s*([\d.,]+)(?=[\s\S]{0,160}?(?:indennit|spese di viaggio|rimborso delle spese|rimborso spese|rimborsi spese))",
        text,
        re.IGNORECASE,
    )
    if match_spese:
        rimborsi_spese = parse_decimal(match_spese.group(1))

    extracted_fields = []
    if numero_rg:
        extracted_fields.extend(["numero_rg", "anno_rg"])
    if compenso > 0:
        extracted_fields.append("compenso")
    if rimborsi_spese >= 0:
        extracted_fields.append("rimborsi_spese")

    return {
        "numero_rg": numero_rg,
        "anno_rg": anno_rg,
        "compenso": fmt(compenso),
        "rimborsi_spese": fmt(rimborsi_spese),
        "pagante": extract_payer(text),
        "estratti": extracted_fields,
        "metodo": "regex_fallback",
        "warning": "",
    }


# ---------------------------------------------------------------------------
# OpenAI extraction
# ---------------------------------------------------------------------------

def openai_extract_structured_data(document_text: str) -> dict:
    if not OPENAI_API_KEY:
        raise RuntimeError("API key OpenAI non configurata")

    schema = {
        "type": "object",
        "properties": {
            "numero_rg": {"type": "string"},
            "anno_rg": {"type": "string"},
            "compenso": {"type": "number"},
            "rimborsi_spese": {"type": "number"},
        },
        "required": ["numero_rg", "anno_rg", "compenso", "rimborsi_spese"],
        "additionalProperties": False,
    }

    prompt = (
        "Leggi il testo di un decreto di liquidazione italiano e rispondi in JSON.\n"
        "Estrai solo questi dati:\n"
        "1. numero_rg: il numero di R.G. senza anno\n"
        "2. anno_rg: l'anno del R.G.\n"
        "3. compenso: la somma dovuta al professionista per compenso/onorario\n"
        "4. rimborsi_spese: la somma dovuta per rimborsi spese/indennita'/spese di viaggio\n\n"
        "Regole:\n"
        "- non inventare valori assenti\n"
        "- se il dato non e' presente, usa stringa vuota per numero_rg/anno_rg e 0 per gli importi\n"
        "- usa numeri decimali senza simbolo euro\n"
        "- il compenso non deve includere i rimborsi spese\n"
        "- i rimborsi spese sono separati dal compenso quando il decreto li distingue esplicitamente"
    )

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": "Sei un estrattore affidabile di dati da decreti di liquidazione."},
            {"role": "user", "content": f"{prompt}\n\nTESTO DECRETO:\n{document_text[:50000]}"},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "decreto_liquidazione",
                "schema": schema,
                "strict": True,
            }
        },
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        try:
            error_payload = json.loads(body)
            message = error_payload.get("error", {}).get("message") or body
        except json.JSONDecodeError:
            message = body or str(exc)
        raise RuntimeError(f"Errore OpenAI: {message}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Errore di rete OpenAI: {exc.reason}") from exc

    output_text = response_output_text(payload)
    if not output_text:
        raise RuntimeError("Risposta OpenAI vuota")

    data = parse_json_output(output_text)
    return {
        "numero_rg": str(data.get("numero_rg", "")).strip(),
        "anno_rg": str(data.get("anno_rg", "")).strip(),
        "compenso": normalize_money_string(data.get("compenso", 0)),
        "rimborsi_spese": normalize_money_string(data.get("rimborsi_spese", 0)),
        "metodo": "openai",
        "warning": "",
    }


def extract_decreto_data(
    pdf_bytes: bytes,
    owner_user_id: int | None = None,
    created_by_user_id: int | None = None,
) -> dict:
    try:
        document_text = extract_pdf_text(pdf_bytes)
    except Exception as exc:
        raise RuntimeError(f"Impossibile leggere il PDF: {exc}") from exc

    if not document_text.strip():
        raise RuntimeError("Il PDF non contiene testo leggibile")

    fallback = extract_local_data(document_text)
    quota_summary = get_user_api_quota_summary(owner_user_id) if owner_user_id else None

    if quota_summary and quota_summary["is_exhausted"]:
        fallback["warning"] = (
            f"Quota API esaurita: {quota_summary['used_count']} chiamate su "
            f"{quota_summary['quota_value']} nella finestra {quota_summary['window_label']}"
        )
        fallback["api_quota"] = quota_summary
        return fallback

    try:
        ai_data = openai_extract_structured_data(document_text)
        if owner_user_id:
            record_api_usage_event(
                owner_user_id,
                created_by_user_id or owner_user_id,
                meta={"model": OPENAI_MODEL},
            )
            quota_summary = get_user_api_quota_summary(owner_user_id)
        result = {
            "numero_rg": ai_data["numero_rg"] or fallback["numero_rg"],
            "anno_rg": ai_data["anno_rg"] or fallback["anno_rg"],
            "compenso": ai_data["compenso"],
            "rimborsi_spese": ai_data["rimborsi_spese"],
            "pagante": fallback["pagante"],
            "estratti": ["numero_rg", "anno_rg", "compenso", "rimborsi_spese", "pagante"],
            "metodo": ai_data["metodo"],
            "warning": "",
            "api_quota": quota_summary,
        }
    except Exception as exc:
        fallback["warning"] = str(exc)
        fallback["api_quota"] = quota_summary
        result = fallback

    return result


# ---------------------------------------------------------------------------
# XML import
# ---------------------------------------------------------------------------

def xml_find_text(node, tag_name: str, default="") -> str:
    values = node.xpath(f'.//*[local-name()="{tag_name}"]/text()')
    if values:
        return str(values[0]).strip()
    return default


def xml_find_texts(node, tag_name: str) -> list[str]:
    return [str(value).strip() for value in node.xpath(f'.//*[local-name()="{tag_name}"]/text()') if str(value).strip()]


def parse_rg_from_text(text: str) -> tuple[str, str]:
    match = re.search(r"n\.?\s*(\d+)\s*[-/]\s*(\d{4})\s*R\.?\s*G\.?", text, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def parse_payer_from_invoice_text(text: str) -> str:
    match = re.search(r"\(([^()]+)\)\s*$", text.strip())
    if match:
        return match.group(1).strip().upper()
    return "INPS"


def parse_invoice_xml(xml_bytes: bytes, filename: str) -> dict:
    filename = original_import_filename(filename)
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as exc:
        raise ValueError(f"XML non valido per {filename}: {exc}") from exc

    invoice_number = xml_find_text(root, "Numero")
    invoice_date = xml_find_text(root, "Data")
    payment_date = xml_find_text(root, "DataRiferimentoTerminiPagamento") or invoice_date
    total_amount = xml_find_text(root, "ImportoTotaleDocumento", "0.00")
    withholding_amount = xml_find_text(root, "ImportoRitenuta", "0.00")
    payment_amount = xml_find_text(root, "ImportoPagamento", "0.00")
    vat_amount = "0.00"
    imponibile = Decimal("0.00")

    for riepilogo in root.xpath('//*[local-name()="DatiRiepilogo"]'):
        imponibile += parse_decimal(xml_find_text(riepilogo, "ImponibileImporto", "0.00"))
        vat_amount = fmt(parse_decimal(vat_amount) + parse_decimal(xml_find_text(riepilogo, "Imposta", "0.00")))

    compenso = Decimal("0.00")
    rimborsi_spese = Decimal("0.00")
    detail_lines = root.xpath('//*[local-name()="DettaglioLinee"]')
    for line in detail_lines:
        line_total = parse_decimal(
            xml_find_text(line, "PrezzoTotale") or xml_find_text(line, "PrezzoUnitario") or "0.00"
        )
        has_withholding = xml_find_text(line, "Ritenuta").upper() == "SI"
        if has_withholding:
            compenso += line_total
        else:
            rimborsi_spese += line_total

    if imponibile == 0 and detail_lines:
        imponibile = quantize_money(compenso + rimborsi_spese)

    if compenso == 0 and rimborsi_spese == 0:
        compenso = imponibile

    causale = xml_find_text(root, "Causale")
    attachment_description = xml_find_text(root, "DescrizioneAttachment")
    rg_number, rg_year = parse_rg_from_text(causale or attachment_description)
    payer = parse_payer_from_invoice_text(causale) if causale else "INPS"

    if not invoice_number or not invoice_date:
        raise ValueError(f"XML {filename} privo di numero o data fattura")

    return {
        "invoice_number": invoice_number,
        "invoice_date": ensure_iso_date(invoice_date, "data_fattura"),
        "rg_number": rg_number,
        "rg_year": rg_year,
        "payer": payer,
        "compenso": fmt(compenso),
        "expense_reimbursement": fmt(rimborsi_spese),
        "taxable_amount": fmt(imponibile),
        "vat_amount": fmt(parse_decimal(vat_amount)),
        "withholding_amount": fmt(parse_decimal(withholding_amount)),
        "total_amount": fmt(parse_decimal(total_amount)),
        "payment_amount": fmt(parse_decimal(payment_amount)),
        "payment_date": ensure_iso_date(payment_date, "data_pagamento", required=False) or ensure_iso_date(invoice_date, "data_fattura"),
        "original_filename": filename,
    }


# ---------------------------------------------------------------------------
# XML generation
# ---------------------------------------------------------------------------

def append_xml_text(parent, tag_name: str, value: str, required: bool = True):
    raw_value = str(value or "").strip()
    if not raw_value and not required:
        return None
    if required and not raw_value:
        raise ValueError(f"Valore obbligatorio mancante per {tag_name}")
    element = etree.SubElement(parent, tag_name)
    element.text = raw_value
    return element


def genera_xml(dati: dict, pdf_bytes: bytes, pdf_filename: str, profile: dict | None = None) -> bytes:
    profile_values = merge_invoice_profile(profile)
    compenso = parse_decimal(dati["compenso"])
    rimborsi_spese = parse_decimal(dati.get("rimborsi_spese"))
    totals = calculate_totals(
        compenso,
        rimborsi_spese,
        vat_rate=profile_values["vat_rate"],
        withholding_rate=profile_values["withholding_rate"],
    )

    numero_rg = dati["numero_rg"]
    anno_rg = dati["anno_rg"]
    pagante = dati.get("pagante", profile_values["default_payer"]) or profile_values["default_payer"]
    data_fattura = dati["data_fattura"]
    data_pagamento = dati.get("data_pagamento") or data_fattura
    numero_fattura = dati["numero_fattura"]
    template_context = {
        "numero_rg": numero_rg,
        "anno_rg": anno_rg,
        "pagante": pagante,
    }

    descrizione_decreto = render_invoice_profile_template(
        profile_values["description_main_template"],
        template_context,
        "description_main_template",
    )
    descrizione_rimborsi = render_invoice_profile_template(
        profile_values["description_expense_template"],
        template_context,
        "description_expense_template",
    )
    descrizione_allegato = render_invoice_profile_template(
        profile_values["attachment_description_template"],
        template_context,
        "attachment_description_template",
    )

    root = etree.Element(
        f"{{{NS}}}FatturaElettronica",
        versione=profile_values["transmission_format"],
        nsmap=NS_MAP,
    )

    header = etree.SubElement(root, "FatturaElettronicaHeader")

    dt = etree.SubElement(header, "DatiTrasmissione")
    idt = etree.SubElement(dt, "IdTrasmittente")
    append_xml_text(idt, "IdPaese", profile_values["transmitter_country_code"])
    append_xml_text(idt, "IdCodice", profile_values["transmitter_id_code"])
    append_xml_text(dt, "ProgressivoInvio", random_progressivo())
    append_xml_text(dt, "FormatoTrasmissione", profile_values["transmission_format"])
    append_xml_text(dt, "CodiceDestinatario", profile_values["recipient_code"])

    cp = etree.SubElement(header, "CedentePrestatore")
    da = etree.SubElement(cp, "DatiAnagrafici")
    idiva = etree.SubElement(da, "IdFiscaleIVA")
    append_xml_text(idiva, "IdPaese", profile_values["issuer_vat_country"])
    append_xml_text(idiva, "IdCodice", profile_values["issuer_vat_code"])
    append_xml_text(da, "CodiceFiscale", profile_values["issuer_tax_code"])
    anag = etree.SubElement(da, "Anagrafica")
    append_xml_text(anag, "Nome", profile_values["issuer_first_name"])
    append_xml_text(anag, "Cognome", profile_values["issuer_last_name"])
    append_xml_text(da, "RegimeFiscale", profile_values["issuer_tax_regime"])
    sede = etree.SubElement(cp, "Sede")
    append_xml_text(sede, "Indirizzo", profile_values["issuer_address"])
    append_xml_text(sede, "NumeroCivico", profile_values["issuer_street_number"], required=False)
    append_xml_text(sede, "CAP", profile_values["issuer_zip"])
    append_xml_text(sede, "Comune", profile_values["issuer_city"])
    append_xml_text(sede, "Provincia", profile_values["issuer_province"], required=False)
    append_xml_text(sede, "Nazione", profile_values["issuer_country"])

    cc = etree.SubElement(header, "CessionarioCommittente")
    da2 = etree.SubElement(cc, "DatiAnagrafici")
    append_xml_text(da2, "CodiceFiscale", profile_values["client_tax_code"])
    anag2 = etree.SubElement(da2, "Anagrafica")
    append_xml_text(anag2, "Denominazione", profile_values["client_name"])
    sede2 = etree.SubElement(cc, "Sede")
    append_xml_text(sede2, "Indirizzo", profile_values["client_address"])
    append_xml_text(sede2, "NumeroCivico", profile_values["client_street_number"], required=False)
    append_xml_text(sede2, "CAP", profile_values["client_zip"])
    append_xml_text(sede2, "Comune", profile_values["client_city"])
    append_xml_text(sede2, "Provincia", profile_values["client_province"], required=False)
    append_xml_text(sede2, "Nazione", profile_values["client_country"])

    body = etree.SubElement(root, "FatturaElettronicaBody")

    dg = etree.SubElement(body, "DatiGenerali")
    dgd = etree.SubElement(dg, "DatiGeneraliDocumento")
    append_xml_text(dgd, "TipoDocumento", profile_values["document_type"])
    append_xml_text(dgd, "Divisa", profile_values["currency"])
    append_xml_text(dgd, "Data", data_fattura)
    append_xml_text(dgd, "Numero", numero_fattura)
    dr = etree.SubElement(dgd, "DatiRitenuta")
    append_xml_text(dr, "TipoRitenuta", profile_values["withholding_type"])
    append_xml_text(dr, "ImportoRitenuta", fmt(totals["ritenuta"]))
    append_xml_text(dr, "AliquotaRitenuta", profile_values["withholding_rate"])
    append_xml_text(dr, "CausalePagamento", profile_values["withholding_payment_reason"])
    append_xml_text(dgd, "ImportoTotaleDocumento", fmt(totals["totale_documento"]))
    append_xml_text(dgd, "Causale", descrizione_decreto)

    dbs = etree.SubElement(body, "DatiBeniServizi")

    line_number = 1
    if compenso > 0:
        dl = etree.SubElement(dbs, "DettaglioLinee")
        append_xml_text(dl, "NumeroLinea", str(line_number))
        append_xml_text(dl, "Descrizione", descrizione_decreto)
        append_xml_text(dl, "PrezzoUnitario", fmt(compenso))
        append_xml_text(dl, "PrezzoTotale", fmt(compenso))
        append_xml_text(dl, "AliquotaIVA", profile_values["vat_rate"])
        append_xml_text(dl, "Ritenuta", "SI")
        line_number += 1

    if rimborsi_spese > 0:
        dl = etree.SubElement(dbs, "DettaglioLinee")
        append_xml_text(dl, "NumeroLinea", str(line_number))
        append_xml_text(dl, "Descrizione", descrizione_rimborsi)
        append_xml_text(dl, "PrezzoUnitario", fmt(rimborsi_spese))
        append_xml_text(dl, "PrezzoTotale", fmt(rimborsi_spese))
        append_xml_text(dl, "AliquotaIVA", profile_values["vat_rate"])

    riepilogo = etree.SubElement(dbs, "DatiRiepilogo")
    append_xml_text(riepilogo, "AliquotaIVA", profile_values["vat_rate"])
    append_xml_text(riepilogo, "ImponibileImporto", fmt(totals["imponibile"]))
    append_xml_text(riepilogo, "Imposta", fmt(totals["iva"]))
    append_xml_text(riepilogo, "EsigibilitaIVA", profile_values["vat_collection_method"])

    dp = etree.SubElement(body, "DatiPagamento")
    append_xml_text(dp, "CondizioniPagamento", profile_values["payment_terms_code"])
    dett_pag = etree.SubElement(dp, "DettaglioPagamento")
    append_xml_text(dett_pag, "Beneficiario", profile_values["payment_beneficiary"])
    append_xml_text(dett_pag, "ModalitaPagamento", profile_values["payment_method_code"])
    append_xml_text(dett_pag, "DataRiferimentoTerminiPagamento", data_pagamento)
    append_xml_text(dett_pag, "ImportoPagamento", fmt(totals["importo_pagamento"]))
    append_xml_text(dett_pag, "IstitutoFinanziario", profile_values["payment_bank_name"])
    append_xml_text(dett_pag, "IBAN", profile_values["payment_iban"])

    allegati = etree.SubElement(body, "Allegati")
    append_xml_text(allegati, "NomeAttachment", pdf_filename)
    append_xml_text(allegati, "DescrizioneAttachment", descrizione_allegato)
    append_xml_text(allegati, "Attachment", base64.b64encode(pdf_bytes).decode("ascii"))

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


# ---------------------------------------------------------------------------
# XSLT / PDF
# ---------------------------------------------------------------------------

def applica_xsl(xml_bytes: bytes) -> str:
    xsl_tree = etree.parse(str(XSL_PATH))
    transform = etree.XSLT(xsl_tree)
    xml_tree = etree.fromstring(xml_bytes)
    result = transform(xml_tree)
    return str(result)


def genera_pdf_fattura(html_str: str) -> bytes:
    launch_args = env_csv("PLAYWRIGHT_CHROMIUM_ARGS")
    if env_bool("PLAYWRIGHT_NO_SANDBOX", False):
        for flag in ("--no-sandbox", "--disable-setuid-sandbox"):
            if flag not in launch_args:
                launch_args.append(flag)
    if env_bool("PLAYWRIGHT_DISABLE_DEV_SHM_USAGE", False):
        flag = "--disable-dev-shm-usage"
        if flag not in launch_args:
            launch_args.append(flag)

    with sync_playwright() as p:
        launch_options = {}
        if launch_args:
            launch_options["args"] = launch_args
        browser = p.chromium.launch(**launch_options)
        page = browser.new_page()
        page.set_content(html_str, wait_until="domcontentloaded")
        pdf_bytes = page.pdf(
            format="A4",
            margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"},
        )
        browser.close()
    return pdf_bytes


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def invoice_row_to_view(row: sqlite3.Row) -> dict:
    source_labels = {
        "system": "Sistema",
        "manual": "Manuale",
        "imported": "Importata",
    }
    xml_path = stored_path(row["xml_path"])
    pdf_path = stored_path(row["pdf_path"])
    decree_path = stored_path(row["decree_path"])
    return {
        "id": row["id"],
        "source": row["source"],
        "source_label": source_labels.get(row["source"], row["source"].title()),
        "invoice_number": row["invoice_number"],
        "invoice_date": row["invoice_date"],
        "invoice_date_display": format_display_date(row["invoice_date"]),
        "rg_display": (
            f"{row['rg_number']}/{row['rg_year']}"
            if row["rg_number"] and row["rg_year"]
            else ""
        ),
        "payer": row["payer"],
        "compenso": format_euro(row["compenso"]),
        "expense_reimbursement": format_euro(row["expense_reimbursement"]),
        "taxable_amount": format_euro(row["taxable_amount"]),
        "vat_amount": format_euro(row["vat_amount"]),
        "withholding_amount": format_euro(row["withholding_amount"]),
        "total_amount": format_euro(row["total_amount"]),
        "net_amount": format_euro(row["payment_amount"]),
        "payment_amount": format_euro(row["payment_amount"]),
        "payment_date": row["payment_date"],
        "signed": bool(row["signed"]),
        "sent": bool(row["sent"]),
        "locked": bool(row["locked"]),
        "receipt_date": row["receipt_date"],
        "xml_available": bool(xml_path),
        "pdf_available": bool(pdf_path),
        "decree_available": bool(decree_path),
        "notes": row["notes"],
        "owner_username": row["owner_username"] if "owner_username" in row.keys() else "",
    }


def summary_from_rows(rows) -> dict:
    summary = {
        "count": len(rows),
        "taxable_amount": Decimal("0.00"),
        "total_amount": Decimal("0.00"),
        "payment_amount": Decimal("0.00"),
        "collected_amount": Decimal("0.00"),
        "outstanding_amount": Decimal("0.00"),
    }
    for row in rows:
        payment_amount = parse_decimal(row["payment_amount"])
        summary["taxable_amount"] += parse_decimal(row["taxable_amount"])
        summary["total_amount"] += parse_decimal(row["total_amount"])
        summary["payment_amount"] += payment_amount
        if (row["receipt_date"] or "").strip():
            summary["collected_amount"] += payment_amount
        else:
            summary["outstanding_amount"] += payment_amount

    return {
        "count": summary["count"],
        "taxable_amount": format_euro(summary["taxable_amount"]),
        "total_amount": format_euro(summary["total_amount"]),
        "payment_amount": format_euro(summary["payment_amount"]),
        "collected_amount": format_euro(summary["collected_amount"]),
        "outstanding_amount": format_euro(summary["outstanding_amount"]),
    }


def list_invoices(year: int, month: int | None, owner_user_id: int | None = None):
    if month is None:
        start_date = date(year, 1, 1).isoformat()
        end_date = date(year + 1, 1, 1).isoformat()
    else:
        start_date = date(year, month, 1).isoformat()
        if month == 12:
            end_date = date(year + 1, 1, 1).isoformat()
        else:
            end_date = date(year, month + 1, 1).isoformat()

    with get_connection() as conn:
        if owner_user_id is None:
            rows = conn.execute(
                """
                SELECT invoices.*, users.username AS owner_username
                FROM invoices
                LEFT JOIN users ON users.id = invoices.owner_user_id
                WHERE invoice_date >= ? AND invoice_date < ?
                """,
                (start_date, end_date),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT invoices.*, users.username AS owner_username
                FROM invoices
                LEFT JOIN users ON users.id = invoices.owner_user_id
                WHERE invoice_date >= ? AND invoice_date < ?
                  AND owner_user_id = ?
                """,
                (start_date, end_date, owner_user_id),
            ).fetchall()
    return sorted(rows, key=invoice_sort_key)


def parse_archive_filters(args) -> tuple[int, int | None, str]:
    today = date.today()
    month_raw = (args.get("month") or "").strip()
    year_raw = (args.get("year") or str(today.year)).strip()

    try:
        year = int(year_raw)
    except ValueError:
        year = today.year

    if month_raw in {"", "all"}:
        return year, None, ""

    try:
        month = int(month_raw)
        if month < 1 or month > 12:
            raise ValueError
        return year, month, str(month)
    except ValueError:
        return year, None, ""


def archive_export_filename(year: int, month: int | None) -> str:
    if month is None:
        return f"fatture_{year}_tutti-i-mesi.xlsx"
    return f"fatture_{year}_{month:02d}.xlsx"


def xlsx_column_name(index: int) -> str:
    name = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        name = chr(65 + remainder) + name
    return name


def xlsx_inline_string_cell(ref: str, value: str) -> str:
    return (
        f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">'
        f"{escape(value or '')}"
        f"</t></is></c>"
    )


def xlsx_number_cell(ref: str, value: Decimal) -> str:
    numeric = f"{quantize_money(value):.2f}"
    return f'<c r="{ref}"><v>{numeric}</v></c>'


def build_archive_xlsx(rows, year: int, month: int | None) -> bytes:
    headers = [
        "Origine",
        "Bloccata",
        "Numero",
        "Data",
        "R.G.",
        "Pagante",
        "Compenso",
        "Spese",
        "Totale documento",
        "Netto da incassare",
        "XML disponibile",
        "PDF disponibile",
        "Firma",
        "Invio",
        "Data incasso",
        "Note",
    ]
    column_types = [
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
        "number",
        "number",
        "number",
        "number",
        "string",
        "string",
        "string",
        "string",
        "string",
        "string",
    ]

    worksheet_rows = [[(header, "string") for header in headers]]
    for row in rows:
        row_values = [
            invoice_row_to_view(row)["source_label"],
            "Si" if row["locked"] else "No",
            row["invoice_number"],
            format_display_date(row["invoice_date"]),
            f"{row['rg_number']}/{row['rg_year']}"
            if row["rg_number"] and row["rg_year"]
            else "",
            row["payer"],
            parse_decimal(row["compenso"]),
            parse_decimal(row["expense_reimbursement"]),
            parse_decimal(row["total_amount"]),
            parse_decimal(row["payment_amount"]),
            "Si" if stored_path(row["xml_path"]) else "No",
            "Si" if stored_path(row["pdf_path"]) else "No",
            "Si" if row["signed"] else "No",
            "Si" if row["sent"] else "No",
            format_display_date(row["receipt_date"]),
            row["notes"] or "",
        ]
        worksheet_rows.append(list(zip(row_values, column_types)))

    row_xml = []
    for row_index, row_values in enumerate(worksheet_rows, start=1):
        cells = []
        for column_index, (value, kind) in enumerate(row_values, start=1):
            ref = f"{xlsx_column_name(column_index)}{row_index}"
            if kind == "number":
                cells.append(xlsx_number_cell(ref, value if isinstance(value, Decimal) else parse_decimal(value)))
            else:
                cells.append(xlsx_inline_string_cell(ref, str(value)))
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    last_column = xlsx_column_name(len(headers))
    last_row = len(worksheet_rows)
    worksheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:{last_column}{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        f'<sheetData>{"".join(row_xml)}</sheetData>'
        '</worksheet>'
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Archivio" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '</Relationships>'
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '</Types>'
    )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as xlsx:
        xlsx.writestr("[Content_Types].xml", content_types_xml)
        xlsx.writestr("_rels/.rels", rels_xml)
        xlsx.writestr("xl/workbook.xml", workbook_xml)
        xlsx.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        xlsx.writestr("xl/worksheets/sheet1.xml", worksheet_xml)
    buffer.seek(0)
    return buffer.getvalue()


def save_generated_invoice(record: dict) -> int:
    now = now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO invoices (
                source, owner_user_id, created_by_user_id, invoice_number, invoice_date, rg_number, rg_year, payer,
                compenso, expense_reimbursement, taxable_amount, vat_amount,
                withholding_amount, total_amount, payment_amount, payment_date,
                signed, sent, receipt_date, xml_path, pdf_path, decree_path,
                decree_source_filename, filename_base, notes, extraction_method,
                created_at, updated_at
            ) VALUES (
                :source, :owner_user_id, :created_by_user_id, :invoice_number, :invoice_date, :rg_number, :rg_year, :payer,
                :compenso, :expense_reimbursement, :taxable_amount, :vat_amount,
                :withholding_amount, :total_amount, :payment_amount, :payment_date,
                0, 0, '', :xml_path, :pdf_path, :decree_path,
                :decree_source_filename, :filename_base, :notes, :extraction_method,
                :created_at, :updated_at
            )
            ON CONFLICT(owner_user_id, invoice_number, invoice_date) DO UPDATE SET
                source = excluded.source,
                created_by_user_id = excluded.created_by_user_id,
                rg_number = excluded.rg_number,
                rg_year = excluded.rg_year,
                payer = excluded.payer,
                compenso = excluded.compenso,
                expense_reimbursement = excluded.expense_reimbursement,
                taxable_amount = excluded.taxable_amount,
                vat_amount = excluded.vat_amount,
                withholding_amount = excluded.withholding_amount,
                total_amount = excluded.total_amount,
                payment_amount = excluded.payment_amount,
                payment_date = excluded.payment_date,
                xml_path = excluded.xml_path,
                pdf_path = excluded.pdf_path,
                decree_path = excluded.decree_path,
                decree_source_filename = excluded.decree_source_filename,
                filename_base = excluded.filename_base,
                notes = excluded.notes,
                extraction_method = excluded.extraction_method,
                updated_at = excluded.updated_at
            """,
            {
                **record,
                "source": "system",
                "created_at": now,
                "updated_at": now,
                "notes": record.get("notes", ""),
            },
        )
        row = conn.execute(
            """
            SELECT id FROM invoices
            WHERE owner_user_id = ? AND invoice_number = ? AND invoice_date = ?
            """,
            (record["owner_user_id"], record["invoice_number"], record["invoice_date"]),
        ).fetchone()
        return row["id"]


def save_imported_invoice(record: dict) -> int:
    now = now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO invoices (
                source, owner_user_id, created_by_user_id, invoice_number, invoice_date, rg_number, rg_year, payer,
                compenso, expense_reimbursement, taxable_amount, vat_amount,
                withholding_amount, total_amount, payment_amount, payment_date,
                signed, sent, receipt_date, xml_path, pdf_path, decree_path,
                decree_source_filename, filename_base, notes, extraction_method,
                created_at, updated_at
            ) VALUES (
                'imported', :owner_user_id, :created_by_user_id, :invoice_number, :invoice_date, :rg_number, :rg_year, :payer,
                :compenso, :expense_reimbursement, :taxable_amount, :vat_amount,
                :withholding_amount, :total_amount, :payment_amount, :payment_date,
                0, 0, '', :xml_path, '', '',
                :decree_source_filename, :filename_base, :notes, 'xml_import',
                :created_at, :updated_at
            )
            ON CONFLICT(owner_user_id, invoice_number, invoice_date) DO UPDATE SET
                source = 'imported',
                created_by_user_id = excluded.created_by_user_id,
                rg_number = excluded.rg_number,
                rg_year = excluded.rg_year,
                payer = excluded.payer,
                compenso = excluded.compenso,
                expense_reimbursement = excluded.expense_reimbursement,
                taxable_amount = excluded.taxable_amount,
                vat_amount = excluded.vat_amount,
                withholding_amount = excluded.withholding_amount,
                total_amount = excluded.total_amount,
                payment_amount = excluded.payment_amount,
                payment_date = excluded.payment_date,
                xml_path = excluded.xml_path,
                decree_source_filename = excluded.decree_source_filename,
                filename_base = excluded.filename_base,
                notes = excluded.notes,
                extraction_method = excluded.extraction_method,
                updated_at = excluded.updated_at
            """,
            {
                **record,
                "created_at": now,
                "updated_at": now,
            },
        )
        row = conn.execute(
            """
            SELECT id FROM invoices
            WHERE owner_user_id = ? AND invoice_number = ? AND invoice_date = ?
            """,
            (record["owner_user_id"], record["invoice_number"], record["invoice_date"]),
        ).fetchone()
        return row["id"]


def note_for_imported_xml(filename: str) -> str:
    return f"Import XML: {original_import_filename(filename)}"


def imported_filename_from_row(row: sqlite3.Row) -> str:
    filename = original_import_filename(row["decree_source_filename"])
    if filename:
        return filename

    notes = (row["notes"] or "").strip()
    prefix = "Import XML:"
    if notes.lower().startswith(prefix.lower()):
        return original_import_filename(notes[len(prefix):].strip())
    return ""


def find_xml_import_duplicates(parsed_files: list[dict], owner_user_id: int) -> list[dict]:
    duplicates = []
    seen_numbers = {}
    seen_filenames = {}

    with get_connection() as conn:
        for item in parsed_files:
            parsed = item["parsed"]
            invoice_number = parsed["invoice_number"]
            filename = original_import_filename(item["filename"])
            note_value = note_for_imported_xml(filename)
            reasons = []
            matches = []

            number_matches = conn.execute(
                """
                SELECT id, invoice_number, invoice_date, decree_source_filename, notes
                FROM invoices
                WHERE invoice_number = ?
                  AND owner_user_id = ?
                ORDER BY invoice_date DESC, id DESC
                """,
                (invoice_number, owner_user_id),
            ).fetchall()
            if number_matches:
                reasons.append("numero fattura gia' presente")
                matches.extend(number_matches)

            file_matches = conn.execute(
                """
                SELECT id, invoice_number, invoice_date, decree_source_filename, notes
                FROM invoices
                WHERE owner_user_id = ?
                  AND (
                        lower(coalesce(decree_source_filename, '')) = lower(?)
                     OR lower(coalesce(notes, '')) = lower(?)
                  )
                ORDER BY invoice_date DESC, id DESC
                """,
                (owner_user_id, filename, note_value),
            ).fetchall()
            if file_matches:
                reasons.append("nome file gia' presente")
                matches.extend(file_matches)

            previous_number_file = seen_numbers.get(invoice_number)
            if previous_number_file:
                reasons.append(
                    f"numero fattura duplicato nella selezione ({previous_number_file})"
                )
            else:
                seen_numbers[invoice_number] = filename

            filename_key = filename.lower()
            previous_same_file = seen_filenames.get(filename_key)
            if previous_same_file:
                reasons.append(
                    f"nome file duplicato nella selezione ({previous_same_file})"
                )
            else:
                seen_filenames[filename_key] = filename

            if reasons:
                unique_matches = []
                seen_match_ids = set()
                for row in matches:
                    if row["id"] in seen_match_ids:
                        continue
                    seen_match_ids.add(row["id"])
                    unique_matches.append(
                        {
                            "invoice_id": row["id"],
                            "invoice_number": row["invoice_number"],
                            "invoice_date": row["invoice_date"],
                            "filename": imported_filename_from_row(row),
                        }
                    )
                duplicates.append(
                    {
                        "file": filename,
                        "invoice_number": invoice_number,
                        "invoice_date": parsed["invoice_date"],
                        "reasons": reasons,
                        "matches": unique_matches,
                    }
                )

    return duplicates


def redirect_period_for_imported_rows(imported: list[dict]) -> tuple[int, int] | tuple[None, None]:
    if not imported:
        return None, None

    latest_invoice_date = max(
        date.fromisoformat(item["invoice_date"])
        for item in imported
    )
    return latest_invoice_date.month, latest_invoice_date.year


def insert_manual_invoice(record: dict) -> int:
    now = now_iso()
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO invoices (
                    source, owner_user_id, created_by_user_id, invoice_number, invoice_date, rg_number, rg_year, payer,
                    compenso, expense_reimbursement, taxable_amount, vat_amount,
                    withholding_amount, total_amount, payment_amount, payment_date,
                    signed, sent, receipt_date, xml_path, pdf_path, decree_path,
                    decree_source_filename, filename_base, notes, extraction_method,
                    created_at, updated_at
                ) VALUES (
                    'manual', :owner_user_id, :created_by_user_id, :invoice_number, :invoice_date, :rg_number, :rg_year, :payer,
                    :compenso, :expense_reimbursement, :taxable_amount, :vat_amount,
                    :withholding_amount, :total_amount, :payment_amount, :payment_date,
                    0, 0, '', '', '', '',
                    '', :filename_base, :notes, 'manual',
                    :created_at, :updated_at
                )
                """,
                {
                    **record,
                    "created_at": now,
                    "updated_at": now,
                },
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("Esiste gia' una fattura con lo stesso numero e data per questo utente") from exc
        return cursor.lastrowid


def get_invoice_row(invoice_id: int) -> sqlite3.Row | None:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT invoices.*, users.username AS owner_username
            FROM invoices
            LEFT JOIN users ON users.id = invoices.owner_user_id
            WHERE invoices.id = ?
            """,
            (invoice_id,),
        ).fetchone()


def update_invoice_status(invoice_id: int, signed: int, sent: int, receipt_date: str) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT signed, sent, locked
            FROM invoices
            WHERE id = ?
            """,
            (invoice_id,),
        ).fetchone()
        if not row:
            raise ValueError("Fattura non trovata")

        if row["locked"] and (signed != row["signed"] or sent != row["sent"]):
            raise ValueError("Fattura bloccata: puoi aggiornare solo la data di incasso")

        conn.execute(
            """
            UPDATE invoices
            SET signed = ?, sent = ?, receipt_date = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                row["signed"] if row["locked"] else signed,
                row["sent"] if row["locked"] else sent,
                receipt_date,
                now_iso(),
                invoice_id,
            ),
        )
        updated = conn.execute(
            """
            SELECT signed, sent, locked, receipt_date
            FROM invoices
            WHERE id = ?
            """,
            (invoice_id,),
        ).fetchone()
    return {
        "signed": bool(updated["signed"]),
        "sent": bool(updated["sent"]),
        "locked": bool(updated["locked"]),
        "receipt_date": updated["receipt_date"],
    }


def lock_invoice(invoice_id: int) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT sent, locked, receipt_date
            FROM invoices
            WHERE id = ?
            """,
            (invoice_id,),
        ).fetchone()
        if not row:
            raise ValueError("Fattura non trovata")
        if row["locked"]:
            return {
                "locked": True,
                "sent": bool(row["sent"]),
                "receipt_date": row["receipt_date"],
            }
        if not row["sent"]:
            raise ValueError("Il blocco e' disponibile solo dopo avere confermato l'invio")

        conn.execute(
            """
            UPDATE invoices
            SET locked = 1, updated_at = ?
            WHERE id = ?
            """,
            (now_iso(), invoice_id),
        )
    return {
        "locked": True,
        "sent": True,
        "receipt_date": row["receipt_date"],
    }


def delete_invoice(invoice_id: int):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, locked, xml_path, pdf_path, decree_path
            FROM invoices
            WHERE id = ?
            """,
            (invoice_id,),
        ).fetchone()
        if not row:
            raise ValueError("Fattura non trovata")
        if row["locked"]:
            raise ValueError("La fattura bloccata non puo' essere eliminata")

        conn.execute(
            """
            DELETE FROM invoices
            WHERE id = ?
            """,
            (invoice_id,),
        )

    for raw_path in (row["xml_path"], row["pdf_path"], row["decree_path"]):
        path = stored_path(raw_path)
        if not path:
            continue
        try:
            path.unlink(missing_ok=True)
        except OSError:
            app.logger.warning("Impossibile eliminare il file associato alla fattura %s: %s", invoice_id, path)


def invoice_xml_path(invoice_id: int) -> Path | None:
    row = get_invoice_row(invoice_id)
    if not row:
        return None
    return stored_path(row["xml_path"])


def invoice_pdf_path(invoice_id: int) -> Path | None:
    row = get_invoice_row(invoice_id)
    if not row:
        return None
    return stored_path(row["pdf_path"])


def ensure_invoice_pdf(invoice_id: int) -> Path:
    row = get_invoice_row(invoice_id)
    if not row:
        raise ValueError("Fattura non trovata")

    existing_pdf = stored_path(row["pdf_path"])
    if existing_pdf:
        return existing_pdf

    xml_path = stored_path(row["xml_path"])
    if not xml_path:
        raise ValueError("PDF generabile solo per fatture con XML disponibile")

    xml_bytes = xml_path.read_bytes()
    preview_html = applica_xsl(xml_bytes)
    pdf_bytes = genera_pdf_fattura(preview_html)
    filename_base = row["filename_base"] or nome_file_base(row["invoice_number"], row["invoice_date"])
    pdf_path = PDF_OUTPUT_DIR / f"{filename_base}.pdf"
    save_bytes(pdf_path, pdf_bytes)

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE invoices
            SET pdf_path = ?, updated_at = ?
            WHERE id = ?
            """,
            (str(pdf_path), now_iso(), invoice_id),
        )
    return pdf_path


# ---------------------------------------------------------------------------
# Form parsing and validation
# ---------------------------------------------------------------------------

def read_invoice_form() -> dict:
    return {
        "numero_fattura": request.form.get("numero_fattura", "").strip(),
        "data_fattura": request.form.get("data_fattura", "").strip(),
        "numero_rg": request.form.get("numero_rg", "").strip(),
        "anno_rg": request.form.get("anno_rg", "").strip(),
        "compenso": request.form.get("compenso", "").strip(),
        "rimborsi_spese": request.form.get("rimborsi_spese", "").strip(),
        "pagante": request.form.get("pagante", "").strip(),
        "data_pagamento": request.form.get("data_pagamento", "").strip(),
        "extraction_method": request.form.get("extraction_method", "").strip() or "manual",
        "note": request.form.get("note", "").strip(),
    }


def validate_invoice_data(data: dict, require_rg=True) -> dict:
    numero_fattura = data["numero_fattura"].strip()
    if not numero_fattura:
        raise ValueError("Campo obbligatorio mancante: numero_fattura")

    data_fattura = ensure_iso_date(data["data_fattura"], "data_fattura")
    numero_rg = data["numero_rg"].strip()
    anno_rg = validate_year(data["anno_rg"], "anno_rg", required=require_rg)

    if require_rg and not numero_rg:
        raise ValueError("Campo obbligatorio mancante: numero_rg")

    compenso = parse_decimal(data.get("compenso"))
    rimborsi_spese = parse_decimal(data.get("rimborsi_spese"))
    if compenso < 0 or rimborsi_spese < 0:
        raise ValueError("Compenso e rimborsi spese non possono essere negativi")
    if compenso + rimborsi_spese <= 0:
        raise ValueError("Inserisci almeno un importo maggiore di zero")

    data_pagamento = ensure_iso_date(
        data.get("data_pagamento") or data_fattura,
        "data_pagamento",
        required=False,
    ) or data_fattura

    return {
        **data,
        "numero_fattura": numero_fattura,
        "data_fattura": data_fattura,
        "numero_rg": numero_rg,
        "anno_rg": anno_rg,
        "compenso": fmt(compenso),
        "rimborsi_spese": fmt(rimborsi_spese),
        "data_pagamento": data_pagamento,
        "pagante": (data.get("pagante") or "").strip(),
    }


def build_generated_record(data: dict, totals: dict, base: str, paths: dict) -> dict:
    return {
        "invoice_number": data["numero_fattura"],
        "invoice_date": data["data_fattura"],
        "rg_number": data["numero_rg"],
        "rg_year": data["anno_rg"],
        "payer": data["pagante"],
        "compenso": fmt(totals["compenso"]),
        "expense_reimbursement": fmt(totals["rimborsi_spese"]),
        "taxable_amount": fmt(totals["imponibile"]),
        "vat_amount": fmt(totals["iva"]),
        "withholding_amount": fmt(totals["ritenuta"]),
        "total_amount": fmt(totals["totale_documento"]),
        "payment_amount": fmt(totals["importo_pagamento"]),
        "payment_date": data["data_pagamento"],
        "xml_path": str(paths["xml_path"]),
        "pdf_path": str(paths["pdf_path"]) if paths["pdf_path"] else "",
        "decree_path": str(paths["decree_path"]),
        "decree_source_filename": paths["decree_source_filename"],
        "filename_base": base,
        "notes": data.get("note", ""),
        "extraction_method": data.get("extraction_method", "manual"),
    }


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

def sanitize_next_url(value: str) -> str:
    value = (value or "").strip()
    if not value.startswith("/") or value.startswith("//"):
        return url_for("index")
    return value


@app.route("/setup", methods=["GET", "POST"])
def setup_master():
    if users_exist():
        return redirect(url_for("login"))

    defaults = {
        "username": BOOTSTRAP_MASTER_USERNAME,
        "email": BOOTSTRAP_MASTER_EMAIL,
    }

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        password_confirm = request.form.get("password_confirm", "")

        if password != password_confirm:
            return render_template(
                "setup_master.html",
                defaults={"username": username, "email": email},
                error="Le password non coincidono.",
            )

        try:
            create_user_account(username, email, password, role="master", is_active=True)
        except ValueError as exc:
            return render_template(
                "setup_master.html",
                defaults={"username": username, "email": email},
                error=str(exc),
            )

        flash("Account master creato. Esegui il login.", "success")
        return redirect(url_for("login"))

    return render_template("setup_master.html", defaults=defaults, error="")


@app.route("/login", methods=["GET", "POST"])
def login():
    if not users_exist():
        return redirect(url_for("setup_master"))

    if current_user():
        return redirect(url_for("index"))

    next_url = sanitize_next_url(request.args.get("next", ""))
    if request.method == "POST":
        login_value = request.form.get("login", "").strip()
        password = request.form.get("password", "")
        user = get_user_by_login(login_value)

        if not user or not check_password_hash(user["password_hash"], password):
            return render_template(
                "login.html",
                error="Credenziali non valide.",
                next_url=next_url,
                login_value=login_value,
            )
        if not user["is_active"]:
            return render_template(
                "login.html",
                error="Account disattivato. Contatta il master.",
                next_url=next_url,
                login_value=login_value,
            )

        login_user(user)
        flash(f"Accesso effettuato come {user['username']}.", "success")
        return redirect(sanitize_next_url(request.form.get("next", next_url)))

    return render_template("login.html", error="", next_url=next_url, login_value="")


@app.route("/logout")
def logout():
    logout_user()
    flash("Sessione chiusa.", "success")
    return redirect(url_for("login"))


@app.route("/utenti")
@master_required
def utenti():
    user_rows = list_users()
    return render_template(
        "users.html",
        users=user_rows,
        api_quota_summaries=get_api_quota_summary_map(user_rows),
        quota_period_options=API_QUOTA_PERIOD_OPTIONS,
    )


@app.route("/utenti/crea", methods=["POST"])
@master_required
def crea_utente():
    permissions = {
        "can_import_xml_history": bool(request.form.get("can_import_xml_history")),
        "can_insert_archive_manual": bool(request.form.get("can_insert_archive_manual")),
        "can_manage_invoice_flags": bool(request.form.get("can_manage_invoice_flags")),
    }
    api_quota = normalize_api_quota_form(request.form, "user")
    try:
        create_user_account(
            request.form.get("username", ""),
            request.form.get("email", ""),
            request.form.get("password", ""),
            role="user",
            is_active=True,
            permissions=permissions,
            api_quota=api_quota,
        )
        flash("Utente creato correttamente.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("utenti"))


@app.route("/utenti/<int:user_id>/stato", methods=["POST"])
@master_required
def stato_utente(user_id: int):
    try:
        update_user_active(user_id, bool(request.form.get("is_active")))
        flash("Stato utente aggiornato.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("utenti"))


@app.route("/utenti/<int:user_id>/password", methods=["POST"])
@master_required
def password_utente(user_id: int):
    try:
        reset_user_password(user_id, request.form.get("password", ""))
        flash("Password aggiornata.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("utenti"))


@app.route("/utenti/<int:user_id>/permessi", methods=["POST"])
@master_required
def permessi_utente(user_id: int):
    permissions = {
        "can_import_xml_history": bool(request.form.get("can_import_xml_history")),
        "can_insert_archive_manual": bool(request.form.get("can_insert_archive_manual")),
        "can_manage_invoice_flags": bool(request.form.get("can_manage_invoice_flags")),
    }
    try:
        update_user_permissions(user_id, permissions)
        flash("Permessi aggiornati.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("utenti"))


@app.route("/utenti/<int:user_id>/quota", methods=["POST"])
@master_required
def quota_utente(user_id: int):
    try:
        update_user_api_quota(
            user_id,
            normalize_api_quota_form(request.form),
        )
        flash("Quota API aggiornata.", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("utenti"))


@app.route("/configurazione-fattura", methods=["GET", "POST"])
@master_required
def configurazione_fattura():
    user_rows = list_users()
    if not user_rows:
        flash("Nessun utente disponibile per la configurazione.", "error")
        return redirect(url_for("utenti"))

    user_lookup = {row["id"]: row for row in user_rows}
    selected_user_id = parse_int(request.values.get("user_id"))
    if selected_user_id not in user_lookup:
        selected_user_id = user_rows[0]["id"]

    if request.method == "POST":
        try:
            selected_user_id = parse_int(request.form.get("user_id"))
            if selected_user_id not in user_lookup:
                raise ValueError("Utente non valido")
            profile = validate_invoice_profile(
                normalize_invoice_profile_form(request.form)
            )
            update_invoice_profile(selected_user_id, profile)
            flash("Profilo fattura aggiornato.", "success")
            return redirect(url_for("configurazione_fattura", user_id=selected_user_id))
        except ValueError as exc:
            flash(str(exc), "error")

    profile = get_invoice_profile(selected_user_id)
    return render_template(
        "invoice_profile.html",
        users=user_rows,
        selected_user_id=selected_user_id,
        selected_user=user_lookup[selected_user_id],
        profile=profile,
        template_fields=sorted(INVOICE_PROFILE_TEMPLATE_FIELDS),
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    user = current_user()
    owner_choices = list_active_users(include_master=True) if is_master_user(user) else [user]
    owner_profile_defaults = build_owner_profile_defaults(owner_choices)
    owner_api_quota_summaries = get_api_quota_summary_map(owner_choices)
    selected_owner_id = user["id"]
    return render_template(
        "index.html",
        today=today_iso(),
        openai_model=OPENAI_MODEL,
        openai_configured=bool(OPENAI_API_KEY),
        suggested_invoice_number=get_suggested_invoice_number(user["id"]),
        owner_choices=owner_choices,
        selected_owner_id=selected_owner_id,
        suggested_invoice_numbers={str(row["id"]): get_suggested_invoice_number(row["id"]) for row in owner_choices},
        owner_profile_defaults=owner_profile_defaults,
        owner_api_quota_summaries=owner_api_quota_summaries,
        default_payer=owner_profile_defaults.get(str(selected_owner_id), {}).get("default_payer", "INPS"),
    )


@app.route("/fatture")
def elenco_fatture():
    today = date.today()
    year, month, selected_month = parse_archive_filters(request.args)
    user = current_user()
    owner_choices = list_active_users(include_master=True) if is_master_user(user) else [user]
    selected_owner_id = parse_int(request.args.get("owner_user_id"))
    if not is_master_user(user):
        selected_owner_id = user["id"]
    elif selected_owner_id and not any(row["id"] == selected_owner_id for row in owner_choices):
        selected_owner_id = None

    rows = list_invoices(year, month, selected_owner_id)
    invoices = [invoice_row_to_view(row) for row in rows]
    form_owner_id = selected_owner_id or user["id"]
    owner_profile_defaults = build_owner_profile_defaults(owner_choices)
    return render_template(
        "fatture.html",
        invoices=invoices,
        summary=summary_from_rows(rows),
        selected_month=selected_month,
        selected_year=year,
        month_options=ARCHIVE_MONTH_OPTIONS,
        year_options=list(range(today.year - 3, today.year + 3)),
        today=today_iso(),
        owner_choices=owner_choices,
        selected_owner_id=selected_owner_id,
        owner_profile_defaults=owner_profile_defaults,
        default_payer=owner_profile_defaults.get(str(form_owner_id), {}).get("default_payer", "INPS"),
    )


@app.route("/fatture/export.xlsx")
def esporta_fatture_xlsx():
    year, month, _ = parse_archive_filters(request.args)
    user = current_user()
    owner_filter = parse_int(request.args.get("owner_user_id")) if is_master_user(user) else user["id"]
    rows = list_invoices(year, month, owner_filter)
    workbook_bytes = build_archive_xlsx(rows, year, month)
    return send_file(
        io.BytesIO(workbook_bytes),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=archive_export_filename(year, month),
    )


@app.route("/healthz")
def healthz():
    storage_state = {
        "storage": STORAGE_DIR.exists(),
        "xml": XML_OUTPUT_DIR.exists(),
        "pdf": PDF_OUTPUT_DIR.exists(),
        "decreti": DECREE_OUTPUT_DIR.exists(),
        "logs": LOG_DIR.exists(),
    }

    try:
        with get_connection() as conn:
            conn.execute("SELECT 1").fetchone()
    except sqlite3.Error as exc:
        app.logger.exception("Healthcheck DB fallito")
        return jsonify(
            {
                "ok": False,
                "environment": APP_ENV,
                "database": "error",
                "storage": storage_state,
                "error": str(exc),
            }
        ), 503

    return jsonify(
        {
            "ok": True,
            "environment": APP_ENV,
            "database": "ok",
            "storage": storage_state,
        }
    )


@app.route("/estrai", methods=["POST"])
def estrai():
    pdf_file = request.files.get("pdf")
    if not pdf_file or pdf_file.filename == "":
        return jsonify({"errore": "Nessun file PDF"}), 400

    try:
        user = current_user()
        owner_user_id = resolve_owner_user_id(request.form.get("owner_user_id"))
        result = extract_decreto_data(
            pdf_file.read(),
            owner_user_id=owner_user_id,
            created_by_user_id=user["id"],
        )
    except Exception as exc:
        return jsonify({"errore": str(exc)}), 400

    return jsonify(result)


@app.route("/genera", methods=["POST"])
def genera():
    pdf_file = request.files.get("pdf")
    if not pdf_file or pdf_file.filename == "":
        return jsonify({"errore": "Nessun file PDF caricato"}), 400
    user = current_user()

    try:
        owner_user_id = resolve_owner_user_id(request.form.get("owner_user_id"))
        profile = get_invoice_profile(owner_user_id)
        form_data = read_invoice_form()
        form_data["pagante"] = form_data.get("pagante") or profile["default_payer"]
        data = validate_invoice_data(form_data, require_rg=True)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    pdf_bytes = pdf_file.read()
    pdf_filename = pdf_file.filename
    totals = calculate_totals(
        parse_decimal(data["compenso"]),
        parse_decimal(data["rimborsi_spese"]),
        vat_rate=profile["vat_rate"],
        withholding_rate=profile["withholding_rate"],
    )

    try:
        xml_bytes = genera_xml(data, pdf_bytes, pdf_filename, profile)
    except Exception as exc:
        return jsonify({"errore": f"Errore generazione XML: {exc}"}), 500

    try:
        preview_html = applica_xsl(xml_bytes)
    except Exception as exc:
        preview_html = f"<p>Anteprima non disponibile: {exc}</p>"

    fattura_pdf = b""
    pdf_b64 = ""
    pdf_error = ""
    try:
        fattura_pdf = genera_pdf_fattura(preview_html)
        pdf_b64 = base64.b64encode(fattura_pdf).decode("ascii")
    except Exception as exc:
        pdf_error = str(exc)

    base = nome_file_base(data["numero_fattura"], data["data_fattura"])
    xml_path = XML_OUTPUT_DIR / f"{base}.xml"
    decree_path = DECREE_OUTPUT_DIR / f"{base}__decreto.pdf"
    pdf_path = PDF_OUTPUT_DIR / f"{base}.pdf"

    save_bytes(xml_path, xml_bytes)
    save_bytes(decree_path, pdf_bytes)
    if fattura_pdf:
        save_bytes(pdf_path, fattura_pdf)

    record = build_generated_record(
        data,
        totals,
        base,
        {
            "xml_path": xml_path,
            "pdf_path": pdf_path if fattura_pdf else None,
            "decree_path": decree_path,
            "decree_source_filename": pdf_filename,
        },
    )
    record["owner_user_id"] = owner_user_id
    record["created_by_user_id"] = user["id"]
    invoice_id = save_generated_invoice(record)

    return jsonify(
        {
            "invoice_id": invoice_id,
            "filename_base": base,
            "xml_b64": base64.b64encode(xml_bytes).decode("ascii"),
            "pdf_b64": pdf_b64,
            "preview_html": preview_html,
            "storage_message": f"XML salvato automaticamente in {xml_path}",
            "pdf_warning": pdf_error,
            "suggested_invoice_number": get_suggested_invoice_number(owner_user_id),
        }
    )


@app.route("/pacchetto", methods=["POST"])
def pacchetto():
    pdf_file = request.files.get("pdf")
    if not pdf_file or pdf_file.filename == "":
        return jsonify({"errore": "Nessun file PDF caricato"}), 400

    try:
        owner_user_id = resolve_owner_user_id(request.form.get("owner_user_id"))
        profile = get_invoice_profile(owner_user_id)
        form_data = read_invoice_form()
        form_data["pagante"] = form_data.get("pagante") or profile["default_payer"]
        data = validate_invoice_data(form_data, require_rg=True)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    pdf_bytes = pdf_file.read()
    pdf_filename = pdf_file.filename

    try:
        xml_bytes = genera_xml(data, pdf_bytes, pdf_filename, profile)
        preview_html = applica_xsl(xml_bytes)
        fattura_pdf = genera_pdf_fattura(preview_html)
    except Exception as exc:
        return jsonify({"errore": f"Errore generazione: {exc}"}), 500

    base = nome_file_base(data["numero_fattura"], data["data_fattura"])

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{base}.xml", xml_bytes)
        zf.writestr(f"{base}.pdf", fattura_pdf)
        zf.writestr(pdf_filename, pdf_bytes)
    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{base}.zip",
    )


@app.route("/fatture/importa-xml", methods=["POST"])
def importa_xml_fatture():
    user = current_user()
    if not can_import_xml(user):
        return jsonify({"errore": "Non hai i permessi per importare XML storici"}), 403

    files = request.files.getlist("xml_files")
    valid_files = [file for file in files if file and file.filename]
    if not valid_files:
        return jsonify({"errore": "Seleziona almeno un file XML"}), 400
    try:
        owner_user_id = resolve_owner_user_id(request.form.get("owner_user_id"))
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    errors = []
    parsed_files = []
    force_import = is_truthy(request.form.get("force", "0"))

    for file in valid_files:
        filename = original_import_filename(file.filename)
        try:
            xml_bytes = file.read()
            parsed = parse_invoice_xml(xml_bytes, filename)
            parsed_files.append(
                {
                    "filename": filename,
                    "xml_bytes": xml_bytes,
                    "parsed": parsed,
                }
            )
        except Exception as exc:
            errors.append({"file": filename, "errore": str(exc)})

    duplicates = find_xml_import_duplicates(parsed_files, owner_user_id)
    if duplicates and not force_import:
        return (
            jsonify(
                {
                    "errore": "Sono stati trovati XML gia' presenti in archivio. Conferma per forzare l'importazione.",
                    "requires_force": True,
                    "duplicates": duplicates,
                    "error_count": len(errors),
                    "errors": errors,
                }
            ),
            409,
        )

    imported = []
    for item in parsed_files:
        try:
            parsed = item["parsed"]
            filename_base = nome_file_base(parsed["invoice_number"], parsed["invoice_date"])
            xml_path = XML_OUTPUT_DIR / f"{filename_base}.xml"
            save_bytes(xml_path, item["xml_bytes"])

            invoice_id = save_imported_invoice(
                {
                    **parsed,
                    "owner_user_id": owner_user_id,
                    "created_by_user_id": user["id"],
                    "xml_path": str(xml_path),
                    "filename_base": filename_base,
                    "decree_source_filename": parsed["original_filename"],
                    "notes": note_for_imported_xml(parsed["original_filename"]),
                }
            )
            imported.append(
                {
                    "invoice_id": invoice_id,
                    "invoice_number": parsed["invoice_number"],
                    "invoice_date": parsed["invoice_date"],
                }
            )
        except Exception as exc:
            errors.append({"file": item["filename"], "errore": str(exc)})

    if not imported and errors:
        return jsonify({"errore": "Nessun XML importato", "dettagli": errors}), 400

    redirect_month, redirect_year = redirect_period_for_imported_rows(imported)
    return jsonify(
        {
            "ok": True,
            "imported_count": len(imported),
            "error_count": len(errors),
            "imported": imported,
            "errors": errors,
            "duplicates_forced": bool(duplicates and force_import),
            "redirect_month": redirect_month,
            "redirect_year": redirect_year,
            "redirect_owner_user_id": owner_user_id,
            "suggested_invoice_number": get_suggested_invoice_number(owner_user_id),
        }
    )


@app.route("/fatture/manuale", methods=["POST"])
def aggiungi_fattura_manuale():
    user = current_user()
    if not can_insert_archive_manual(user):
        return jsonify({"errore": "Non hai i permessi per inserire fatture manuali in archivio"}), 403

    payload = request.get_json(silent=True) or request.form
    try:
        owner_user_id = resolve_owner_user_id(payload.get("owner_user_id"))
        profile = get_invoice_profile(owner_user_id)
        raw_data = {
            "numero_fattura": (payload.get("numero_fattura") or "").strip(),
            "data_fattura": (payload.get("data_fattura") or "").strip(),
            "numero_rg": (payload.get("numero_rg") or "").strip(),
            "anno_rg": (payload.get("anno_rg") or "").strip(),
            "compenso": (payload.get("compenso") or "").strip(),
            "rimborsi_spese": (payload.get("rimborsi_spese") or "").strip(),
            "pagante": (payload.get("pagante") or "").strip() or profile["default_payer"],
            "data_pagamento": (payload.get("data_pagamento") or "").strip(),
            "note": (payload.get("note") or "").strip(),
        }
        data = validate_invoice_data(raw_data, require_rg=False)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    totals = calculate_totals(
        parse_decimal(data["compenso"]),
        parse_decimal(data["rimborsi_spese"]),
        vat_rate=profile["vat_rate"],
        withholding_rate=profile["withholding_rate"],
    )
    record = {
        "owner_user_id": owner_user_id,
        "created_by_user_id": user["id"],
        "invoice_number": data["numero_fattura"],
        "invoice_date": data["data_fattura"],
        "rg_number": data["numero_rg"],
        "rg_year": data["anno_rg"],
        "payer": data["pagante"],
        "compenso": fmt(totals["compenso"]),
        "expense_reimbursement": fmt(totals["rimborsi_spese"]),
        "taxable_amount": fmt(totals["imponibile"]),
        "vat_amount": fmt(totals["iva"]),
        "withholding_amount": fmt(totals["ritenuta"]),
        "total_amount": fmt(totals["totale_documento"]),
        "payment_amount": fmt(totals["importo_pagamento"]),
        "payment_date": data["data_pagamento"],
        "filename_base": nome_file_base(data["numero_fattura"], data["data_fattura"]),
        "notes": data.get("note", ""),
    }

    try:
        invoice_id = insert_manual_invoice(record)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    invoice_date = date.fromisoformat(data["data_fattura"])
    return jsonify(
        {
            "ok": True,
            "invoice_id": invoice_id,
            "suggested_invoice_number": get_suggested_invoice_number(owner_user_id),
            "redirect_month": invoice_date.month,
            "redirect_year": invoice_date.year,
            "redirect_owner_user_id": owner_user_id,
        }
    )


@app.route("/fatture/<int:invoice_id>/stato", methods=["POST"])
def aggiorna_stato_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404

    payload = request.get_json(silent=True) or request.form
    try:
        signed = parse_bool(payload.get("firma"))
        sent = parse_bool(payload.get("invio"))
        receipt_date = ensure_iso_date(payload.get("data_incasso", ""), "data_incasso", required=False)
        if not can_manage_invoice_flags(current_user()):
            if signed != row["signed"] or sent != row["sent"]:
                raise ValueError("Non hai i permessi per modificare firma o invio")
            signed = row["signed"]
            sent = row["sent"]
        updated = update_invoice_status(invoice_id, signed, sent, receipt_date)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    return jsonify({"ok": True, **updated})


@app.route("/fatture/<int:invoice_id>/blocco", methods=["POST"])
def blocca_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404
    if not can_manage_invoice_flags(current_user()):
        return jsonify({"errore": "Non hai i permessi per bloccare la fattura"}), 403
    try:
        updated = lock_invoice(invoice_id)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    return jsonify({"ok": True, **updated})


@app.route("/fatture/<int:invoice_id>/elimina", methods=["POST"])
def elimina_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404

    try:
        delete_invoice(invoice_id)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400

    return jsonify({"ok": True})


@app.route("/fatture/<int:invoice_id>/xml")
def scarica_xml_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404
    path = stored_path(row["xml_path"])
    if not path:
        return jsonify({"errore": "XML non disponibile"}), 404
    return send_file(path, as_attachment=True, download_name=path.name, mimetype="application/xml")


@app.route("/fatture/<int:invoice_id>/pdf")
def scarica_pdf_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404
    try:
        path = ensure_invoice_pdf(invoice_id)
    except ValueError as exc:
        return jsonify({"errore": str(exc)}), 400
    except Exception as exc:
        return jsonify({"errore": f"Errore generazione PDF: {exc}"}), 500

    return send_file(path, as_attachment=True, download_name=path.name, mimetype="application/pdf")


@app.route("/fatture/<int:invoice_id>/decreto")
def scarica_decreto_fattura(invoice_id: int):
    row = get_accessible_invoice_row(invoice_id)
    if not row:
        return jsonify({"errore": "Fattura non trovata"}), 404
    path = stored_path(row["decree_path"])
    if not path:
        return jsonify({"errore": "Decreto non disponibile"}), 404
    return send_file(path, as_attachment=True, download_name=path.name, mimetype="application/pdf")


if __name__ == "__main__":
    app.run(debug=APP_DEBUG, host=APP_HOST, port=APP_PORT)
