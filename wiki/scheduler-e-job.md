---
title: Scheduler e Job Giornalieri
area: background-jobs
source_files:
  - scripts/run_daily_jobs.py
  - scripts/run_daily_jobs_daemon.py
  - app.py
related:
  - setup-e-deploy.md
  - architettura.md
---

# Scheduler e Job Giornalieri

## Esecuzione

- Container `jobs` avvia `run_daily_jobs_daemon.py`.
- Lo scheduler legge timezone/orario da env e lancia `run_daily_jobs.py` una volta al giorno.

## Configurazioni env

- `JOB_TIMEZONE` (default `Europe/Rome`)
- `JOB_DAEMON_RUN_AT` (default `20:00`)
- `JOB_DAEMON_POLL_SECONDS` (default `60`, minimo effettivo `15`)

## Job presenti

- `daily_invoice_summary`: conta fatture con invio attivo.
- `daily_payment_summary`: conta fatture con data incasso valorizzata.

Stato persistenza job in tabella `scheduled_jobs`.

## Correlati

- [Setup e Deploy](./setup-e-deploy.md)
- [Architettura](./architettura.md)
