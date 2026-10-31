---
title: Setup, Ambiente e Deploy Docker
area: operations
source_files:
  - README.md
  - docker-compose.yml
  - Dockerfile
  - secrets/app.env.example
related:
  - architettura.md
  - scheduler-e-job.md
---

# Setup, Ambiente e Deploy Docker

## Servizi Docker

- `app`: espone Flask/Waitress su `18743`, monta `storage`, `logs`, `.env`.
- `jobs`: stesso image, avvia `scripts/run_daily_jobs_daemon.py`.

## Persistenza

- `./storage:/app/storage`
- `./logs:/app/logs`
- `./secrets/app.env:/app/.env:ro`

## Healthcheck

- Endpoint: `GET /healthz`
- Verifica DB e presenza directory storage.

## Quando fare rebuild

Usare `docker compose up -d --build` quando cambiano:
- `Dockerfile`
- dipendenze (`requirements.txt`)
- file copiati in immagine durante build

Altrimenti basta `docker compose up -d`.

## Correlati

- [Architettura](./architettura.md)
- [Scheduler e Job](./scheduler-e-job.md)
