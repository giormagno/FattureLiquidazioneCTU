---
title: Architettura e Componenti
area: architecture
source_files:
  - app.py
  - templates/base.html
  - docker-compose.yml
related:
  - setup-e-deploy.md
  - mappa-route-api.md
  - storage-e-documenti.md
---

# Architettura e Componenti

## Stack

- Backend: Flask monolitico in `app.py`.
- Frontend: template Jinja in `templates/` con JS inline per interazioni.
- DB: SQLite in `storage/fatture.db`.
- File documenti: XML/PDF/Decreti su filesystem (`storage/`).
- Runtime: Docker Compose con due servizi (`app`, `jobs`).

## Componenti principali

- `app.py`: contiene bootstrap, schema/migrazioni, logica dominio, route HTTP.
- `templates/base.html`: layout condiviso, navbar, stile comune.
- `templates/index.html`: nuova fattura.
- `templates/fatture.html`: archivio fatture e azioni operative.
- `templates/users.html`: amministrazione utenti/quota/permessi.
- `templates/invoice_profile.html`: configurazione profilo XML/PDF per utente.
- `scripts/run_daily_jobs.py`: esecuzione job giornalieri.
- `scripts/run_daily_jobs_daemon.py`: scheduler continuo nel container `jobs`.

## Pattern operativo

- Il browser chiama route JSON/POST per update stato e operazioni archivio.
- Le route validano permessi con utente in sessione.
- I documenti vengono salvati fisicamente e il path persistito in DB.

## Correlati

- [Setup e Deploy](./setup-e-deploy.md)
- [Storage e Documenti](./storage-e-documenti.md)
- [Mappa Route API](./mappa-route-api.md)
