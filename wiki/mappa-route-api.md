---
title: Mappa Route HTTP/API
area: api-map
source_files:
  - app.py
related:
  - archivio-fatture.md
  - generazione-fattura.md
  - autenticazione-e-permessi.md
---

# Mappa Route HTTP/API

## Setup/Auth

- `GET|POST /setup`
- `GET|POST /login`
- `GET /logout`

## Utenti (solo master)

- `GET /utenti`
- `POST /utenti/crea`
- `POST /utenti/<user_id>/stato`
- `POST /utenti/<user_id>/password`
- `POST /utenti/<user_id>/permessi`
- `POST /utenti/<user_id>/quota`

## Profili fattura (solo master)

- `GET|POST /configurazione-fattura`

## Nuova fattura

- `GET /`
- `POST /estrai`
- `POST /genera`
- `GET /fatture/<invoice_id>/pacchetto`

## Archivio

- `GET /fatture`
- `GET /fatture/export.xlsx`
- `POST /fatture/importa-xml`
- `POST /fatture/manuale`
- `GET /fatture/<invoice_id>/modifica`
- `POST /fatture/<invoice_id>/modifica`
- `POST /fatture/<invoice_id>/stato`
- `POST /fatture/<invoice_id>/blocco`
- `POST /fatture/<invoice_id>/elimina`
- `GET /fatture/<invoice_id>/xml`
- `GET /fatture/<invoice_id>/pdf`
- `GET /fatture/<invoice_id>/decreto`
- `POST /fatture/<invoice_id>/decreto`

## Sistema

- `GET /healthz`

## Correlati

- [Archivio Fatture](./archivio-fatture.md)
- [Generazione Fattura](./generazione-fattura.md)
- [Autenticazione e Permessi](./autenticazione-e-permessi.md)
