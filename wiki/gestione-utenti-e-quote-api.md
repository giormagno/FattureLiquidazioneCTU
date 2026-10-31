---
title: Gestione Utenti e Quote API
area: administration
source_files:
  - app.py
  - templates/users.html
related:
  - autenticazione-e-permessi.md
  - profili-fattura.md
  - generazione-fattura.md
---

# Gestione Utenti e Quote API

## Area amministrativa

Pagina: `GET /utenti` (solo master).

Operazioni:
- creazione utente (`POST /utenti/crea`)
- attivazione/disattivazione (`POST /utenti/<id>/stato`)
- reset password (`POST /utenti/<id>/password`)
- update permessi (`POST /utenti/<id>/permessi`)
- update quota API (`POST /utenti/<id>/quota`)

## Vincoli importanti

- Un solo master consentito.
- Master non disattivabile.
- Permessi extra non applicabili al master.

## Quota API

Tabelle/funzioni:
- `user_api_quotas`
- `usage_events`
- `get_api_quota_summary_map`
- `record_api_usage_event`

Uso: monitorare e limitare le chiamate di estrazione AI per utente/finestra rolling.

## Correlati

- [Autenticazione e Permessi](./autenticazione-e-permessi.md)
- [Profili Fattura](./profili-fattura.md)
- [Generazione Fattura](./generazione-fattura.md)
