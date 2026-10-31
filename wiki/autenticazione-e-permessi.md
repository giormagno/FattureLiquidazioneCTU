---
title: Autenticazione e Autorizzazioni
area: security
source_files:
  - app.py
  - templates/login.html
  - templates/base.html
related:
  - gestione-utenti-e-quote-api.md
  - mappa-route-api.md
---

# Autenticazione e Autorizzazioni

## Flusso auth

- Bootstrap iniziale: `GET/POST /setup` crea account master se non esistono utenti.
- Login: `GET/POST /login` con username o email.
- Logout: `GET /logout`.
- Sessione: `session["user_id"]`.

## Decoratori e guardie

- `login_required`: blocca accesso se non autenticato.
- `master_required`: consente solo master.
- `should_bypass_auth`: eccezioni (`/healthz`, `/login`, `/setup`, `/logout`, static).

## Permessi dominio fatture

Funzioni chiave:
- `can_import_xml`
- `can_insert_archive_manual`
- `can_manage_invoice_flags`

Regola: master ha sempre accesso; utente normale dipende dai flag su `user_permissions`.

## Correlati

- [Gestione Utenti e Quote API](./gestione-utenti-e-quote-api.md)
- [Mappa Route API](./mappa-route-api.md)
