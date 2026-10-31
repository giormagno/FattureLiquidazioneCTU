---
title: Profili Fattura per Utente
area: invoicing-profile
source_files:
  - app.py
  - templates/invoice_profile.html
related:
  - gestione-utenti-e-quote-api.md
  - generazione-fattura.md
---

# Profili Fattura per Utente

## Scopo

Ogni utente ha un profilo con dati fiscali/anagrafici/template usato per generare XML/PDF.

## UI e route

- Pagina: `GET /configurazione-fattura?user_id=...` (solo master)
- Salvataggio: `POST /configurazione-fattura`

## Dati gestiti

- trasmittente, cedente/prestatore, cliente
- metodi di pagamento e IBAN
- aliquote IVA/ritenuta
- template testuali con placeholder (`{numero_rg}`, `{anno_rg}`, `{pagante}`, ...)

## Impatto funzionale

Le modifiche al profilo valgono per nuove fatture generate/importate manualmente, non alterano retroattivamente i record storici già salvati.

## Correlati

- [Gestione Utenti e Quote API](./gestione-utenti-e-quote-api.md)
- [Generazione Fattura](./generazione-fattura.md)
