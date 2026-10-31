---
title: Archivio Fatture
area: invoice-archive
source_files:
  - app.py
  - templates/fatture.html
related:
  - import-export-archivio.md
  - storage-e-documenti.md
  - mappa-route-api.md
---

# Archivio Fatture

## Pagina

- `GET /fatture` con filtri anno/mese/owner.
- Vista desktop tabellare + vista mobile card.

## Funzioni principali

- stato fattura: firma/invio/data incasso
- modifica completa su pagina dedicata per fatture non bloccate
- blocco fattura
- eliminazione fattura
- download XML/PDF/Decreto
- upload/sostituzione decreto
- export XLSX

## Regole di business chiave

- Fattura bloccata:
  - non eliminabile
  - firma/invio non modificabili
  - data incasso ancora modificabile
- Blocco consentito solo se `sent = true` e permesso adeguato.
- Accesso ai record filtrato per owner (utente normale vede solo le proprie).

## Modifica dati fattura

Per le fatture non bloccate il pulsante `Modifica` apre la pagina dedicata:
- `GET /fatture/<invoice_id>/modifica`
- `POST /fatture/<invoice_id>/modifica`

Campi aggiornabili:
- numero e data fattura
- riferimenti R.G.
- cliente/pagante
- compenso e rimborsi (con ricalcolo imponibile/IVA/ritenuta/totali)
- data pagamento
- data incasso
- note

Effetto documenti:
- se la fattura ha un decreto PDF associato, al salvataggio vengono rigenerati XML e PDF fattura;
- il decreto allegato resta invariato (non viene riscritto ne' alterato).

## Correlati

- [Import/Export Archivio](./import-export-archivio.md)
- [Storage e Documenti](./storage-e-documenti.md)
- [Mappa Route API](./mappa-route-api.md)
