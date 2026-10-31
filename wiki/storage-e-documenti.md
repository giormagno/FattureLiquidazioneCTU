---
title: Storage File e Lifecycle Documenti
area: storage
source_files:
  - app.py
  - docker-compose.yml
related:
  - generazione-fattura.md
  - archivio-fatture.md
  - setup-e-deploy.md
---

# Storage File e Lifecycle Documenti

## Directory

- XML: `storage/xml`
- PDF fatture: `storage/pdf`
- PDF decreti: `storage/decreti`
- DB SQLite: `storage/fatture.db`

## Lifecycle

- Generazione nuova fattura: salva XML + decreto + eventuale PDF.
- Import XML: salva XML e crea record importato.
- Upload decreto da archivio: aggiorna `decree_path` e filename sorgente.
- Eliminazione fattura non bloccata: rimuove record DB e tenta unlink file associati.

## Note implementative

- I path sono salvati in DB e validati con `stored_path`.
- `ensure_invoice_pdf` consente generazione lazy del PDF se manca ma XML disponibile.

## Correlati

- [Generazione Fattura](./generazione-fattura.md)
- [Archivio Fatture](./archivio-fatture.md)
- [Setup e Deploy](./setup-e-deploy.md)
