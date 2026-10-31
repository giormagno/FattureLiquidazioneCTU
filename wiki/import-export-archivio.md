---
title: Import XML, Inserimento Manuale ed Export
area: archive-io
source_files:
  - app.py
  - templates/fatture.html
related:
  - archivio-fatture.md
  - generazione-fattura.md
---

# Import XML, Inserimento Manuale ed Export

## Import XML storico

Route: `POST /fatture/importa-xml`

Caratteristiche:
- permesso richiesto: `can_import_xml_history` (o master)
- parser XML e validazione campi
- controllo duplicati con possibilità di `force`
- salvataggio XML su storage e record in tabella `invoices`

## Inserimento manuale

Route: `POST /fatture/manuale`

Caratteristiche:
- permesso richiesto: `can_insert_archive_manual` (o master)
- validazione base dati fattura
- calcolo importi come nella generazione
- inserimento record senza XML/PDF obbligatori

## Export XLSX

Route: `GET /fatture/export.xlsx`

Caratteristiche:
- rispetta filtri periodo e owner
- crea workbook in memoria con colonne operative (stato documenti, importi, flags)

## Correlati

- [Archivio Fatture](./archivio-fatture.md)
- [Generazione Fattura](./generazione-fattura.md)
