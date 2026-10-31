---
title: Generazione Fattura da Decreto
area: invoice-generation
source_files:
  - app.py
  - templates/index.html
  - FoglioStileAssoSoftware.xsl
related:
  - profili-fattura.md
  - storage-e-documenti.md
  - archivio-fatture.md
---

# Generazione Fattura da Decreto

## Pagina principale

`GET /` (`templates/index.html`): upload decreto PDF, compilazione dati, preview, download XML/PDF/ZIP.

## API operative

- `POST /estrai`: estrae dati dal decreto (OpenAI o fallback locale).
- `POST /genera`: genera XML, preview HTML (XSL), PDF, salva su storage e crea record archivio. Rifiuta con `409` un numero fattura gia' usato dallo stesso utente nello stesso anno, o un nome file gia' presente (`find_invoice_number_conflict`), prima di scrivere qualunque file.
- `GET /fatture/<id>/pacchetto`: ZIP con XML, PDF e decreto **salvati** della fattura. Non usa i dati del modulo: dopo "Genera" il campo numero avanza gia' al successivo.

## Pipeline sintetica

1. Validazione form e owner user.
2. Calcolo totali (`calculate_totals`).
3. Generazione XML (`genera_xml`).
4. Trasformazione anteprima (`applica_xsl`).
5. Generazione PDF fattura (`genera_pdf_fattura`).
6. Persistenza file e inserimento record invoice.

Nella pagina, dopo una generazione riuscita il pulsante "Genera" resta disabilitato finche' non si carica un nuovo decreto: un secondo clic creerebbe la fattura successiva con lo stesso decreto. Per correggere una fattura si usa la modifica dall'archivio.

## Rigenerazione dopo modifica archivio

Quando una fattura non bloccata viene modificata dalla pagina dedicata (`/fatture/<id>/modifica`):
- i dati fattura vengono aggiornati a DB, con lo stesso controllo sui numeri gia' usati di `/genera`;
- se esiste un decreto PDF associato, XML e PDF fattura vengono rigenerati con i nuovi dati;
- l'allegato decreto non viene alterato: restano invariati bytes e filename sorgente del decreto originale.

## Correlati

- [Profili Fattura](./profili-fattura.md)
- [Storage e Documenti](./storage-e-documenti.md)
- [Archivio Fatture](./archivio-fatture.md)
