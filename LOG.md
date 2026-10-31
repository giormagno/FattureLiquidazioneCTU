# LOG

## 2026-05-04

- Added mandatory documentation workflow in `CLAUDE.md` and `AGENTS.md`: after every change, update `wiki/` (including add/remove pages when needed) and append a log entry to `LOG.md`.
- Updated `wiki/README.md` with the same post-change documentation and logging requirement.
- Reworked invoice editing flow: `Modifica` in Archivio now opens a dedicated edit page for non-locked invoices (`/fatture/<id>/modifica`) with full invoice-field updates and server-side totals recalculation.
- Updated invoice edit behavior: after saving a non-locked invoice, XML/PDF are regenerated from updated data when a decree is available, while preserving the original decree attachment bytes and filename.

## 2026-10-31

- Fixed duplicate invoice numbers: `/genera` now rejects (409) a number already used by the same user in the same year, or a filename that would overwrite existing files, before writing anything (previously it silently upserted the existing invoice). Same check on archive edit.
- ZIP download now uses the saved invoice (`GET /fatture/<id>/pacchetto`); removed `POST /pacchetto`, which rebuilt the invoice from the form after the number had already advanced.
- `index.html`: after a successful generation, "Genera" stays disabled until a new decree is loaded.
- Updated `wiki/generazione-fattura.md` and `wiki/mappa-route-api.md`.
