# LOG

## 2026-05-04

- Added mandatory documentation workflow in `CLAUDE.md` and `AGENTS.md`: after every change, update `wiki/` (including add/remove pages when needed) and append a log entry to `LOG.md`.
- Updated `wiki/README.md` with the same post-change documentation and logging requirement.
- Reworked invoice editing flow: `Modifica` in Archivio now opens a dedicated edit page for non-locked invoices (`/fatture/<id>/modifica`) with full invoice-field updates and server-side totals recalculation.
- Updated invoice edit behavior: after saving a non-locked invoice, XML/PDF are regenerated from updated data when a decree is available, while preserving the original decree attachment bytes and filename.
