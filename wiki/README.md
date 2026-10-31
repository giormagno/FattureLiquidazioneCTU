---
title: Wiki Applicazione Fatture Liquidazione CTU
area: documentation
source_files:
  - app.py
  - templates/
  - docker-compose.yml
  - scripts/
related:
  - architettura.md
  - mappa-route-api.md
  - archivio-fatture.md
---

# Wiki Applicazione Fatture Liquidazione CTU

Questa cartella contiene la memoria funzionale stabile del progetto per fare modifiche mirate senza rileggere tutto il codice.

## Indice

- [Architettura e Componenti](./architettura.md)
- [Setup, Ambiente e Deploy Docker](./setup-e-deploy.md)
- [Autenticazione e Autorizzazioni](./autenticazione-e-permessi.md)
- [Gestione Utenti e Quote API](./gestione-utenti-e-quote-api.md)
- [Profili Fattura per Utente](./profili-fattura.md)
- [Generazione Fattura da Decreto](./generazione-fattura.md)
- [Archivio Fatture](./archivio-fatture.md)
- [Import XML, Inserimento Manuale ed Export](./import-export-archivio.md)
- [Storage File e Lifecycle Documenti](./storage-e-documenti.md)
- [Scheduler e Job Giornalieri](./scheduler-e-job.md)
- [Mappa Route HTTP/API](./mappa-route-api.md)

## Come usare la wiki per cambiare il codice

1. Parti dalla pagina funzionale interessata.
2. Segui i link "Correlati" per impatti trasversali.
3. Vai ai file sorgente indicati nel front matter `source_files`.
4. Applica la modifica solo nelle aree collegate.
5. A modifica completata, aggiorna i file wiki impattati (aggiunta/rimozione pagine se necessario) e registra la modifica in `LOG.md`.
