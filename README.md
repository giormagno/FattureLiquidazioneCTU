## Deploy Docker

Questo pacchetto contiene solo i file minimi per pubblicare l'app con Docker su Ubuntu 24.

### Contenuto

- `Dockerfile`: build dell'immagine applicativa
- `docker-compose.yml`: avvio dei container `app` e `jobs`
- `app.py`: applicazione Flask
- `templates/`: viste HTML
- `FoglioStileAssoSoftware.xsl`: stylesheet usato per la fattura
- `requirements.txt`: dipendenze Python
- `ops/serve.py`: avvio Waitress
- `scripts/run_daily_jobs.py`: job giornalieri
- `scripts/run_daily_jobs_daemon.py`: scheduler del container `jobs`
- `secrets/app.env.example`: esempio di configurazione segreta

### Prerequisiti

- Ubuntu Server 24
- Docker Engine
- Docker Compose plugin
- Cloudflare Tunnel gia' installato e funzionante sul server host

### Installazione Docker su Ubuntu 24

Se Docker non e' gia' installato:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable docker
sudo systemctl start docker
```

### Preparazione cartella

Copia questo pacchetto in una cartella del server, ad esempio:

```bash
mkdir -p /opt/fe-bitetto
cd /opt/fe-bitetto
```

Estrai il contenuto del pacchetto in quella cartella.

### Preparazione directory persistenti

Nella cartella del progetto crea:

```bash
mkdir -p storage logs secrets
chmod 700 secrets
cp secrets/app.env.example secrets/app.env
chmod 600 secrets/app.env
```

### Configurazione `secrets/app.env`

Compila almeno questi valori:

```env
APP_BASE_URL=https://fatture.tuodominio.it
APP_ENV=production
APP_DEBUG=0
APP_HOST=0.0.0.0
APP_PORT=18743
APP_SECRET_KEY=metti-qui-una-chiave-lunga-casuale
APP_STORAGE_DIR=/app/storage
APP_LOG_DIR=/app/logs
WAITRESS_HOST=0.0.0.0
WAITRESS_PORT=18743
WAITRESS_THREADS=4
WAITRESS_CONNECTION_LIMIT=100
OPENAI_MODEL=gpt-5.4-mini
OPENAI_API_KEY=
RESEND_API_KEY=
MAIL_FROM=
MAIL_REPLY_TO=
JOB_TIMEZONE=Europe/Rome
JOB_DAEMON_RUN_AT=20:00
JOB_DAEMON_POLL_SECONDS=60
PLAYWRIGHT_NO_SANDBOX=1
PLAYWRIGHT_DISABLE_DEV_SHM_USAGE=1
```

### Avvio

Dalla root del pacchetto:

```bash
docker compose up -d --build
```

Controlli utili:

```bash
docker compose ps
docker compose logs -f app
docker compose logs -f jobs
curl http://127.0.0.1:18743/healthz
```

### Cloudflare Tunnel

Il tunnel gira fuori da Docker. Deve puntare a:

```text
http://127.0.0.1:18743
```

### Aggiornamento

Se aggiorni questi file, dalla stessa cartella esegui:

```bash
docker compose up -d --build
```

### Stop

```bash
docker compose down
```

