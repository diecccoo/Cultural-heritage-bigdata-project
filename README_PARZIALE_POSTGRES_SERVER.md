# PostgreSQL + pgAdmin – Accesso alla GUI

Questa configurazione Docker avvia un server PostgreSQL (`heritage`) e, opzionalmente, un'interfaccia grafica pgAdmin accessibile da browser.

## 📦 Composizione dei servizi

- `postgres`: contiene il database `heritage`
- `pgadmin`: GUI accessibile su `http://localhost:5050`

## 🧠 Accesso alla GUI

La GUI **pgAdmin** consente di:
- esplorare visivamente le tabelle
- eseguire query SQL
- monitorare lo stato del database

Per **collegare pgAdmin al server PostgreSQL** la prima volta, esistono due opzioni:

---

### ✅ Opzione 1: Manuale (solo la prima volta)

Dopo aver aperto `http://localhost:5050` e fatto login (`admin@heritage.com` / `admin`):
1. Clicca su “Add New Server”
2. Vai alla tab **Connection** e inserisci:

| Campo         | Valore            |
|---------------|--------------------|
| Name          | heritage           |
| Host name     | postgres           |
| Username      | postgres           |
| Password      | postgres           |
| Port          | 5432               |

---

### ✅ Opzione 2: Automatica con file `pgadmin_servers.json`

Se non vuoi configurare il collegamento al server PostgreSQL manualmente, puoi usare il file `pgadmin_servers.json` per preconfigurare pgAdmin automaticamente.

#### 🔧 Posizionamento del file

Assicurati che il file `pgadmin_servers.json` sia salvato nel progetto, ad esempio in:

```
./config/pgadmin/pgadmin_servers.json
```

#### 📝 Modifica del `docker-compose.yml`

Nel servizio `pgadmin`, aggiungi questo volume:

```yaml
volumes:
  - ./config/pgadmin/pgadmin_servers.json:/pgadmin4/servers.json:ro
```

Esempio completo:

```yaml
pgadmin:
  image: dpage/pgadmin4:8.5
  container_name: pgadmin
  restart: always
  ports:
    - "5050:80"
  environment:
    PGADMIN_DEFAULT_EMAIL: admin@heritage.com
    PGADMIN_DEFAULT_PASSWORD: admin
  volumes:
    - pgadmin-data:/var/lib/pgadmin
    - ./config/pgadmin/pgadmin_servers.json:/pgadmin4/servers.json:ro
  depends_on:
    - postgres
```

> `:ro` significa **read-only**, per evitare che pgAdmin sovrascriva il file.

#### 📌 Quando viene caricato il file?

Il file `pgadmin_servers.json` viene letto **solo al primo avvio** del container pgAdmin, se il volume `pgadmin-data` è ancora vuoto.

#### 💥 Se vuoi forzare il caricamento del file:

```bash
docker-compose down -v  # ⚠️ Cancella i volumi persistenti (compresi dati e configurazioni)
docker-compose up --build
```

#### ✅ Risultato atteso

Quando visiti `http://localhost:5050` e accedi con:

- **Email**: `admin@heritage.com`
- **Password**: `admin`

Troverai **già configurata** la connessione al server PostgreSQL `heritage`.

---

### 📂 Volumi e Persistenza

I dati del database e della GUI sono salvati in volumi Docker:

- `postgres-data`: contiene i dati del DB
- `pgadmin-data`: contiene le connessioni, layout, password salvate ecc.

Per evitare di perdere la configurazione della GUI, **non usare `docker-compose down -v` a meno che non sia necessario**.