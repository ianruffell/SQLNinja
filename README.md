# SQL Ninja

SQL Ninja is a small web UI for MariaDB/MySQL schema exploration and notebook-style SQL execution.

## Features

- Create and manage multiple MariaDB or MySQL server connection profiles from a dedicated connections page.
- Discover all accessible databases on a server, then inspect their tables, views, columns, and indexes from `INFORMATION_SCHEMA`.
- Open a workspace per saved connection, browse the full instance tree, choose a database context for queries, and insert object names into the active SQL cell.
- Use a local Ollama server to generate SQL from natural language and optimize existing SQL queries.
- Import JSON from public REST APIs into MariaDB/MySQL with a separate CLI application that flattens payload fields into table columns, including endpoints such as the Jolpica Ergast-compatible API.
- Execute SQL in notebook-style cells and inspect result sets or command metadata.
- Run multiple SQL statements in one cell.

## Stack

- React + Vite frontend
- Express API
- `mysql2/promise` for database access

## Getting started

1. Install dependencies:

   ```bash
   npm install
   ```

2. Start the app in development mode:

   ```bash
   npm run dev
   ```

3. Open `http://localhost:5173`.

The frontend proxies API requests to the backend on port `8787`.

To enable AI features, run a local Ollama server. By default the backend uses `http://127.0.0.1:11434`.
You can override that with:

```bash
OLLAMA_BASE_URL=http://127.0.0.1:11434 npm run dev
```

For a production-style build:

```bash
npm run build
npm start
```

## API endpoints

- `GET /api/health`
- `POST /api/schema`
- `POST /api/query`
- `GET /api/ai/models`
- `POST /api/ai/generate-sql`
- `POST /api/ai/optimize-sql`

## REST Import CLI

Use the standalone importer to load REST API responses into a database table as unpacked columns:

```bash
npm run rest:import -- \
  --url https://api.jolpi.ca/ergast/f1/current.json \
  --user root \
  --password secret \
  --database app_db \
  --table ergast_imports
```

To import a nested array from the response, use `--root-path`:

```bash
npm run rest:import -- \
  --url https://api.jolpi.ca/ergast/f1/current/driverStandings.json \
  --user root \
  --password secret \
  --database app_db \
  --table driver_standings \
  --root-path MRData.StandingsTable.StandingsLists.0.DriverStandings \
  --truncate
```

To drop and recreate the target table before importing:

```bash
npm run rest:import -- \
  --url https://api.jolpi.ca/ergast/f1/current/driverStandings.json \
  --user root \
  --password secret \
  --database app_db \
  --table driver_standings \
  --root-path MRData.StandingsTable.StandingsLists.0.DriverStandings \
  --recreate
```

Both POST endpoints accept:

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "secret"
}
```

`/api/query` also accepts:

```json
{
  "database": "app_db",
  "sql": "SELECT * FROM users LIMIT 10;"
}
```

## Notes

- Credentials are sent from the UI to the local backend for each request and are not persisted.
- The backend currently enables multi-statement execution to support notebook cells that contain several SQL statements.
