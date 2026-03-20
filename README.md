# SQL Ninja

SQL Ninja is a small web UI for multi-database schema exploration and notebook-style SQL execution.

## Features

- Create and manage saved connection profiles for MariaDB, MySQL, PostgreSQL, Oracle, SQL Server, Apache Ignite 2.x / GridGain 8.x, and Apache Ignite 3.x / GridGain 9.x from a dedicated connections page.
- Discover the databases or schemas exposed by each engine, then inspect tables, views, columns, and indexes from the schema tree.
- Open a workspace per saved connection, browse the full catalog tree, choose a database or schema context for queries, and insert object names into the active SQL cell.
- Use a local Ollama server to generate SQL from natural language and optimize existing SQL queries.
- Connect to Apache Ignite 2.x / GridGain 8.x through the thin client path, and to Apache Ignite 3.x / GridGain 9.x through the JDBC path.
- Import JSON from public REST APIs into MariaDB/MySQL with a separate CLI application that flattens payload fields into table columns, including endpoints such as the Jolpica Ergast-compatible API.
- Execute SQL in notebook-style cells and inspect result sets or command metadata.
- Run multiple SQL statements in one cell.

## Stack

- React + Vite frontend
- Express API
- `mysql2`, `pg`, `oracledb`, `mssql`, and `apache-ignite-client` for database access

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

To enable AI features, run an Ollama server. By default the backend uses `http://127.0.0.1:11434`.
It first checks `OLLAMA_HOST`, then falls back to `OLLAMA_BASE_URL`.
Values like `ollama.local:11434` are accepted and treated as `http://ollama.local:11434`.
You can override that with:

```bash
OLLAMA_HOST=ollama.local:11434 npm run dev
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
  "type": "mariadb",
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "secret",
  "database": "optional-initial-database-or-service"
}
```

`/api/query` also accepts:

```json
{
  "type": "postgres",
  "selectedDatabase": "app_db",
  "sql": "SELECT * FROM users LIMIT 10;"
}
```

## Notes

- Credentials are sent from the UI to the local backend for each request and are not persisted.
- The backend currently enables multi-statement execution to support notebook cells that contain several SQL statements.
