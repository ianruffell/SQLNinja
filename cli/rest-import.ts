import { RowDataPacket, createPool } from "mysql2/promise";

type Options = {
  url: string;
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  table: string;
  rootPath: string;
  truncate: boolean;
  recreate: boolean;
};

type RestPayload = {
  status: number;
  contentType: string;
  body: unknown;
  finalUrl: string;
};

type FlatField = {
  columnName: string;
  value: unknown;
};

type ColumnRow = RowDataPacket & {
  Field: string;
  Type: string;
  Null: "YES" | "NO";
  Default: string | null;
  Extra: string;
};

const DEFAULTS = {
  host: "127.0.0.1",
  port: 3306,
  table: "rest_api_imports",
  rootPath: "",
} as const;

async function main() {
  const options = parseArgs(process.argv.slice(2));
  validateOptions(options);

  console.log(`Fetching ${options.url}`);
  const payload = await fetchRestPayload(options.url);
  const extracted = resolveRootPath(payload.body, options.rootPath);
  const records = Array.isArray(extracted) ? extracted : [extracted];
  const flattenedRecords = records.map((record) => flattenRecord(record));

  const pool = createPool({
    host: options.host,
    port: options.port,
    user: options.user,
    password: options.password,
    database: options.database,
    waitForConnections: false,
    connectionLimit: 2,
    queueLimit: 0,
  });

  try {
    const tableName = options.table;
    const tableColumns = await ensureImporterTable(
      pool,
      tableName,
      options.recreate,
      flattenedRecords,
    );

    if (options.truncate) {
      console.log(`Truncating table ${tableName}`);
      await pool.query(`TRUNCATE TABLE \`${tableName}\``);
    }

    for (let index = 0; index < flattenedRecords.length; index += 1) {
      const row = buildInsertRow(flattenedRecords[index], tableColumns);
      const fieldList = row.map(([field]) => `\`${field}\``).join(", ");
      const placeholders = row.map(() => "?").join(", ");
      const values = row.map(([, value]) => value);

      await pool.query(
        `
          INSERT INTO \`${tableName}\` (${fieldList})
          VALUES (${placeholders})
        `,
        values,
      );
    }

    console.log(
      `Imported ${records.length} record(s) from ${payload.finalUrl} into ${options.database}.${tableName}`,
    );
  } finally {
    await pool.end();
  }
}

async function ensureImporterTable(
  pool: ReturnType<typeof createPool>,
  tableName: string,
  recreate: boolean,
  flattenedRecords: FlatField[][],
) {
  const exists = await tableExists(pool, tableName);

  if (recreate) {
    console.log(`Dropping and recreating table ${tableName}`);
    await pool.query(`DROP TABLE IF EXISTS \`${tableName}\``);
  }

  if (recreate || !exists) {
    const definitions = inferColumnDefinitions(flattenedRecords);
    const fieldSql = definitions.length > 0 ? `,\n        ${definitions.join(",\n        ")}` : "";
    await pool.query(`
      CREATE TABLE IF NOT EXISTS \`${tableName}\` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY${fieldSql},
        imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }

  const [columns] = await pool.query<ColumnRow[]>(`SHOW COLUMNS FROM \`${tableName}\``);
  const existing = new Set(columns.map((column) => column.Field));
  const missing = inferColumnDefinitions(flattenedRecords).filter((definition) => {
    const match = definition.match(/^`([^`]+)`/);
    return match ? !existing.has(match[1]) : false;
  });

  if (missing.length > 0) {
    console.log(`Adding ${missing.length} missing column(s) to ${tableName}`);
    await pool.query(`ALTER TABLE \`${tableName}\` ${missing.map((item) => `ADD COLUMN ${item}`).join(", ")}`);
  }

  const [updatedColumns] = await pool.query<ColumnRow[]>(`SHOW COLUMNS FROM \`${tableName}\``);
  return updatedColumns;
}

function buildInsertRow(
  flattenedRecord: FlatField[],
  tableColumns: ColumnRow[],
) {
  const valuesByColumn = new Map(flattenedRecord.map((field) => [field.columnName, field.value]));

  const row: Array<[string, unknown]> = [];
  const missingRequired: string[] = [];

  for (const column of tableColumns) {
    const field = column.Field;
    const lowerExtra = column.Extra.toLowerCase();

    if (field === "id" && lowerExtra.includes("auto_increment")) {
      continue;
    }

    if (field === "imported_at" && column.Default !== null) {
      continue;
    }

    if (valuesByColumn.has(field)) {
      const matchedValue = coerceValueForColumn(valuesByColumn.get(field), column);
      row.push([field, matchedValue]);
      continue;
    }

    if (column.Default !== null || column.Null === "YES") {
      continue;
    }

    if (lowerExtra.includes("auto_increment")) {
      continue;
    }

    missingRequired.push(field);
  }

  if (missingRequired.length > 0) {
    const availableFields = flattenedRecord.map((field) => field.columnName).sort().join(", ") || "none";
    throw new Error(
      `Target table requires columns not present in the API payload: ${missingRequired.join(
        ", ",
      )}. Available payload fields: ${availableFields}`,
    );
  }

  if (row.length === 0) {
    throw new Error("No matching columns were found for the target table.");
  }

  return row;
}

async function tableExists(pool: ReturnType<typeof createPool>, tableName: string) {
  const [rows] = await pool.query<RowDataPacket[]>("SHOW TABLES LIKE ?", [tableName]);
  return rows.length > 0;
}

function normalizeColumnValue(value: unknown) {
  if (value === null || typeof value === "string" || typeof value === "number") {
    return value;
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return JSON.stringify(value);
}

function flattenRecord(record: unknown) {
  const result: FlatField[] = [];

  function visit(value: unknown, path: string[]) {
    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      for (const [key, child] of Object.entries(value)) {
        visit(child, [...path, key]);
      }
      return;
    }

    const columnName = sanitizeColumnName(path.join("_"));
    if (!columnName) {
      return;
    }

    result.push({
      columnName,
      value: Array.isArray(value) ? JSON.stringify(value) : value,
    });
  }

  if (typeof record === "object" && record !== null && !Array.isArray(record)) {
    visit(record, []);
  } else {
    result.push({
      columnName: "value",
      value: Array.isArray(record) ? JSON.stringify(record) : record,
    });
  }

  return dedupeFlatFields(result);
}

function dedupeFlatFields(fields: FlatField[]) {
  const seen = new Set<string>();
  const result: FlatField[] = [];

  for (const field of fields) {
    if (!seen.has(field.columnName)) {
      seen.add(field.columnName);
      result.push(field);
    }
  }

  return result;
}

function sanitizeColumnName(value: string) {
  const sanitized = value
    .replace(/[^A-Za-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  if (!sanitized) {
    return "";
  }

  return /^[A-Za-z_]/.test(sanitized) ? sanitized.slice(0, 64) : `c_${sanitized}`.slice(0, 64);
}

function inferColumnDefinitions(flattenedRecords: FlatField[][]) {
  const byColumn = new Map<string, unknown[]>();

  for (const record of flattenedRecords) {
    for (const field of record) {
      const existing = byColumn.get(field.columnName) ?? [];
      existing.push(field.value);
      byColumn.set(field.columnName, existing);
    }
  }

  return Array.from(byColumn.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([columnName, values]) => `\`${columnName}\` ${inferSqlType(values)} NULL`);
}

function inferSqlType(values: unknown[]) {
  const defined = values.filter((value) => value !== null && typeof value !== "undefined");
  if (defined.length === 0) {
    return "TEXT";
  }

  if (defined.every((value) => typeof value === "boolean")) {
    return "TINYINT(1)";
  }

  if (defined.every((value) => typeof value === "number" && Number.isInteger(value))) {
    return "BIGINT";
  }

  if (defined.every((value) => typeof value === "number")) {
    return "DOUBLE";
  }

  if (defined.every((value) => typeof value === "string" && looksJsonString(value))) {
    return "JSON";
  }

  const maxLength = Math.max(...defined.map((value) => String(value).length));
  return maxLength <= 255 ? "VARCHAR(255)" : "TEXT";
}

function looksJsonString(value: unknown) {
  if (typeof value !== "string") {
    return false;
  }

  const trimmed = value.trim();
  return (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  );
}

function coerceValueForColumn(value: unknown, column: ColumnRow) {
  if (value === null) {
    return null;
  }

  const type = column.Type.toLowerCase();

  if (isJsonLikeColumn(type)) {
    return typeof value === "string" ? value : JSON.stringify(value);
  }

  if (isTextLikeColumn(type)) {
    return typeof value === "string" ? value : JSON.stringify(value);
  }

  if (typeof value === "object" && value !== null) {
    return undefined;
  }

  if (type.startsWith("tinyint(1)") || type.startsWith("boolean") || type.startsWith("bool")) {
    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }
    if (typeof value === "string") {
      if (value.toLowerCase() === "true") {
        return 1;
      }
      if (value.toLowerCase() === "false") {
        return 0;
      }
    }
  }

  if (isIntegerLikeColumn(type)) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? Math.trunc(numeric) : value;
  }

  if (isNumericLikeColumn(type)) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : value;
  }

  return String(value);
}

function isJsonLikeColumn(type: string) {
  return type.startsWith("json");
}

function isTextLikeColumn(type: string) {
  return (
    type.startsWith("varchar") ||
    type.startsWith("char") ||
    type.includes("text") ||
    type.startsWith("enum") ||
    type.startsWith("set") ||
    type.startsWith("date") ||
    type.startsWith("datetime") ||
    type.startsWith("timestamp") ||
    type.startsWith("time") ||
    type.startsWith("year")
  );
}

function isIntegerLikeColumn(type: string) {
  return (
    type.startsWith("int") ||
    type.startsWith("tinyint") ||
    type.startsWith("smallint") ||
    type.startsWith("mediumint") ||
    type.startsWith("bigint")
  );
}

function isNumericLikeColumn(type: string) {
  return (
    isIntegerLikeColumn(type) ||
    type.startsWith("decimal") ||
    type.startsWith("numeric") ||
    type.startsWith("float") ||
    type.startsWith("double") ||
    type.startsWith("real")
  );
}

function parseArgs(argv: string[]): Options {
  const parsed: Partial<Options> = {
    host: DEFAULTS.host,
    port: DEFAULTS.port,
    table: DEFAULTS.table,
    rootPath: DEFAULTS.rootPath,
    truncate: false,
    recreate: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--truncate") {
      parsed.truncate = true;
      continue;
    }

    if (arg === "--recreate") {
      parsed.recreate = true;
      continue;
    }

    if (!arg.startsWith("--")) {
      continue;
    }

    const key = arg.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }

    index += 1;
    switch (key) {
      case "url":
        parsed.url = value;
        break;
      case "host":
        parsed.host = value;
        break;
      case "port":
        parsed.port = Number(value);
        break;
      case "user":
        parsed.user = value;
        break;
      case "password":
        parsed.password = value;
        break;
      case "database":
        parsed.database = value;
        break;
      case "table":
        parsed.table = value;
        break;
      case "root-path":
        parsed.rootPath = value;
        break;
      default:
        throw new Error(`Unknown argument: --${key}`);
    }
  }

  return parsed as Options;
}

function validateOptions(options: Options) {
  if (!options.url) {
    throw new Error("Missing required --url");
  }

  if (!options.user) {
    throw new Error("Missing required --user");
  }

  if (!options.database) {
    throw new Error("Missing required --database");
  }

  if (!Number.isFinite(options.port) || options.port <= 0) {
    throw new Error("Invalid --port");
  }

  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(options.table)) {
    throw new Error("Invalid --table. Use only letters, numbers, and underscores.");
  }
}

async function fetchRestPayload(url: string): Promise<RestPayload> {
  const parsedUrl = new URL(url);
  if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
    throw new Error("Only http and https URLs are supported.");
  }

  const response = await fetch(url, {
    headers: {
      Accept: "application/json, text/plain;q=0.9, */*;q=0.8",
      "User-Agent": "SQLNinja REST Importer/0.1",
    },
    signal: AbortSignal.timeout(15000),
  });

  const contentType = response.headers.get("content-type") ?? "";
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}\n${text}`);
  }

  let body: unknown = text;
  if (contentType.includes("application/json")) {
    body = JSON.parse(text);
  }

  return {
    status: response.status,
    contentType,
    body,
    finalUrl: response.url,
  };
}

function resolveRootPath(body: unknown, rootPath: string) {
  if (!rootPath) {
    return body;
  }

  const segments = rootPath.split(".").filter(Boolean);
  let current: unknown = body;

  for (const segment of segments) {
    if (Array.isArray(current)) {
      const index = Number(segment);
      current = Number.isInteger(index) ? current[index] : undefined;
      continue;
    }

    if (typeof current === "object" && current !== null) {
      current = (current as Record<string, unknown>)[segment];
      continue;
    }

    current = undefined;
  }

  if (typeof current === "undefined") {
    throw new Error(`root-path "${rootPath}" did not resolve to a value in the response payload.`);
  }

  return current;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  console.error("");
  console.error("Usage:");
  console.error(
    "  npm run rest:import -- --url <url> --user <user> --password <password> --database <database> [--host 127.0.0.1] [--port 3306] [--table rest_api_imports] [--root-path path.to.items] [--truncate] [--recreate]",
  );
  process.exit(1);
});
