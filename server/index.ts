import "dotenv/config";
import cors from "cors";
import express from "express";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { RowDataPacket, createPool } from "mysql2/promise";
import { z } from "zod";

const app = express();
const port = Number(process.env.PORT ?? 8787);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const clientDistPath = join(__dirname, "../client");
const SYSTEM_DATABASES = ["information_schema", "mysql", "performance_schema", "sys"];
const ollamaBaseUrl = (process.env.OLLAMA_BASE_URL ?? "http://127.0.0.1:11434").replace(/\/+$/, "");

app.use(cors());
app.use(express.json({ limit: "1mb" }));

const databaseSchema = z.preprocess(
  (value) => {
    if (typeof value !== "string") {
      return undefined;
    }

    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  },
  z.string().min(1).optional(),
);

const connectionSchema = z.object({
  host: z.string().trim().min(1),
  port: z.coerce.number().int().positive().max(65535).catch(3306),
  user: z.string().trim().min(1),
  password: z.string().catch(""),
  database: databaseSchema,
});

const sqlSchema = connectionSchema.extend({
  sql: z.string().min(1),
});

const aiRequestSchema = z.object({
  model: z.string().trim().min(1),
  selectedDatabase: databaseSchema,
  schemaSummary: z.string().trim().max(25000).default(""),
  prompt: z.string().trim().max(8000).default(""),
  sql: z.string().trim().max(40000).optional(),
});

type ConnectionInput = z.infer<typeof connectionSchema>;

type SchemaNode = {
  id: string;
  label: string;
  type: "database" | "group" | "table" | "view" | "column" | "index";
  description?: string;
  reference?: string;
  children?: SchemaNode[];
};

type SchemaResponse = {
  databases: string[];
  tree: SchemaNode;
};

type AiOperation = "generate" | "optimize";

type OllamaTagsResponse = {
  models?: Array<{
    name?: string;
    model?: string;
  }>;
};

type OllamaChatResponse = {
  message?: {
    content?: string;
  };
};

type SchemaRecord = RowDataPacket & {
  schemaName: string;
};

type ColumnRecord = RowDataPacket & {
  schemaName: string;
  tableName: string;
  columnName: string;
  columnType: string;
  isNullable: "YES" | "NO";
  columnKey: string | null;
  columnDefault: string | null;
  extra: string | null;
};

type TableRecord = RowDataPacket & {
  schemaName: string;
  tableName: string;
  tableType: string;
  tableComment: string | null;
};

type IndexRecord = RowDataPacket & {
  schemaName: string;
  tableName: string;
  indexName: string;
  columnName: string;
  isUnique: number;
};

function getPool(input: ConnectionInput) {
  return createPool({
    host: input.host,
    port: input.port,
    user: input.user,
    password: input.password,
    database: input.database,
    waitForConnections: false,
    connectionLimit: 4,
    queueLimit: 0,
    namedPlaceholders: true,
    multipleStatements: true,
  });
}

async function listDatabases(pool: ReturnType<typeof getPool>) {
  const placeholders = SYSTEM_DATABASES.map(() => "?").join(", ");
  const [rows] = await pool.query<SchemaRecord[]>(
    `
      SELECT SCHEMA_NAME AS schemaName
      FROM INFORMATION_SCHEMA.SCHEMATA
      WHERE SCHEMA_NAME NOT IN (${placeholders})
      ORDER BY SCHEMA_NAME
    `,
    SYSTEM_DATABASES,
  );

  return rows.map((row) => row.schemaName);
}

function createPlaceholders(count: number) {
  return Array.from({ length: count }, () => "?").join(", ");
}

async function discoverSchema(input: ConnectionInput): Promise<SchemaResponse> {
  const pool = getPool(input);

  try {
    const databases = await listDatabases(pool);
    if (databases.length === 0) {
      return {
        databases: [],
        tree: {
          id: `instance:${input.host}:${input.port}`,
          label: `${input.host}:${input.port}`,
          type: "group",
          children: [],
        },
      };
    }

    const placeholders = createPlaceholders(databases.length);

    const [tables] = await pool.query<TableRecord[]>(
      `
        SELECT
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          TABLE_TYPE AS tableType,
          TABLE_COMMENT AS tableComment
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN (${placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME
      `,
      databases,
    );

    const [columns] = await pool.query<ColumnRecord[]>(
      `
        SELECT
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          COLUMN_NAME AS columnName,
          COLUMN_TYPE AS columnType,
          IS_NULLABLE AS isNullable,
          COLUMN_KEY AS columnKey,
          COLUMN_DEFAULT AS columnDefault,
          EXTRA AS extra
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN (${placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
      `,
      databases,
    );

    const [indexes] = await pool.query<IndexRecord[]>(
      `
        SELECT
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          INDEX_NAME AS indexName,
          COLUMN_NAME AS columnName,
          NON_UNIQUE AS isUnique
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA IN (${placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
      `,
      databases,
    );

    const columnsByTable = groupBy(columns, (item) => `${item.schemaName}.${item.tableName}`);
    const indexesByTable = groupBy(indexes, (item) => `${item.schemaName}.${item.tableName}`);
    const tablesByDatabase = groupBy(tables, (item) => item.schemaName);

    return {
      databases,
      tree: {
        id: `instance:${input.host}:${input.port}`,
        label: `${input.host}:${input.port}`,
        type: "group",
        children: databases.map((databaseName) =>
          buildDatabaseNode(
            databaseName,
            tablesByDatabase[databaseName] ?? [],
            columnsByTable,
            indexesByTable,
          ),
        ),
      },
    };
  } finally {
    await pool.end();
  }
}

async function testConnection(input: ConnectionInput) {
  const pool = getPool(input);

  try {
    const [rows] = await pool.query<RowDataPacket[]>(
      `
        SELECT
          DATABASE() AS databaseName,
          VERSION() AS serverVersion
      `,
    );

    const databases = await listDatabases(pool);
    const firstRow = rows[0];

    return {
      databaseName: typeof firstRow?.databaseName === "string" ? firstRow.databaseName : null,
      serverVersion:
        typeof firstRow?.serverVersion === "string" ? firstRow.serverVersion : "unknown",
      databases,
    };
  } finally {
    await pool.end();
  }
}

function groupBy<T>(items: T[], keySelector: (item: T) => string) {
  return items.reduce<Record<string, T[]>>((acc, item) => {
    const key = keySelector(item);
    acc[key] ??= [];
    acc[key].push(item);
    return acc;
  }, {});
}

function buildDatabaseNode(
  databaseName: string,
  tables: TableRecord[],
  columnsByTable: Record<string, ColumnRecord[]>,
  indexesByTable: Record<string, IndexRecord[]>,
): SchemaNode {
  const tableNodes = tables
    .filter((item) => item.tableType === "BASE TABLE")
    .map((table) => buildTableNode(table, columnsByTable, indexesByTable));

  const viewNodes = tables
    .filter((item) => item.tableType !== "BASE TABLE")
    .map((table) => buildTableNode(table, columnsByTable, indexesByTable));

  return {
    id: `database:${databaseName}`,
    label: databaseName,
    type: "database",
    reference: `\`${databaseName}\``,
    children: [
      {
        id: `group:${databaseName}:tables`,
        label: `Tables (${tableNodes.length})`,
        type: "group",
        children: tableNodes,
      },
      {
        id: `group:${databaseName}:views`,
        label: `Views (${viewNodes.length})`,
        type: "group",
        children: viewNodes,
      },
    ],
  };
}

function buildTableNode(
  table: TableRecord,
  columnsByTable: Record<string, ColumnRecord[]>,
  indexesByTable: Record<string, IndexRecord[]>,
): SchemaNode {
  const tableKey = `${table.schemaName}.${table.tableName}`;
  const tableReference = `\`${table.schemaName}\`.\`${table.tableName}\``;
  const columnNodes = (columnsByTable[tableKey] ?? []).map((column) => {
    const traits = [
      column.columnType,
      column.isNullable === "NO" ? "not null" : "nullable",
      column.columnKey || null,
      column.extra || null,
    ].filter(Boolean);

    return {
      id: `column:${table.schemaName}:${table.tableName}:${column.columnName}`,
      label: column.columnName,
      type: "column" as const,
      description: traits.join(" | "),
      reference: `${tableReference}.\`${column.columnName}\``,
    };
  });

  const indexMap = new Map<string, IndexRecord[]>();
  for (const index of indexesByTable[tableKey] ?? []) {
    const existing = indexMap.get(index.indexName) ?? [];
    existing.push(index);
    indexMap.set(index.indexName, existing);
  }

  const indexNodes = Array.from(indexMap.entries()).map(([indexName, records]) => ({
    id: `index:${table.schemaName}:${table.tableName}:${indexName}`,
    label: indexName,
    type: "index" as const,
    description: `${records[0]?.isUnique === 0 ? "non-unique" : "unique"} | ${records
      .map((record) => record.columnName)
      .join(", ")}`,
  }));

  return {
    id: `${table.tableType === "BASE TABLE" ? "table" : "view"}:${table.schemaName}:${table.tableName}`,
    label: table.tableName,
    type: table.tableType === "BASE TABLE" ? "table" : "view",
    description: table.tableComment || undefined,
    reference: tableReference,
    children: [
      {
        id: `group:${table.schemaName}:${table.tableName}:columns`,
        label: `Columns (${columnNodes.length})`,
        type: "group",
        children: columnNodes,
      },
      {
        id: `group:${table.schemaName}:${table.tableName}:indexes`,
        label: `Indexes (${indexNodes.length})`,
        type: "group",
        children: indexNodes,
      },
    ],
  };
}

function normalizeQueryResult(result: unknown) {
  if (Array.isArray(result)) {
    if (result.length === 0) {
      return {
        kind: "result-set" as const,
        columns: [] as string[],
        rows: [] as Record<string, unknown>[],
        rowCount: 0,
      };
    }

    if (typeof result[0] === "object" && result[0] !== null && !Array.isArray(result[0])) {
      const rows = result as Record<string, unknown>[];
      const columns = Object.keys(rows[0] ?? {});
      return {
        kind: "result-set" as const,
        columns,
        rows,
        rowCount: rows.length,
      };
    }
  }

  if (typeof result === "object" && result !== null) {
    const info = result as {
      affectedRows?: number;
      insertId?: number;
      warningStatus?: number;
    };

    return {
      kind: "command" as const,
      affectedRows: info.affectedRows ?? 0,
      insertId: info.insertId ?? null,
      warningStatus: info.warningStatus ?? 0,
    };
  }

  return {
    kind: "unknown" as const,
    value: result,
  };
}

function isCommandPacket(value: unknown) {
  return (
    typeof value === "object" &&
    value !== null &&
    ("affectedRows" in value || "insertId" in value || "warningStatus" in value || "fieldCount" in value)
  );
}

function normalizeStatementResults(rawResults: unknown) {
  if (!Array.isArray(rawResults)) {
    return [normalizeQueryResult(rawResults)];
  }

  if (rawResults.length === 0) {
    return [normalizeQueryResult(rawResults)];
  }

  if (Array.isArray(rawResults[0]) || isCommandPacket(rawResults[0])) {
    return rawResults.map((item) => normalizeQueryResult(item));
  }

  return [normalizeQueryResult(rawResults)];
}

async function fetchOllamaJson<T>(path: string, init?: RequestInit) {
  const response = await fetch(`${ollamaBaseUrl}${path}`, init);
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json") ? ((await response.json()) as T) : null;

  if (!response.ok) {
    const message =
      payload && typeof payload === "object" && payload !== null && "error" in payload
        ? String((payload as { error?: unknown }).error ?? `Ollama request failed with status ${response.status}`)
        : `Ollama request failed with status ${response.status}`;
    throw new Error(message);
  }

  return payload;
}

async function listOllamaModels() {
  const payload = await fetchOllamaJson<OllamaTagsResponse>("/api/tags");
  return (payload?.models ?? [])
    .map((model) => model.name ?? model.model ?? "")
    .filter((name) => name.length > 0);
}

function buildAiSystemPrompt(operation: AiOperation) {
  if (operation === "generate") {
    return [
      "You are a senior SQL assistant for MariaDB and MySQL.",
      "Generate a syntactically valid SQL query or set of queries based on the user request.",
      "Prefer explicit column names when reasonable, avoid unsafe destructive statements unless the user clearly asks for them.",
      "Respond only as JSON with keys: sql, notes.",
      "notes must be a short plain-English explanation.",
    ].join(" ");
  }

  return [
    "You are a senior SQL performance assistant for MariaDB and MySQL.",
    "Optimize the provided SQL while preserving behavior unless the user request explicitly allows semantic changes.",
    "Favor readable SQL, reduced unnecessary work, and better join/filter placement.",
    "Respond only as JSON with keys: sql, notes.",
    "notes must briefly explain the optimization decisions and any assumptions.",
  ].join(" ");
}

function buildAiUserPrompt(
  operation: AiOperation,
  input: z.infer<typeof aiRequestSchema>,
) {
  const sections = [
    operation === "generate" ? "Task: Generate SQL from a natural-language request." : "Task: Optimize an existing SQL query.",
    `Selected database: ${input.selectedDatabase ?? "none"}`,
    `Schema summary:\n${input.schemaSummary || "No schema summary provided."}`,
  ];

  if (operation === "generate") {
    sections.push(`User request:\n${input.prompt || "No prompt provided."}`);
  } else {
    sections.push(`Existing SQL:\n${input.sql || ""}`);
    sections.push(`Optimization goal:\n${input.prompt || "Improve performance and readability while preserving behavior."}`);
  }

  sections.push('Return JSON exactly like {"sql":"...","notes":"..."}');
  return sections.join("\n\n");
}

function parseAiResponse(content: string | undefined) {
  if (!content) {
    throw new Error("Ollama returned an empty response.");
  }

  const parsed = JSON.parse(content) as { sql?: unknown; notes?: unknown };
  if (typeof parsed.sql !== "string" || parsed.sql.trim().length === 0) {
    throw new Error("Ollama did not return SQL in the expected format.");
  }

  return {
    sql: parsed.sql.trim(),
    notes: typeof parsed.notes === "string" ? parsed.notes.trim() : "",
  };
}

async function runAiTask(operation: AiOperation, input: z.infer<typeof aiRequestSchema>) {
  const payload = await fetchOllamaJson<OllamaChatResponse>("/api/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: input.model,
      stream: false,
      format: "json",
      messages: [
        {
          role: "system",
          content: buildAiSystemPrompt(operation),
        },
        {
          role: "user",
          content: buildAiUserPrompt(operation, input),
        },
      ],
    }),
  });

  return parseAiResponse(payload?.message?.content);
}

app.use(express.static(clientDistPath));

app.get("/api/health", (_request, response) => {
  response.json({ ok: true });
});

app.post("/api/schema", async (request, response) => {
  const parsed = connectionSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid connection payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  try {
    const schema = await discoverSchema(parsed.data);
    response.json(schema);
  } catch (error) {
    response.status(500).json({
      message: "Failed to discover schema.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/test-connection", async (request, response) => {
  const parsed = connectionSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid connection payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  try {
    const result = await testConnection(parsed.data);
    response.json({
      ok: true,
      ...result,
    });
  } catch (error) {
    response.status(500).json({
      message: "Failed to connect to the database.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/query", async (request, response) => {
  const parsed = sqlSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid query payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  const pool = getPool(parsed.data);

  try {
    const startedAt = performance.now();
    const [rawResults] = await pool.query(parsed.data.sql);
    const normalizedResults = normalizeStatementResults(rawResults);
    const durationMs = Math.round((performance.now() - startedAt) * 100) / 100;

    response.json({
      statements: normalizedResults,
      executedAt: new Date().toISOString(),
      durationMs,
    });
  } catch (error) {
    response.status(500).json({
      message: "Failed to execute SQL.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  } finally {
    await pool.end();
  }
});

app.get("/api/ai/models", async (_request, response) => {
  try {
    const models = await listOllamaModels();
    response.json({
      models,
      baseUrl: ollamaBaseUrl,
    });
  } catch (error) {
    response.status(502).json({
      message: "Failed to reach the local Ollama server.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/ai/generate-sql", async (request, response) => {
  const parsed = aiRequestSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid AI request payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  try {
    const result = await runAiTask("generate", parsed.data);
    response.json(result);
  } catch (error) {
    response.status(502).json({
      message: "Failed to generate SQL with Ollama.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/ai/optimize-sql", async (request, response) => {
  const parsed = aiRequestSchema.safeParse(request.body);
  if (!parsed.success || !parsed.data.sql) {
    response.status(400).json({
      message: "Invalid AI optimization payload.",
      issues: parsed.success ? [{ message: "sql is required" }] : parsed.error.issues,
    });
    return;
  }

  try {
    const result = await runAiTask("optimize", parsed.data);
    response.json(result);
  } catch (error) {
    response.status(502).json({
      message: "Failed to optimize SQL with Ollama.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.get("/{*path}", (_request, response) => {
  response.sendFile(join(clientDistPath, "index.html"));
});

app.listen(port, () => {
  console.log(`SQL Ninja API listening on http://localhost:${port}`);
});
