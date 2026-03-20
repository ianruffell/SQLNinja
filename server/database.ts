import { execFile, spawn } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { RowDataPacket, createPool } from "mysql2/promise";

export type DatabaseType = "mysql" | "mariadb" | "postgres" | "oracle" | "sqlserver" | "ignite2" | "ignite3";

export type ConnectionInput = {
  type: DatabaseType;
  host: string;
  port: number;
  user: string;
  password: string;
  database?: string;
  selectedDatabase?: string;
};

export type SchemaNode = {
  id: string;
  label: string;
  type: "database" | "group" | "table" | "view" | "column" | "index";
  description?: string;
  reference?: string;
  children?: SchemaNode[];
};

export type SchemaResponse = {
  databases: string[];
  contextLabel: string;
  tree: SchemaNode;
};

export type QueryStatementResult =
  | {
      kind: "result-set";
      columns: string[];
      rows: Record<string, unknown>[];
      rowCount: number;
    }
  | {
      kind: "command";
      affectedRows: number;
      insertId: number | null;
      warningStatus: number;
    }
  | {
      kind: "unknown";
      value: unknown;
    };

export type QueryResponse = {
  statements: QueryStatementResult[];
  executedAt: string;
  durationMs: number;
};

export type TestConnectionResponse = {
  serverVersion: string;
  databases: string[];
  currentContext: string | null;
  contextLabel: string;
  dialectLabel: string;
};

type RelationRecord = {
  contextName: string;
  schemaName: string;
  tableName: string;
  tableType: string;
  description?: string | null;
};

type ColumnRecord = {
  contextName: string;
  schemaName: string;
  tableName: string;
  columnName: string;
  dataType: string;
  isNullable?: string | null;
  columnDefault?: string | null;
  extra?: string | null;
  columnKey?: string | null;
};

type IndexRecord = {
  contextName: string;
  schemaName: string;
  tableName: string;
  indexName: string;
  columnList?: string | null;
  definition?: string | null;
  isUnique?: boolean | number | null;
};

type QuoteStyle = "backtick" | "double" | "bracket";

const MYSQL_SYSTEM_DATABASES = ["information_schema", "mysql", "performance_schema", "sys"];
const POSTGRES_SYSTEM_SCHEMAS = ["information_schema", "pg_catalog"];
const SQLSERVER_SYSTEM_DATABASES = ["master", "tempdb", "model", "msdb"];
const SQLSERVER_SYSTEM_SCHEMAS = ["INFORMATION_SCHEMA", "sys"];
const ORACLE_SYSTEM_SCHEMAS = [
  "ANONYMOUS",
  "APPQOSSYS",
  "AUDSYS",
  "CTXSYS",
  "DBSFWUSER",
  "DBSNMP",
  "DIP",
  "DVSYS",
  "GGSYS",
  "GSMADMIN_INTERNAL",
  "LBACSYS",
  "MDSYS",
  "OJVMSYS",
  "OLAPSYS",
  "ORDDATA",
  "ORDPLUGINS",
  "ORDSYS",
  "OUTLN",
  "REMOTE_SCHEDULER_AGENT",
  "SI_INFORMTN_SCHEMA",
  "SYS",
  "SYS$UMF",
  "SYSBACKUP",
  "SYSDG",
  "SYSKM",
  "SYSTEM",
  "WMSYS",
  "XDB",
  "XS$NULL",
];
const IGNITE_SYSTEM_SCHEMAS = ["INFORMATION_SCHEMA", "SYS"];
const execFileAsync = promisify(execFile);
const IGNITE_JDBC_DRIVER_CLASS = "org.apache.ignite.jdbc.IgniteJdbcDriver";
const IGNITE_JDBC_SUPPORT_DIR = join(tmpdir(), "sqlninja-ignite-jdbc");

type IgniteJdbcProfile = {
  key: string;
  label: string;
  version: string;
  groupId: string;
  artifactId: string;
};

type IgniteJdbcSchemaPayload = {
  schemas: string[];
  relations: RelationRecord[];
  columns: ColumnRecord[];
  indexes: IndexRecord[];
  serverVersion: string;
};

type IgniteJdbcQueryPayload =
  | {
      kind: "result-set";
      columns: string[];
      rows: Record<string, unknown>[];
      rowCount: number;
    }
  | {
      kind: "command";
      affectedRows: number;
      insertId: number | null;
      warningStatus: number;
    };

type IgniteJdbcBridgeRequestAction = "test" | "schema" | "query";

type IgniteJdbcBridgeSession = {
  buffer: string;
  nextRequestId: number;
  pending: Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (error: Error) => void;
    }
  >;
  process: ChildProcessWithoutNullStreams;
};

const IGNITE_JDBC_PROFILES: IgniteJdbcProfile[] = [
  {
    key: "gridgain9",
    label: "GridGain 9 JDBC",
    groupId: "org.gridgain",
    artifactId: "ignite-jdbc",
    version: "9.1.17",
  },
  {
    key: "gridgain9-legacy",
    label: "GridGain 9 legacy JDBC",
    groupId: "org.gridgain",
    artifactId: "ignite-jdbc",
    version: "9.1.15",
  },
  {
    key: "ignite3",
    label: "Apache Ignite 3 JDBC",
    groupId: "org.apache.ignite",
    artifactId: "ignite-jdbc",
    version: "3.1.0",
  },
];
const igniteJdbcBridgeSessions = new Map<string, IgniteJdbcBridgeSession>();
const igniteJdbcDependencyPromises = new Map<string, Promise<string>>();
let igniteJdbcBridgeCompilePromise: Promise<string> | null = null;
const igniteJdbcPreferredProfileKeys = new Map<string, string>();

export function getContextLabel(type: DatabaseType) {
  return type === "oracle" || type === "ignite2" || type === "ignite3" ? "Schema" : "Database";
}

export function getDialectLabel(type: DatabaseType) {
  switch (type) {
    case "mysql":
      return "MySQL";
    case "mariadb":
      return "MariaDB";
    case "postgres":
      return "PostgreSQL";
    case "oracle":
      return "Oracle";
    case "sqlserver":
      return "SQL Server";
    case "ignite2":
      return "Apache Ignite 2.x / GridGain 8.x";
    case "ignite3":
      return "Apache Ignite 3.x / GridGain 9.x";
  }
}

function getRootLabel(input: ConnectionInput) {
  return `${getDialectLabel(input.type)} ${input.host}:${input.port}`;
}

function getQuoteStyle(type: DatabaseType): QuoteStyle {
  switch (type) {
    case "mysql":
    case "mariadb":
      return "backtick";
    case "sqlserver":
      return "bracket";
    case "postgres":
    case "oracle":
    case "ignite2":
    case "ignite3":
    default:
      return "double";
  }
}

function quoteIdentifier(value: string, style: QuoteStyle) {
  switch (style) {
    case "backtick":
      return `\`${value.replaceAll("`", "``")}\``;
    case "bracket":
      return `[${value.replaceAll("]", "]]")}]`;
    case "double":
    default:
      return `"${value.replaceAll('"', '""')}"`;
  }
}

function joinIdentifier(parts: string[], style: QuoteStyle) {
  return parts.filter((part) => part.length > 0).map((part) => quoteIdentifier(part, style)).join(".");
}

function groupBy<T>(items: T[], selector: (item: T) => string) {
  return items.reduce<Record<string, T[]>>((accumulator, item) => {
    const key = selector(item);
    accumulator[key] ??= [];
    accumulator[key].push(item);
    return accumulator;
  }, {});
}

function createPlaceholders(count: number) {
  return Array.from({ length: count }, () => "?").join(", ");
}

function createQuotedSqlList(values: string[]) {
  return values.map((value) => `'${value.replaceAll("'", "''")}'`).join(", ");
}

function splitSqlStatements(sql: string) {
  const statements: string[] = [];
  let current = "";
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];

    if (inLineComment) {
      current += char;
      if (char === "\n") {
        inLineComment = false;
      }
      continue;
    }

    if (inBlockComment) {
      current += char;
      if (char === "*" && next === "/") {
        current += "/";
        index += 1;
        inBlockComment = false;
      }
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
      if (char === "-" && next === "-") {
        current += "--";
        index += 1;
        inLineComment = true;
        continue;
      }

      if (char === "/" && next === "*") {
        current += "/*";
        index += 1;
        inBlockComment = true;
        continue;
      }
    }

    if (char === "'" && !inDoubleQuote && !inBacktick) {
      current += char;
      if (inSingleQuote && next === "'") {
        current += "'";
        index += 1;
      } else {
        inSingleQuote = !inSingleQuote;
      }
      continue;
    }

    if (char === '"' && !inSingleQuote && !inBacktick) {
      current += char;
      if (inDoubleQuote && next === '"') {
        current += '"';
        index += 1;
      } else {
        inDoubleQuote = !inDoubleQuote;
      }
      continue;
    }

    if (char === "`" && !inSingleQuote && !inDoubleQuote) {
      current += char;
      inBacktick = !inBacktick;
      continue;
    }

    if (char === ";" && !inSingleQuote && !inDoubleQuote && !inBacktick) {
      const statement = current.trim();
      if (statement.length > 0) {
        statements.push(statement);
      }
      current = "";
      continue;
    }

    current += char;
  }

  const finalStatement = current.trim();
  if (finalStatement.length > 0) {
    statements.push(finalStatement);
  }

  return statements;
}

function normalizeMysqlResult(result: unknown): QueryStatementResult {
  if (Array.isArray(result)) {
    if (result.length === 0) {
      return {
        kind: "result-set",
        columns: [],
        rows: [],
        rowCount: 0,
      };
    }

    if (typeof result[0] === "object" && result[0] !== null && !Array.isArray(result[0])) {
      const rows = result as Record<string, unknown>[];
      return {
        kind: "result-set",
        columns: Object.keys(rows[0] ?? {}),
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
      kind: "command",
      affectedRows: info.affectedRows ?? 0,
      insertId: info.insertId ?? null,
      warningStatus: info.warningStatus ?? 0,
    };
  }

  return {
    kind: "unknown",
    value: result,
  };
}

function normalizeRowsAsResultSet(rows: Record<string, unknown>[], columns: string[], rowCount?: number): QueryStatementResult {
  return {
    kind: "result-set",
    columns,
    rows,
    rowCount: rowCount ?? rows.length,
  };
}

function normalizeCommandResult(affectedRows: number, insertId: number | null = null, warningStatus = 0): QueryStatementResult {
  return {
    kind: "command",
    affectedRows,
    insertId,
    warningStatus,
  };
}

function objectifyRows(columns: string[], rows: unknown[][]) {
  return rows.map((row) =>
    columns.reduce<Record<string, unknown>>((accumulator, column, index) => {
      accumulator[column] = row[index] ?? null;
      return accumulator;
    }, {}),
  );
}

function buildContextNode(
  type: DatabaseType,
  contextName: string,
  relations: RelationRecord[],
  columnsByRelation: Record<string, ColumnRecord[]>,
  indexesByRelation: Record<string, IndexRecord[]>,
  contextKind: "database" | "schema",
): SchemaNode {
  const quoteStyle = getQuoteStyle(type);
  const relationSchemas = Array.from(new Set(relations.map((relation) => relation.schemaName))).sort((left, right) =>
    left.localeCompare(right),
  );

  const groups: SchemaNode[] = [];

  for (const schemaName of relationSchemas) {
    const schemaRelations = relations.filter((relation) => relation.schemaName === schemaName);
    const tableNodes = schemaRelations
      .filter((relation) => relation.tableType === "BASE TABLE")
      .map((relation) =>
        buildRelationNode(type, relation, contextName, contextKind, quoteStyle, columnsByRelation, indexesByRelation),
      );
    const viewNodes = schemaRelations
      .filter((relation) => relation.tableType !== "BASE TABLE")
      .map((relation) =>
        buildRelationNode(type, relation, contextName, contextKind, quoteStyle, columnsByRelation, indexesByRelation),
      );

    const labelSuffix = contextKind === "database" && schemaName !== contextName ? ` • ${schemaName}` : "";
    groups.push({
      id: `group:${contextName}:${schemaName}:tables`,
      label: `Tables (${tableNodes.length})${labelSuffix}`,
      type: "group",
      children: tableNodes,
    });
    groups.push({
      id: `group:${contextName}:${schemaName}:views`,
      label: `Views (${viewNodes.length})${labelSuffix}`,
      type: "group",
      children: viewNodes,
    });
  }

  return {
    id: `database:${contextName}`,
    label: contextName,
    type: "database",
    reference: quoteIdentifier(contextName, quoteStyle),
    children: groups,
  };
}

function buildRelationNode(
  type: DatabaseType,
  relation: RelationRecord,
  contextName: string,
  contextKind: "database" | "schema",
  quoteStyle: QuoteStyle,
  columnsByRelation: Record<string, ColumnRecord[]>,
  indexesByRelation: Record<string, IndexRecord[]>,
): SchemaNode {
  const relationKey = `${relation.contextName}.${relation.schemaName}.${relation.tableName}`;
  const qualifierParts =
    contextKind === "database" && (type === "postgres" || type === "sqlserver")
      ? [relation.schemaName, relation.tableName]
      : [relation.schemaName, relation.tableName];
  const relationReference = joinIdentifier(qualifierParts, quoteStyle);
  const columnNodes = (columnsByRelation[relationKey] ?? []).map((column) => {
    const traits = [column.dataType];
    if (column.isNullable) {
      traits.push(column.isNullable === "NO" ? "not null" : "nullable");
    }
    if (column.columnKey) {
      traits.push(column.columnKey);
    }
    if (column.extra) {
      traits.push(column.extra);
    }
    if (column.columnDefault) {
      traits.push(`default ${String(column.columnDefault).trim()}`);
    }

    return {
      id: `column:${relation.contextName}:${relation.schemaName}:${relation.tableName}:${column.columnName}`,
      label: column.columnName,
      type: "column" as const,
      description: traits.join(" | "),
      reference: `${relationReference}.${quoteIdentifier(column.columnName, quoteStyle)}`,
    };
  });

  const indexNodes = (indexesByRelation[relationKey] ?? []).map((index) => {
    const descriptionParts: string[] = [];
    if (typeof index.isUnique === "boolean") {
      descriptionParts.push(index.isUnique ? "unique" : "non-unique");
    } else if (typeof index.isUnique === "number") {
      descriptionParts.push(index.isUnique ? "unique" : "non-unique");
    }
    if (index.columnList) {
      descriptionParts.push(index.columnList);
    } else if (index.definition) {
      descriptionParts.push(index.definition);
    }

    return {
      id: `index:${relation.contextName}:${relation.schemaName}:${relation.tableName}:${index.indexName}`,
      label: index.indexName,
      type: "index" as const,
      description: descriptionParts.join(" | "),
    };
  });

  return {
    id: `${relation.tableType === "BASE TABLE" ? "table" : "view"}:${relation.contextName}:${relation.schemaName}:${relation.tableName}`,
    label: relation.tableName,
    type: relation.tableType === "BASE TABLE" ? "table" : "view",
    description: relation.description || undefined,
    reference: relationReference,
    children: [
      {
        id: `group:${relation.contextName}:${relation.schemaName}:${relation.tableName}:columns`,
        label: `Columns (${columnNodes.length})`,
        type: "group",
        children: columnNodes,
      },
      {
        id: `group:${relation.contextName}:${relation.schemaName}:${relation.tableName}:indexes`,
        label: `Indexes (${indexNodes.length})`,
        type: "group",
        children: indexNodes,
      },
    ],
  };
}

function buildTree(
  input: ConnectionInput,
  contexts: string[],
  relations: RelationRecord[],
  columns: ColumnRecord[],
  indexes: IndexRecord[],
  contextKind: "database" | "schema",
): SchemaResponse {
  const columnsByRelation = groupBy(columns, (item) => `${item.contextName}.${item.schemaName}.${item.tableName}`);
  const indexesByRelation = groupBy(indexes, (item) => `${item.contextName}.${item.schemaName}.${item.tableName}`);
  const relationsByContext = groupBy(relations, (item) => item.contextName);

  return {
    databases: contexts,
    contextLabel: getContextLabel(input.type),
    tree: {
      id: `instance:${input.type}:${input.host}:${input.port}`,
      label: getRootLabel(input),
      type: "group",
      children: contexts.map((contextName) =>
        buildContextNode(
          input.type,
          contextName,
          relationsByContext[contextName] ?? [],
          columnsByRelation,
          indexesByRelation,
          contextKind,
        ),
      ),
    },
  };
}

function getMysqlPool(input: ConnectionInput, database?: string) {
  return createPool({
    host: input.host,
    port: input.port,
    user: input.user,
    password: input.password,
    database,
    waitForConnections: false,
    connectionLimit: 4,
    queueLimit: 0,
    namedPlaceholders: true,
  });
}

async function listMysqlDatabases(pool: ReturnType<typeof getMysqlPool>) {
  const placeholders = MYSQL_SYSTEM_DATABASES.map(() => "?").join(", ");
  const [rows] = await pool.query<Array<RowDataPacket & { schemaName: string }>>(
    `
      SELECT SCHEMA_NAME AS schemaName
      FROM INFORMATION_SCHEMA.SCHEMATA
      WHERE SCHEMA_NAME NOT IN (${placeholders})
      ORDER BY SCHEMA_NAME
    `,
    MYSQL_SYSTEM_DATABASES,
  );

  return rows.map((row) => row.schemaName);
}

async function discoverMysqlSchema(input: ConnectionInput): Promise<SchemaResponse> {
  const pool = getMysqlPool(input);

  try {
    const databases = await listMysqlDatabases(pool);
    if (databases.length === 0) {
      return buildTree(input, [], [], [], [], "database");
    }

    const placeholders = createPlaceholders(databases.length);
    const [relations] = await pool.query<
      Array<RowDataPacket & { contextName: string; schemaName: string; tableName: string; tableType: string; description: string | null }>
    >(
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          TABLE_TYPE AS tableType,
          TABLE_COMMENT AS description
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN (${placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME
      `,
      databases,
    );
    const [columns] = await pool.query<
      Array<RowDataPacket & { contextName: string; schemaName: string; tableName: string; columnName: string; dataType: string; isNullable: string; columnKey: string | null; columnDefault: string | null; extra: string | null }>
    >(
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          COLUMN_NAME AS columnName,
          COLUMN_TYPE AS dataType,
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
    const [indexes] = await pool.query<
      Array<RowDataPacket & { contextName: string; schemaName: string; tableName: string; indexName: string; columnList: string; isUnique: number }>
    >(
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          INDEX_NAME AS indexName,
          GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') AS columnList,
          MIN(CASE WHEN NON_UNIQUE = 0 THEN 1 ELSE 0 END) AS isUnique
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA IN (${placeholders})
        GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
        ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
      `,
      databases,
    );

    return buildTree(input, databases, relations, columns, indexes, "database");
  } finally {
    await pool.end();
  }
}

async function testMysqlConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
  const pool = getMysqlPool(input, input.database);

  try {
    const [rows] = await pool.query<Array<RowDataPacket & { databaseName: string | null; serverVersion: string | null }>>(
      `
        SELECT DATABASE() AS databaseName, VERSION() AS serverVersion
      `,
    );
    const databases = await listMysqlDatabases(pool);

    return {
      currentContext: rows[0]?.databaseName ?? null,
      serverVersion: rows[0]?.serverVersion ?? "unknown",
      databases,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  } finally {
    await pool.end();
  }
}

async function executeMysqlSql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  const pool = getMysqlPool(input, input.selectedDatabase ?? input.database);

  try {
    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const [result] = await pool.query(statement);
      results.push(normalizeMysqlResult(result));
    }

    return results;
  } finally {
    await pool.end();
  }
}

async function loadPostgresModule() {
  const module = (await import("pg")) as { default?: { Pool: new (config: Record<string, unknown>) => any }; Pool?: new (config: Record<string, unknown>) => any };
  const pg = module.default ?? module;
  if (!pg.Pool) {
    throw new Error("PostgreSQL driver is unavailable.");
  }
  return pg.Pool;
}

async function createPostgresPool(input: ConnectionInput, database?: string) {
  const PostgresPool = await loadPostgresModule();
  return new PostgresPool({
    host: input.host,
    port: input.port,
    user: input.user,
    password: input.password,
    database: database ?? input.database ?? "postgres",
    max: 4,
  });
}

async function listPostgresDatabases(pool: any) {
  const result = await pool.query(`
    SELECT datname AS "databaseName"
    FROM pg_database
    WHERE datistemplate = false
    ORDER BY datname
  `);

  return result.rows
    .map((row: Record<string, unknown>) => (typeof row.databaseName === "string" ? row.databaseName : ""))
    .filter((name: string) => name.length > 0);
}

async function loadPostgresMetadata(input: ConnectionInput, databaseName: string) {
  const pool = await createPostgresPool(input, databaseName);

  try {
    const relationsResult = await pool.query(
      `
        SELECT
          $1::text AS "contextName",
          table_schema AS "schemaName",
          table_name AS "tableName",
          CASE WHEN table_type = 'BASE TABLE' THEN 'BASE TABLE' ELSE 'VIEW' END AS "tableType",
          NULL::text AS description
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_type, table_name
      `,
      [databaseName],
    );
    const columnsResult = await pool.query(
      `
        SELECT
          $1::text AS "contextName",
          table_schema AS "schemaName",
          table_name AS "tableName",
          column_name AS "columnName",
          data_type AS "dataType",
          is_nullable AS "isNullable",
          column_default AS "columnDefault",
          NULL::text AS extra,
          NULL::text AS "columnKey"
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position
      `,
      [databaseName],
    );
    const indexesResult = await pool.query(
      `
        SELECT
          $1::text AS "contextName",
          schemaname AS "schemaName",
          tablename AS "tableName",
          indexname AS "indexName",
          NULL::text AS "columnList",
          indexdef AS definition,
          NULL::boolean AS "isUnique"
        FROM pg_indexes
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename, indexname
      `,
      [databaseName],
    );

    return {
      relations: relationsResult.rows as RelationRecord[],
      columns: columnsResult.rows as ColumnRecord[],
      indexes: indexesResult.rows as IndexRecord[],
    };
  } finally {
    await pool.end();
  }
}

async function discoverPostgresSchema(input: ConnectionInput): Promise<SchemaResponse> {
  const adminPool = await createPostgresPool(input);

  try {
    const databases = await listPostgresDatabases(adminPool);
    const accessibleDatabases: string[] = [];
    const relations: RelationRecord[] = [];
    const columns: ColumnRecord[] = [];
    const indexes: IndexRecord[] = [];

    for (const databaseName of databases) {
      try {
        const metadata = await loadPostgresMetadata(input, databaseName);
        accessibleDatabases.push(databaseName);
        relations.push(...metadata.relations);
        columns.push(...metadata.columns);
        indexes.push(...metadata.indexes);
      } catch {
        continue;
      }
    }

    return buildTree(input, accessibleDatabases, relations, columns, indexes, "database");
  } finally {
    await adminPool.end();
  }
}

async function testPostgresConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
  const pool = await createPostgresPool(input);

  try {
    const result = await pool.query(`SELECT current_database() AS "databaseName", version() AS "serverVersion"`);
    const databases = await listPostgresDatabases(pool);

    return {
      currentContext: typeof result.rows[0]?.databaseName === "string" ? result.rows[0].databaseName : null,
      serverVersion: typeof result.rows[0]?.serverVersion === "string" ? result.rows[0].serverVersion : "unknown",
      databases,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  } finally {
    await pool.end();
  }
}

function normalizePostgresResult(result: any): QueryStatementResult {
  if (Array.isArray(result.rows) && Array.isArray(result.fields) && result.fields.length > 0) {
    return normalizeRowsAsResultSet(
      result.rows as Record<string, unknown>[],
      result.fields.map((field: { name: string }) => field.name),
      typeof result.rowCount === "number" ? result.rowCount : result.rows.length,
    );
  }

  return normalizeCommandResult(typeof result.rowCount === "number" ? result.rowCount : 0);
}

async function executePostgresSql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  const pool = await createPostgresPool(input, input.selectedDatabase ?? input.database);

  try {
    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const result = await pool.query(statement);
      results.push(normalizePostgresResult(result));
    }

    return results;
  } finally {
    await pool.end();
  }
}

async function loadSqlServerModule() {
  const module = (await import("mssql")) as { default?: Record<string, unknown> };
  return (module.default ?? module) as any;
}

async function createSqlServerPool(input: ConnectionInput, database?: string) {
  const sqlServer = await loadSqlServerModule();
  const pool = new sqlServer.ConnectionPool({
    user: input.user,
    password: input.password,
    server: input.host,
    port: input.port,
    database: database ?? input.database ?? "master",
    pool: {
      max: 4,
      min: 0,
    },
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  });

  return pool.connect();
}

async function listSqlServerDatabases(pool: any) {
  const result = await pool
    .request()
    .query(`
      SELECT name AS databaseName
      FROM sys.databases
      WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
      ORDER BY name
    `);

  return (result.recordset ?? [])
    .map((row: Record<string, unknown>) => (typeof row.databaseName === "string" ? row.databaseName : ""))
    .filter((name: string) => name.length > 0);
}

async function loadSqlServerMetadata(input: ConnectionInput, databaseName: string) {
  const pool = await createSqlServerPool(input, databaseName);

  try {
    const relationsResult = await pool.request().query(`
      SELECT
        '${databaseName.replaceAll("'", "''")}' AS contextName,
        TABLE_SCHEMA AS schemaName,
        TABLE_NAME AS tableName,
        CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'BASE TABLE' ELSE 'VIEW' END AS tableType,
        CAST(NULL AS nvarchar(4000)) AS description
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
      ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME
    `);
    const columnsResult = await pool.request().query(`
      SELECT
        '${databaseName.replaceAll("'", "''")}' AS contextName,
        TABLE_SCHEMA AS schemaName,
        TABLE_NAME AS tableName,
        COLUMN_NAME AS columnName,
        DATA_TYPE AS dataType,
        IS_NULLABLE AS isNullable,
        COLUMN_DEFAULT AS columnDefault,
        CAST(NULL AS nvarchar(4000)) AS extra,
        CAST(NULL AS nvarchar(4000)) AS columnKey
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
      ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    `);
    const indexesResult = await pool.request().query(`
      SELECT
        '${databaseName.replaceAll("'", "''")}' AS contextName,
        s.name AS schemaName,
        t.name AS tableName,
        i.name AS indexName,
        STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columnList,
        CAST(NULL AS nvarchar(max)) AS definition,
        CAST(i.is_unique AS bit) AS isUnique
      FROM sys.tables t
      INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
      INNER JOIN sys.indexes i ON i.object_id = t.object_id
      INNER JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
      INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
      WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') AND i.name IS NOT NULL
      GROUP BY s.name, t.name, i.name, i.is_unique
      ORDER BY s.name, t.name, i.name
    `);

    return {
      relations: (relationsResult.recordset ?? []) as RelationRecord[],
      columns: (columnsResult.recordset ?? []) as ColumnRecord[],
      indexes: (indexesResult.recordset ?? []) as IndexRecord[],
    };
  } finally {
    await pool.close();
  }
}

async function discoverSqlServerSchema(input: ConnectionInput): Promise<SchemaResponse> {
  const adminPool = await createSqlServerPool(input);

  try {
    const databases = await listSqlServerDatabases(adminPool);
    const accessibleDatabases: string[] = [];
    const relations: RelationRecord[] = [];
    const columns: ColumnRecord[] = [];
    const indexes: IndexRecord[] = [];

    for (const databaseName of databases) {
      try {
        const metadata = await loadSqlServerMetadata(input, databaseName);
        accessibleDatabases.push(databaseName);
        relations.push(...metadata.relations);
        columns.push(...metadata.columns);
        indexes.push(...metadata.indexes);
      } catch {
        continue;
      }
    }

    return buildTree(input, accessibleDatabases, relations, columns, indexes, "database");
  } finally {
    await adminPool.close();
  }
}

async function testSqlServerConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
  const pool = await createSqlServerPool(input);

  try {
    const result = await pool.request().query(`
      SELECT DB_NAME() AS databaseName, @@VERSION AS serverVersion
    `);
    const row = (result.recordset ?? [])[0] as Record<string, unknown> | undefined;
    const databases = await listSqlServerDatabases(pool);

    return {
      currentContext: typeof row?.databaseName === "string" ? row.databaseName : null,
      serverVersion: typeof row?.serverVersion === "string" ? row.serverVersion : "unknown",
      databases,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  } finally {
    await pool.close();
  }
}

function normalizeSqlServerResult(result: any): QueryStatementResult {
  if (Array.isArray(result.recordset)) {
    const rows = result.recordset as Record<string, unknown>[];
    const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
    return normalizeRowsAsResultSet(rows, columns, rows.length);
  }

  const affectedRows = Array.isArray(result.rowsAffected)
    ? result.rowsAffected.reduce((total: number, value: number) => total + value, 0)
    : 0;
  return normalizeCommandResult(affectedRows);
}

async function executeSqlServerSql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  const pool = await createSqlServerPool(input, input.selectedDatabase ?? input.database);

  try {
    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const result = await pool.request().query(statement);
      results.push(normalizeSqlServerResult(result));
    }

    return results;
  } finally {
    await pool.close();
  }
}

async function loadOracleModule() {
  const module = (await import("oracledb")) as { default?: Record<string, unknown> };
  return (module.default ?? module) as any;
}

function getOracleConnectString(input: ConnectionInput) {
  if (!input.database) {
    throw new Error("Oracle connections require a service name.");
  }

  return `${input.host}:${input.port}/${input.database}`;
}

async function createOracleConnection(input: ConnectionInput) {
  const oracledb = await loadOracleModule();
  const connection = await oracledb.getConnection({
    user: input.user,
    password: input.password,
    connectString: getOracleConnectString(input),
  });

  return { connection, oracledb };
}

async function executeOracleObjects(connection: any, oracledb: any, sql: string) {
  const result = await connection.execute(sql, {}, { outFormat: oracledb.OUT_FORMAT_OBJECT });
  return Array.isArray(result.rows) ? (result.rows as Record<string, unknown>[]) : [];
}

async function listOracleSchemas(connection: any, oracledb: any) {
  const excludedSchemas = createQuotedSqlList(ORACLE_SYSTEM_SCHEMAS);
  const rows = await executeOracleObjects(
    connection,
    oracledb,
    `
      SELECT USERNAME AS "schemaName"
      FROM ALL_USERS
      WHERE USERNAME NOT IN (${excludedSchemas})
      ORDER BY USERNAME
    `,
  );

  return rows
    .map((row) => (typeof row.schemaName === "string" ? row.schemaName : ""))
    .filter((schemaName) => schemaName.length > 0);
}

async function discoverOracleSchema(input: ConnectionInput): Promise<SchemaResponse> {
  const { connection, oracledb } = await createOracleConnection(input);

  try {
    const excludedSchemas = createQuotedSqlList(ORACLE_SYSTEM_SCHEMAS);
    const [schemas, relations, columns, indexes] = await Promise.all([
      listOracleSchemas(connection, oracledb),
      executeOracleObjects(
        connection,
        oracledb,
        `
          SELECT OWNER AS "contextName", OWNER AS "schemaName", TABLE_NAME AS "tableName", 'BASE TABLE' AS "tableType", NULL AS description
          FROM ALL_TABLES
          WHERE OWNER NOT IN (${excludedSchemas})
          UNION ALL
          SELECT OWNER AS "contextName", OWNER AS "schemaName", VIEW_NAME AS "tableName", 'VIEW' AS "tableType", NULL AS description
          FROM ALL_VIEWS
          WHERE OWNER NOT IN (${excludedSchemas})
        `,
      ),
      executeOracleObjects(
        connection,
        oracledb,
        `
          SELECT
            OWNER AS "contextName",
            OWNER AS "schemaName",
            TABLE_NAME AS "tableName",
            COLUMN_NAME AS "columnName",
            DATA_TYPE AS "dataType",
            NULLABLE AS "isNullable",
            DATA_DEFAULT AS "columnDefault",
            NULL AS extra,
            NULL AS "columnKey"
          FROM ALL_TAB_COLUMNS
          WHERE OWNER NOT IN (${excludedSchemas})
          ORDER BY OWNER, TABLE_NAME, COLUMN_ID
        `,
      ),
      executeOracleObjects(
        connection,
        oracledb,
        `
          SELECT
            ic.TABLE_OWNER AS "contextName",
            ic.TABLE_OWNER AS "schemaName",
            ic.TABLE_NAME AS "tableName",
            ic.INDEX_NAME AS "indexName",
            LISTAGG(ic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) AS "columnList",
            NULL AS definition,
            CASE WHEN i.UNIQUENESS = 'UNIQUE' THEN 1 ELSE 0 END AS "isUnique"
          FROM ALL_IND_COLUMNS ic
          INNER JOIN ALL_INDEXES i
            ON i.OWNER = ic.INDEX_OWNER
           AND i.INDEX_NAME = ic.INDEX_NAME
           AND i.TABLE_NAME = ic.TABLE_NAME
          WHERE ic.TABLE_OWNER NOT IN (${excludedSchemas})
          GROUP BY ic.TABLE_OWNER, ic.TABLE_NAME, ic.INDEX_NAME, i.UNIQUENESS
          ORDER BY ic.TABLE_OWNER, ic.TABLE_NAME, ic.INDEX_NAME
        `,
      ),
    ]);

    return buildTree(input, schemas, relations as RelationRecord[], columns as ColumnRecord[], indexes as IndexRecord[], "schema");
  } finally {
    await connection.close();
  }
}

async function testOracleConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
  const { connection, oracledb } = await createOracleConnection(input);

  try {
    const currentSchemaRows = await executeOracleObjects(
      connection,
      oracledb,
      `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS "currentSchema" FROM dual`,
    );
    const versionRows = await executeOracleObjects(
      connection,
      oracledb,
      `
        SELECT VERSION AS "serverVersion"
        FROM PRODUCT_COMPONENT_VERSION
        WHERE PRODUCT LIKE 'Oracle Database%'
          AND ROWNUM = 1
      `,
    );
    const schemas = await listOracleSchemas(connection, oracledb);

    return {
      currentContext: typeof currentSchemaRows[0]?.currentSchema === "string" ? currentSchemaRows[0].currentSchema : null,
      serverVersion: typeof versionRows[0]?.serverVersion === "string" ? versionRows[0].serverVersion : "unknown",
      databases: schemas,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  } finally {
    await connection.close();
  }
}

function normalizeOracleResult(result: any): QueryStatementResult {
  if (Array.isArray(result.rows)) {
    const columns = Array.isArray(result.metaData)
      ? result.metaData
          .map((column: { name?: string }) => (typeof column.name === "string" ? column.name : ""))
          .filter((name: string) => name.length > 0)
      : [];
    return normalizeRowsAsResultSet(result.rows as Record<string, unknown>[], columns, result.rows.length);
  }

  return normalizeCommandResult(typeof result.rowsAffected === "number" ? result.rowsAffected : 0);
}

async function executeOracleSql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  const { connection, oracledb } = await createOracleConnection(input);

  try {
    if (input.selectedDatabase) {
      const safeSchema = quoteIdentifier(input.selectedDatabase, "double");
      await connection.execute(`ALTER SESSION SET CURRENT_SCHEMA = ${safeSchema}`);
    }

    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const result = await connection.execute(statement, {}, { outFormat: oracledb.OUT_FORMAT_OBJECT });
      results.push(normalizeOracleResult(result));
    }

    return results;
  } finally {
    await connection.close();
  }
}

const IGNITE_JDBC_BRIDGE_SOURCE = `
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IgniteJdbcBridge {
    private static final Map<String, Connection> CONNECTIONS = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "serve".equals(args[0])) {
            serve();
            return;
        }

        if (args.length < 4) {
            throw new IllegalArgumentException("Usage: <action> <jdbcUrl> <userBase64> <passwordBase64> [sqlBase64]");
        }

        String action = args[0];
        String jdbcUrl = args[1];
        String user = decode(args[2]);
        String password = decode(args[3]);
        String sql = args.length > 4 ? decode(args[4]) : "";

        Class.forName("${IGNITE_JDBC_DRIVER_CLASS}");

        java.util.Properties properties = new java.util.Properties();
        if (!user.isBlank()) {
            properties.setProperty("user", user);
            properties.setProperty("username", user);
            properties.setProperty("password", password);
        }

        try (Connection connection = openConnection(jdbcUrl, user, password)) {
            switch (action) {
                case "test" -> printJson(runTest(connection));
                case "schema" -> printJson(runSchema(connection));
                case "query" -> printJson(runQuery(connection, sql));
                default -> throw new IllegalArgumentException("Unsupported action: " + action);
            }
        }
    }

    private static void serve() throws Exception {
        Class.forName("${IGNITE_JDBC_DRIVER_CLASS}");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }

            String[] parts = line.split("\\\\t", 6);
            if (parts.length < 5) {
                System.out.println("{\\"id\\":0,\\"ok\\":false,\\"error\\":\\"Invalid bridge request.\\"}");
                System.out.flush();
                continue;
            }

            int requestId = Integer.parseInt(parts[0]);
            String action = parts[1];
            String jdbcUrl = decode(parts[2]);
            String user = decode(parts[3]);
            String password = decode(parts[4]);
            String sql = parts.length > 5 ? decode(parts[5]) : "";

            try {
                Connection connection = getOrCreateConnection(jdbcUrl, user, password);
                Object result = switch (action) {
                    case "test" -> runTest(connection);
                    case "schema" -> runSchema(connection);
                    case "query" -> runQuery(connection, sql);
                    default -> throw new IllegalArgumentException("Unsupported action: " + action);
                };
                System.out.println("{\\"id\\":" + requestId + ",\\"ok\\":true,\\"result\\":" + toJson(result) + "}");
            } catch (Exception error) {
                invalidateConnection(jdbcUrl, user, password);
                System.out.println(
                    "{\\"id\\":" + requestId + ",\\"ok\\":false,\\"error\\":" + quote(error.getMessage() == null ? "Unknown error" : error.getMessage()) + "}"
                );
            }
            System.out.flush();
        }
    }

    private static Connection getOrCreateConnection(String jdbcUrl, String user, String password) throws Exception {
        String key = jdbcUrl + "|" + user + "|" + password;
        Connection existing = CONNECTIONS.get(key);
        if (existing != null) {
            try {
                if (!existing.isClosed() && existing.isValid(2)) {
                    return existing;
                }
            } catch (Exception ignored) {
            }
            invalidateConnection(jdbcUrl, user, password);
        }

        Connection connection = openConnection(jdbcUrl, user, password);
        CONNECTIONS.put(key, connection);
        return connection;
    }

    private static void invalidateConnection(String jdbcUrl, String user, String password) {
        String key = jdbcUrl + "|" + user + "|" + password;
        Connection existing = CONNECTIONS.remove(key);
        if (existing == null) {
            return;
        }

        try {
            existing.close();
        } catch (Exception ignored) {
        }
    }

    private static Connection openConnection(String jdbcUrl, String user, String password) throws Exception {
        java.util.Properties properties = new java.util.Properties();
        if (!user.isBlank()) {
            properties.setProperty("user", user);
            properties.setProperty("username", user);
            properties.setProperty("password", password);
        }
        return DriverManager.getConnection(jdbcUrl, properties);
    }

    private static Map<String, Object> runTest(Connection connection) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("serverVersion", connection.getMetaData().getDatabaseProductVersion());
        payload.put("schemas", listSchemas(connection));
        return payload;
    }

    private static Map<String, Object> runSchema(Connection connection) throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        List<String> schemas = listSchemas(connection);
        List<Map<String, Object>> relations = new ArrayList<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        List<Map<String, Object>> indexes = new ArrayList<>();

        for (String schemaName : schemas) {
            try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[] { "TABLE", "VIEW" })) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String rawType = tables.getString("TABLE_TYPE");
                    String tableType = "VIEW".equalsIgnoreCase(rawType) ? "VIEW" : "BASE TABLE";
                    String remarks = tables.getString("REMARKS");

                    Map<String, Object> relation = new LinkedHashMap<>();
                    relation.put("contextName", schemaName);
                    relation.put("schemaName", schemaName);
                    relation.put("tableName", tableName);
                    relation.put("tableType", tableType);
                    relation.put("description", remarks);
                    relations.add(relation);

                    try (ResultSet columnSet = metaData.getColumns(null, schemaName, tableName, "%")) {
                        while (columnSet.next()) {
                            Map<String, Object> column = new LinkedHashMap<>();
                            column.put("contextName", schemaName);
                            column.put("schemaName", schemaName);
                            column.put("tableName", tableName);
                            column.put("columnName", columnSet.getString("COLUMN_NAME"));
                            column.put("dataType", columnSet.getString("TYPE_NAME"));
                            column.put("isNullable", columnSet.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls ? "NO" : "YES");
                            column.put("columnDefault", columnSet.getString("COLUMN_DEF"));
                            column.put("extra", null);
                            column.put("columnKey", null);
                            columns.add(column);
                        }
                    }

                    try (ResultSet indexSet = metaData.getIndexInfo(null, schemaName, tableName, false, false)) {
                        while (indexSet.next()) {
                            String indexName = indexSet.getString("INDEX_NAME");
                            if (indexName == null || indexName.isBlank()) {
                                continue;
                            }

                            Map<String, Object> index = new LinkedHashMap<>();
                            index.put("contextName", schemaName);
                            index.put("schemaName", schemaName);
                            index.put("tableName", tableName);
                            index.put("indexName", indexName);
                            index.put("columnList", indexSet.getString("COLUMN_NAME"));
                            index.put("definition", null);
                            index.put("isUnique", !indexSet.getBoolean("NON_UNIQUE"));
                            indexes.add(index);
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schemas", schemas);
        payload.put("relations", relations);
        payload.put("columns", columns);
        payload.put("indexes", indexes);
        payload.put("serverVersion", metaData.getDatabaseProductVersion());
        return payload;
    }

    private static Map<String, Object> runQuery(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    List<String> columns = new ArrayList<>();
                    for (int index = 1; index <= metaData.getColumnCount(); index++) {
                        columns.add(metaData.getColumnLabel(index));
                    }

                    List<Map<String, Object>> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int index = 1; index <= columns.size(); index++) {
                            row.put(columns.get(index - 1), normalizeValue(resultSet.getObject(index), metaData.getColumnType(index)));
                        }
                        rows.add(row);
                    }

                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("kind", "result-set");
                    payload.put("columns", columns);
                    payload.put("rows", rows);
                    payload.put("rowCount", rows.size());
                    return payload;
                }
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("kind", "command");
            payload.put("affectedRows", statement.getUpdateCount());
            payload.put("insertId", null);
            payload.put("warningStatus", 0);
            return payload;
        }
    }

    private static List<String> listSchemas(Connection connection) throws Exception {
        List<String> schemas = new ArrayList<>();
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                if (schemaName == null) {
                    continue;
                }

                String normalized = schemaName.toUpperCase(Locale.ROOT);
                if (normalized.equals("INFORMATION_SCHEMA") || normalized.equals("SYS") || normalized.equals("SYSTEM")) {
                    continue;
                }
                schemas.add(schemaName);
            }
        }
        schemas.sort(String::compareToIgnoreCase);
        return schemas;
    }

    private static Object normalizeValue(Object value, int sqlType) {
        if (value == null) {
            return null;
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof java.sql.Array arrayValue) {
            try {
                Object array = arrayValue.getArray();
                if (array instanceof Object[] objects) {
                    List<Object> values = new ArrayList<>();
                    for (Object item : objects) {
                        values.add(normalizeValue(item, Types.JAVA_OBJECT));
                    }
                    return values;
                }
            } catch (Exception ignored) {
            }
        }
        if (value instanceof java.sql.Clob clobValue) {
            try {
                return clobValue.getSubString(1, (int) clobValue.length());
            } catch (Exception ignored) {
                return value.toString();
            }
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof java.util.Date || value instanceof TemporalAccessor) {
            return value.toString();
        }
        return value.toString();
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void printJson(Object value) {
        System.out.println(toJson(value));
    }

    private static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String stringValue) {
            return quote(stringValue);
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof Map<?, ?> mapValue) {
            List<String> parts = new ArrayList<>();
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                parts.add(quote(String.valueOf(entry.getKey())) + ":" + toJson(entry.getValue()));
            }
            return "{" + String.join(",", parts) + "}";
        }
        if (value instanceof Iterable<?> iterableValue) {
            List<String> parts = new ArrayList<>();
            for (Object item : iterableValue) {
                parts.add(toJson(item));
            }
            return "[" + String.join(",", parts) + "]";
        }
        return quote(String.valueOf(value));
    }

    private static String quote(String value) {
        StringBuilder builder = new StringBuilder();
        builder.append('"');
        for (int index = 0; index < value.length(); index++) {
            char ch = value.charAt(index);
            switch (ch) {
                case '\\\\' -> builder.append((char) 92).append((char) 92);
                case '"' -> builder.append((char) 92).append((char) 34);
                case '\\b' -> builder.append("\\\\b");
                case '\\f' -> builder.append("\\\\f");
                case '\\n' -> builder.append("\\\\n");
                case '\\r' -> builder.append("\\\\r");
                case '\\t' -> builder.append("\\\\t");
                default -> {
                    if (ch < 0x20) {
                        builder.append(String.format("\\\\u%04x", (int) ch));
                    } else {
                        builder.append(ch);
                    }
                }
            }
        }
        builder.append('"');
        return builder.toString();
    }
}
`;

async function loadIgniteModule() {
  const module = (await import("apache-ignite-client")) as { default?: any };
  return (module.default ?? module) as any;
}

function encodeBridgeArg(value: string | undefined) {
  return Buffer.from(value ?? "", "utf8").toString("base64");
}

function buildIgniteJdbcUrl(input: ConnectionInput) {
  const schema = input.selectedDatabase ?? input.database;
  return `jdbc:ignite:thin://${input.host}:${input.port}${schema ? `/${encodeURIComponent(schema)}` : ""}`;
}

function getIgniteJdbcProfileCacheKey(input: ConnectionInput) {
  return `${input.host}:${input.port}:${input.user.trim()}`;
}

function createIgniteJdbcPom(profile: IgniteJdbcProfile) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>local.sqlninja</groupId>
  <artifactId>${profile.key}</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>${profile.groupId}</groupId>
      <artifactId>${profile.artifactId}</artifactId>
      <version>${profile.version}</version>
    </dependency>
  </dependencies>
</project>
`;
}

async function ensureIgniteJdbcBridgeCompiled(baseDir: string) {
  if (igniteJdbcBridgeCompilePromise) {
    return igniteJdbcBridgeCompilePromise;
  }

  const sourcePath = join(baseDir, "IgniteJdbcBridge.java");
  const classPath = join(baseDir, "IgniteJdbcBridge.class");
  igniteJdbcBridgeCompilePromise = (async () => {
    await mkdir(baseDir, { recursive: true });
    await writeFile(sourcePath, IGNITE_JDBC_BRIDGE_SOURCE, "utf8");

    let shouldCompile = true;
    try {
      const [sourceStats, classStats] = await Promise.all([stat(sourcePath), stat(classPath)]);
      shouldCompile = classStats.mtimeMs < sourceStats.mtimeMs;
    } catch {
      shouldCompile = true;
    }

    if (shouldCompile) {
      await execFileAsync("javac", ["-d", baseDir, sourcePath], {
        maxBuffer: 1024 * 1024 * 8,
      });
    }

    return classPath;
  })().catch((error) => {
    igniteJdbcBridgeCompilePromise = null;
    throw error;
  });

  return igniteJdbcBridgeCompilePromise;
}

async function ensureIgniteJdbcDependencies(profile: IgniteJdbcProfile, baseDir: string) {
  const existingPromise = igniteJdbcDependencyPromises.get(profile.key);
  if (existingPromise) {
    return existingPromise;
  }

  const pomPath = join(baseDir, "pom.xml");
  const depsDir = join(baseDir, "deps");
  const promise = (async () => {
    await mkdir(depsDir, { recursive: true });
    await writeFile(pomPath, createIgniteJdbcPom(profile), "utf8");

    const existingFiles = await readdir(depsDir).catch(() => []);
    if (!existingFiles.some((file) => file.endsWith(".jar"))) {
      await execFileAsync(
        "mvn",
        ["-q", "dependency:copy-dependencies", `-DoutputDirectory=${depsDir}`, "-DincludeScope=runtime"],
        {
          cwd: baseDir,
          maxBuffer: 1024 * 1024 * 8,
        },
      );
    }

    return depsDir;
  })().catch((error) => {
    igniteJdbcDependencyPromises.delete(profile.key);
    throw error;
  });

  igniteJdbcDependencyPromises.set(profile.key, promise);
  return promise;
}

async function createIgniteJdbcBridgeSession(profile: IgniteJdbcProfile): Promise<IgniteJdbcBridgeSession> {
  const profileDir = join(IGNITE_JDBC_SUPPORT_DIR, profile.key);
  const bridgeDir = join(IGNITE_JDBC_SUPPORT_DIR, "bridge");

  await mkdir(profileDir, { recursive: true });
  await ensureIgniteJdbcBridgeCompiled(bridgeDir);
  const depsDir = await ensureIgniteJdbcDependencies(profile, profileDir);

  const jarFiles = (await readdir(depsDir))
    .filter((file) => file.endsWith(".jar"))
    .map((file) => join(depsDir, file));

  if (jarFiles.length === 0) {
    throw new Error(`${profile.label} dependencies were not downloaded.`);
  }

  const classpath = [bridgeDir, ...jarFiles].join(process.platform === "win32" ? ";" : ":");
  const child = spawn("java", ["-cp", classpath, "IgniteJdbcBridge", "serve"], {
    stdio: ["pipe", "pipe", "pipe"],
  });
  const session: IgniteJdbcBridgeSession = {
    process: child,
    buffer: "",
    nextRequestId: 1,
    pending: new Map(),
  };

  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    session.buffer += chunk;

    while (true) {
      const newlineIndex = session.buffer.indexOf("\n");
      if (newlineIndex === -1) {
        break;
      }

      const line = session.buffer.slice(0, newlineIndex).trim();
      session.buffer = session.buffer.slice(newlineIndex + 1);
      if (!line) {
        continue;
      }

      try {
        const payload = JSON.parse(line) as { id: number; ok: boolean; result?: unknown; error?: string };
        const pending = session.pending.get(payload.id);
        if (!pending) {
          continue;
        }
        session.pending.delete(payload.id);

        if (payload.ok) {
          pending.resolve(payload.result);
        } else {
          pending.reject(new Error(payload.error || `${profile.label} bridge request failed.`));
        }
      } catch (error) {
        const pendingEntries = [...session.pending.values()];
        session.pending.clear();
        for (const pending of pendingEntries) {
          pending.reject(error instanceof Error ? error : new Error("Failed to parse JDBC bridge response."));
        }
      }
    }
  });

  const rejectAll = (error: Error) => {
    const pendingEntries = [...session.pending.values()];
    session.pending.clear();
    for (const pending of pendingEntries) {
      pending.reject(error);
    }
  };

  child.stderr.setEncoding("utf8");
  let stderrOutput = "";
  child.stderr.on("data", (chunk: string) => {
    stderrOutput += chunk;
  });

  child.on("exit", (code) => {
    igniteJdbcBridgeSessions.delete(profile.key);
    const message = `${profile.label} bridge exited${code === null ? "" : ` with code ${code}`}${
      stderrOutput.trim() ? `. ${stderrOutput.trim()}` : ""
    }`;
    rejectAll(new Error(message));
  });

  child.on("error", (error) => {
    igniteJdbcBridgeSessions.delete(profile.key);
    rejectAll(error instanceof Error ? error : new Error(`${profile.label} bridge failed to start.`));
  });

  return session;
}

async function ensureIgniteJdbcBridgeSession(profile: IgniteJdbcProfile) {
  const existing = igniteJdbcBridgeSessions.get(profile.key);
  if (existing) {
    return existing;
  }

  const session = await createIgniteJdbcBridgeSession(profile);
  igniteJdbcBridgeSessions.set(profile.key, session);
  return session;
}

async function runIgniteJdbcBridge<T>(
  profile: IgniteJdbcProfile,
  action: IgniteJdbcBridgeRequestAction,
  input: ConnectionInput,
  sql?: string,
) {
  const session = await ensureIgniteJdbcBridgeSession(profile);
  const requestId = session.nextRequestId++;
  const requestLine = [
    String(requestId),
    action,
    encodeBridgeArg(buildIgniteJdbcUrl(input)),
    encodeBridgeArg(input.user.trim()),
    encodeBridgeArg(input.password),
    encodeBridgeArg(sql),
  ].join("\t");

  return await new Promise<T>((resolve, reject) => {
    session.pending.set(requestId, { resolve: resolve as (value: unknown) => void, reject });
    session.process.stdin.write(`${requestLine}\n`, "utf8", (error) => {
      if (!error) {
        return;
      }
      session.pending.delete(requestId);
      reject(error);
    });
  });
}

async function runIgniteJdbcAcrossProfiles<T>(input: ConnectionInput, action: (profile: IgniteJdbcProfile) => Promise<T>) {
  const cacheKey = getIgniteJdbcProfileCacheKey(input);
  const preferredProfileKey = igniteJdbcPreferredProfileKeys.get(cacheKey);
  const orderedProfiles = preferredProfileKey
    ? [
        ...IGNITE_JDBC_PROFILES.filter((profile) => profile.key === preferredProfileKey),
        ...IGNITE_JDBC_PROFILES.filter((profile) => profile.key !== preferredProfileKey),
      ]
    : IGNITE_JDBC_PROFILES;
  const jdbcErrors: string[] = [];

  for (const profile of orderedProfiles) {
    try {
      const result = await action(profile);
      igniteJdbcPreferredProfileKeys.set(cacheKey, profile.key);
      return result;
    } catch (error) {
      jdbcErrors.push(`${profile.label}: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  throw new Error(`Ignite 3.x / GridGain 9.x JDBC connection failed. ${jdbcErrors.join(" | ")}`);
}

async function createIgniteClient(input: ConnectionInput) {
  const IgniteClient = await loadIgniteModule();
  const client = new IgniteClient(() => {});
  const config = new IgniteClient.IgniteClientConfiguration(`${input.host}:${input.port}`);

  if (input.user.trim()) {
    config.setUserName(input.user.trim());
    config.setPassword(input.password);
  }

  await client.connect(config);
  return { client, IgniteClient };
}

async function runIgniteQuery(client: any, IgniteClient: any, schemaName: string, sql: string) {
  const cacheName = `SQLNINJA_${schemaName}`;
  const cache = await client.getOrCreateCache(
    cacheName,
    new IgniteClient.CacheConfiguration().setSqlSchema(schemaName),
  );
  const cursor = await cache.query(new IgniteClient.SqlFieldsQuery(sql));
  const rows = await cursor.getAll();
  const fieldNames = typeof cursor.getFieldNames === "function" ? cursor.getFieldNames() : [];
  return { rows, fieldNames: Array.isArray(fieldNames) ? fieldNames : [] };
}

async function runIgniteObjectsQuery(client: any, IgniteClient: any, schemaName: string, sql: string) {
  const { rows, fieldNames } = await runIgniteQuery(client, IgniteClient, schemaName, sql);
  return objectifyRows(fieldNames, rows as unknown[][]);
}

async function listIgniteSchemas(client: any, IgniteClient: any) {
  const rows = await runIgniteObjectsQuery(
    client,
    IgniteClient,
    "INFORMATION_SCHEMA",
    `
      SELECT SCHEMA_NAME AS schemaName
      FROM SCHEMATA
      WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'SYS')
      ORDER BY SCHEMA_NAME
    `,
  );

  return rows
    .map((row) => (typeof row.schemaName === "string" ? row.schemaName : ""))
    .filter((name) => name.length > 0);
}

async function discoverIgnite2Schema(input: ConnectionInput): Promise<SchemaResponse> {
  const { client, IgniteClient } = await createIgniteClient(input);

  try {
    const schemas = await listIgniteSchemas(client, IgniteClient);
    const relationRows = await runIgniteObjectsQuery(
      client,
      IgniteClient,
      "INFORMATION_SCHEMA",
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          CASE WHEN TABLE_TYPE = 'TABLE' THEN 'BASE TABLE' ELSE 'VIEW' END AS tableType,
          NULL AS description
        FROM TABLES
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYS')
        ORDER BY TABLE_SCHEMA, TABLE_TYPE, TABLE_NAME
      `,
    );
    const columnRows = await runIgniteObjectsQuery(
      client,
      IgniteClient,
      "INFORMATION_SCHEMA",
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          COLUMN_NAME AS columnName,
          TYPE_NAME AS dataType,
          CASE WHEN IS_NULLABLE = 'YES' THEN 'YES' ELSE 'NO' END AS isNullable,
          COLUMN_DEFAULT AS columnDefault,
          NULL AS extra,
          NULL AS columnKey
        FROM COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYS')
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
      `,
    );
    const indexRows = await runIgniteObjectsQuery(
      client,
      IgniteClient,
      "INFORMATION_SCHEMA",
      `
        SELECT
          TABLE_SCHEMA AS contextName,
          TABLE_SCHEMA AS schemaName,
          TABLE_NAME AS tableName,
          INDEX_NAME AS indexName,
          COLUMN_NAME AS columnList,
          NULL AS definition,
          CASE WHEN NON_UNIQUE = TRUE THEN 0 ELSE 1 END AS isUnique
        FROM INDEXES
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SYS')
        ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
      `,
    );

    const groupedIndexes = Object.values(
      indexRows.reduce<Record<string, IndexRecord>>((accumulator, row) => {
        const key = `${String(row.contextName)}.${String(row.schemaName)}.${String(row.tableName)}.${String(row.indexName)}`;
        if (!accumulator[key]) {
          accumulator[key] = {
            contextName: String(row.contextName),
            schemaName: String(row.schemaName),
            tableName: String(row.tableName),
            indexName: String(row.indexName),
            columnList: String(row.columnList ?? ""),
            isUnique: row.isUnique === 1,
          };
        } else if (row.columnList) {
          const current = accumulator[key].columnList?.trim();
          accumulator[key].columnList = current ? `${current}, ${String(row.columnList)}` : String(row.columnList);
        }
        return accumulator;
      }, {}),
    );

    return buildTree(
      input,
      schemas,
      relationRows as RelationRecord[],
      columnRows as ColumnRecord[],
      groupedIndexes,
      "schema",
    );
  } finally {
    client.disconnect();
  }
}

async function discoverIgnite3Schema(input: ConnectionInput): Promise<SchemaResponse> {
  return runIgniteJdbcAcrossProfiles(input, async (profile) => {
    const payload = await runIgniteJdbcBridge<IgniteJdbcSchemaPayload>(profile, "schema", input);
    const groupedIndexes = Object.values(
      payload.indexes.reduce<Record<string, IndexRecord>>((accumulator, row) => {
        const key = `${row.contextName}.${row.schemaName}.${row.tableName}.${row.indexName}`;
        if (!accumulator[key]) {
          accumulator[key] = {
            ...row,
            columnList: row.columnList ?? "",
          };
        } else if (row.columnList) {
          const current = accumulator[key].columnList?.trim();
          accumulator[key].columnList = current ? `${current}, ${row.columnList}` : row.columnList;
        }
        return accumulator;
      }, {}),
    );

    return buildTree(input, payload.schemas, payload.relations, payload.columns, groupedIndexes, "schema");
  });
}

async function testIgnite2Connection(input: ConnectionInput): Promise<TestConnectionResponse> {
  const { client, IgniteClient } = await createIgniteClient(input);

  try {
    const schemas = await listIgniteSchemas(client, IgniteClient);
    await runIgniteQuery(client, IgniteClient, input.database ?? "PUBLIC", "SELECT 1");

    return {
      currentContext: input.database ?? schemas[0] ?? null,
      serverVersion: "thin client connected",
      databases: schemas,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  } finally {
    client.disconnect();
  }
}

async function testIgnite3Connection(input: ConnectionInput): Promise<TestConnectionResponse> {
  return runIgniteJdbcAcrossProfiles(input, async (profile) => {
    const payload = await runIgniteJdbcBridge<{ serverVersion: string; schemas: string[] }>(profile, "test", input);
    return {
      currentContext: input.database ?? payload.schemas[0] ?? null,
      serverVersion: payload.serverVersion || `${profile.label} connected`,
      databases: payload.schemas,
      contextLabel: getContextLabel(input.type),
      dialectLabel: getDialectLabel(input.type),
    };
  });
}

function normalizeIgniteResult(result: { rows: unknown[][]; fieldNames: string[] }): QueryStatementResult {
  if (result.fieldNames.length > 0) {
    return normalizeRowsAsResultSet(objectifyRows(result.fieldNames, result.rows), result.fieldNames, result.rows.length);
  }

  if (result.rows.length === 1 && Array.isArray(result.rows[0]) && result.rows[0].length === 1) {
    const value = result.rows[0][0];
    if (typeof value === "number") {
      return normalizeCommandResult(value);
    }
  }

  return {
    kind: "unknown",
    value: result.rows,
  };
}

async function executeIgnite2Sql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  const { client, IgniteClient } = await createIgniteClient(input);

  try {
    const schema = input.selectedDatabase ?? input.database ?? "PUBLIC";
    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const result = await runIgniteQuery(client, IgniteClient, schema, statement);
      results.push(normalizeIgniteResult(result));
    }

    return results;
  } finally {
    client.disconnect();
  }
}

async function executeIgnite3Sql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
  return runIgniteJdbcAcrossProfiles(input, async (profile) => {
    const statements = splitSqlStatements(sql);
    const results: QueryStatementResult[] = [];

    for (const statement of statements) {
      const result = await runIgniteJdbcBridge<IgniteJdbcQueryPayload>(profile, "query", input, statement);
      if (result.kind === "result-set") {
        results.push(normalizeRowsAsResultSet(result.rows, result.columns, result.rowCount));
      } else {
        results.push(normalizeCommandResult(result.affectedRows, result.insertId, result.warningStatus));
      }
    }

    return results;
  });
}

export async function discoverSchema(input: ConnectionInput): Promise<SchemaResponse> {
  switch (input.type) {
    case "mysql":
    case "mariadb":
      return discoverMysqlSchema(input);
    case "postgres":
      return discoverPostgresSchema(input);
    case "oracle":
      return discoverOracleSchema(input);
    case "sqlserver":
      return discoverSqlServerSchema(input);
    case "ignite2":
      return discoverIgnite2Schema(input);
    case "ignite3":
      return discoverIgnite3Schema(input);
  }
}

export async function testConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
  switch (input.type) {
    case "mysql":
    case "mariadb":
      return testMysqlConnection(input);
    case "postgres":
      return testPostgresConnection(input);
    case "oracle":
      return testOracleConnection(input);
    case "sqlserver":
      return testSqlServerConnection(input);
    case "ignite2":
      return testIgnite2Connection(input);
    case "ignite3":
      return testIgnite3Connection(input);
  }
}

export async function executeSql(input: ConnectionInput, sql: string): Promise<QueryResponse> {
  const startedAt = performance.now();

  const statements = await (() => {
    switch (input.type) {
      case "mysql":
      case "mariadb":
        return executeMysqlSql(input, sql);
      case "postgres":
        return executePostgresSql(input, sql);
      case "oracle":
        return executeOracleSql(input, sql);
      case "sqlserver":
        return executeSqlServerSql(input, sql);
      case "ignite2":
        return executeIgnite2Sql(input, sql);
      case "ignite3":
        return executeIgnite3Sql(input, sql);
    }
  })();

  return {
    statements,
    executedAt: new Date().toISOString(),
    durationMs: Math.round((performance.now() - startedAt) * 100) / 100,
  };
}
