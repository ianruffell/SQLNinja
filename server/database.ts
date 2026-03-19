import { RowDataPacket, createPool } from "mysql2/promise";

export type DatabaseType = "mysql" | "mariadb" | "postgres" | "oracle" | "sqlserver" | "ignite";

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

export function getContextLabel(type: DatabaseType) {
  return type === "oracle" || type === "ignite" ? "Schema" : "Database";
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
    case "ignite":
      return "Apache Ignite";
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
    case "ignite":
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

async function loadIgniteModule() {
  const module = (await import("apache-ignite-client")) as { default?: any };
  return (module.default ?? module) as any;
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

async function discoverIgniteSchema(input: ConnectionInput): Promise<SchemaResponse> {
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

    return buildTree(input, schemas, relationRows as RelationRecord[], columnRows as ColumnRecord[], groupedIndexes, "schema");
  } finally {
    client.disconnect();
  }
}

async function testIgniteConnection(input: ConnectionInput): Promise<TestConnectionResponse> {
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

async function executeIgniteSql(input: ConnectionInput, sql: string): Promise<QueryStatementResult[]> {
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
    case "ignite":
      return discoverIgniteSchema(input);
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
    case "ignite":
      return testIgniteConnection(input);
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
      case "ignite":
        return executeIgniteSql(input, sql);
    }
  })();

  return {
    statements,
    executedAt: new Date().toISOString(),
    durationMs: Math.round((performance.now() - startedAt) * 100) / 100,
  };
}
