import type {
  ConnectionConfig,
  ConnectionDraft,
  DatabaseType,
  SavedConnection,
  SchemaNode,
  SchemaPayload,
} from "../types";

const DEFAULT_PORTS: Record<DatabaseType, number> = {
  mysql: 3306,
  mariadb: 3306,
  postgres: 5432,
  oracle: 1521,
  sqlserver: 1433,
  ignite: 10800,
};

const DATABASE_TYPE_LABELS: Record<DatabaseType, string> = {
  mysql: "MySQL",
  mariadb: "MariaDB",
  postgres: "PostgreSQL",
  oracle: "Oracle",
  sqlserver: "SQL Server",
  ignite: "Apache Ignite",
};

export const DATABASE_TYPE_OPTIONS = (Object.keys(DATABASE_TYPE_LABELS) as DatabaseType[]).map((type) => ({
  value: type,
  label: DATABASE_TYPE_LABELS[type],
}));

function normalizeDatabaseType(value: unknown): DatabaseType {
  switch (value) {
    case "mysql":
    case "mariadb":
    case "postgres":
    case "oracle":
    case "sqlserver":
    case "ignite":
      return value;
    default:
      return "mariadb";
  }
}

export function getDefaultPort(type: DatabaseType) {
  return DEFAULT_PORTS[type];
}

export function getDatabaseTypeLabel(type: DatabaseType) {
  return DATABASE_TYPE_LABELS[type];
}

export function getContextLabel(type: DatabaseType) {
  return type === "oracle" || type === "ignite" ? "Schema" : "Database";
}

export function getConnectionTargetLabel(type: DatabaseType) {
  switch (type) {
    case "oracle":
      return "Service name";
    case "postgres":
    case "sqlserver":
      return "Initial database";
    case "ignite":
      return "Default schema";
    case "mysql":
    case "mariadb":
    default:
      return "Initial database";
  }
}

export function getConnectionTargetPlaceholder(type: DatabaseType) {
  switch (type) {
    case "oracle":
      return "XEPDB1";
    case "postgres":
      return "postgres";
    case "sqlserver":
      return "master";
    case "ignite":
      return "PUBLIC";
    case "mysql":
    case "mariadb":
    default:
      return "Optional";
  }
}

export function getConnectionTypeDescription(type: DatabaseType) {
  switch (type) {
    case "oracle":
      return "Oracle needs a service name or connect target so SQL Ninja can open the service and discover schemas.";
    case "postgres":
      return "PostgreSQL can connect through an initial database such as postgres, then inspect accessible databases.";
    case "sqlserver":
      return "SQL Server typically connects through an initial database such as master, then discovers other databases.";
    case "ignite":
      return "Ignite connects over the thin client port and can use an optional default SQL schema.";
    case "mysql":
    case "mariadb":
    default:
      return "Database selection happens in the workspace after the server connection succeeds.";
  }
}

export function normalizeSavedConnection(value: unknown): SavedConnection | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  const type = normalizeDatabaseType(candidate.type);
  const host = typeof candidate.host === "string" ? candidate.host.trim() : "";
  const user = typeof candidate.user === "string" ? candidate.user.trim() : "";

  if (!host || !user) {
    return null;
  }

  const portCandidate =
    typeof candidate.port === "number"
      ? candidate.port
      : typeof candidate.port === "string"
        ? Number(candidate.port)
        : getDefaultPort(type);

  return {
    id: typeof candidate.id === "string" && candidate.id ? candidate.id : crypto.randomUUID(),
    name:
      typeof candidate.name === "string" && candidate.name.trim()
        ? candidate.name.trim()
        : `${user}@${host}`,
    type,
    host,
    port: Number.isFinite(portCandidate) && portCandidate > 0 ? portCandidate : getDefaultPort(type),
    user,
    password: typeof candidate.password === "string" ? candidate.password : "",
    database: typeof candidate.database === "string" ? candidate.database.trim() : "",
  };
}

export function toDraft(connection: SavedConnection): ConnectionDraft {
  return {
    name: connection.name,
    type: connection.type,
    host: connection.host,
    port: connection.port,
    user: connection.user,
    password: connection.password,
    database: connection.database ?? "",
  };
}

export function summarizeConnection(connection: SavedConnection) {
  const base = `${getDatabaseTypeLabel(connection.type)} • ${connection.user}@${connection.host}:${connection.port}`;
  return connection.database ? `${base} / ${connection.database}` : base;
}

export function sanitizeDraft(draft: ConnectionDraft) {
  return {
    ...draft,
    name: draft.name.trim() || `${draft.user}@${draft.host}`,
    type: normalizeDatabaseType(draft.type),
    host: draft.host.trim(),
    port:
      Number.isFinite(draft.port) && draft.port > 0 ? draft.port : getDefaultPort(normalizeDatabaseType(draft.type)),
    user: draft.user.trim(),
    database: draft.database.trim(),
  };
}

export function toConnectionPayload(connection: SavedConnection | ConnectionConfig, selectedDatabase?: string | null) {
  return {
    type: connection.type,
    host: connection.host.trim(),
    port: Number.isFinite(connection.port) && connection.port > 0 ? connection.port : getDefaultPort(connection.type),
    user: connection.user.trim(),
    password: connection.password,
    database: connection.database?.trim() ? connection.database.trim() : undefined,
    selectedDatabase: selectedDatabase?.trim() ? selectedDatabase.trim() : undefined,
  };
}

export function formatApiError(payload: unknown, fallback: string) {
  if (typeof payload !== "object" || payload === null) {
    return fallback;
  }

  const candidate = payload as {
    error?: unknown;
    message?: unknown;
    issues?: Array<{ path?: Array<string | number>; message?: string }>;
  };

  if (Array.isArray(candidate.issues) && candidate.issues.length > 0) {
    return candidate.issues
      .map((issue) => {
        const path = Array.isArray(issue.path) && issue.path.length > 0 ? `${issue.path.join(".")}: ` : "";
        return `${path}${issue.message ?? "Invalid value"}`;
      })
      .join(" | ");
  }

  if (typeof candidate.error === "string" && candidate.error.length > 0) {
    return candidate.error;
  }

  if (typeof candidate.message === "string" && candidate.message.length > 0) {
    return candidate.message;
  }

  return fallback;
}

export function normalizeSchemaPayload(payload: unknown): SchemaPayload {
  if (typeof payload !== "object" || payload === null) {
    return {
      databases: [],
      contextLabel: "Database",
      tree: {
        id: "instance:unknown",
        label: "Databases",
        type: "group",
        children: [],
      },
    };
  }

  const candidate = payload as Partial<SchemaPayload>;
  return {
    databases: Array.isArray(candidate.databases)
      ? candidate.databases.filter((item): item is string => typeof item === "string")
      : [],
    contextLabel: typeof candidate.contextLabel === "string" && candidate.contextLabel.trim() ? candidate.contextLabel : "Database",
    tree:
      typeof candidate.tree === "object" && candidate.tree !== null
        ? (candidate.tree as SchemaNode)
        : {
            id: "instance:unknown",
            label: "Databases",
            type: "group",
            children: [],
          },
  };
}

export function summarizeSchemaForAi(schema: SchemaNode | null, selectedDatabase: string | null) {
  if (!schema?.children) {
    return "No schema loaded.";
  }

  const targetDatabases = schema.children.filter(
    (node) => node.type === "database" && (!selectedDatabase || node.label === selectedDatabase),
  );

  const lines: string[] = [];
  for (const database of targetDatabases) {
    lines.push(`DATABASE ${database.label}`);
    for (const group of database.children ?? []) {
      if (group.type !== "group") {
        continue;
      }

      for (const relation of group.children ?? []) {
        if (relation.type !== "table" && relation.type !== "view") {
          continue;
        }

        const columnsGroup = relation.children?.find((child) => child.id.includes(":columns"));
        const columnNames = (columnsGroup?.children ?? []).slice(0, 20).map((column) => column.label);
        lines.push(`${relation.type.toUpperCase()} ${relation.label} (${columnNames.join(", ")})`);
      }
    }
  }

  return lines.slice(0, 120).join("\n") || "No schema details available.";
}
