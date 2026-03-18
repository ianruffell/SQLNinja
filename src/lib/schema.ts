import type {
  ConnectionConfig,
  ConnectionDraft,
  SavedConnection,
  SchemaNode,
  SchemaPayload,
} from "../types";

export function normalizeSavedConnection(value: unknown): SavedConnection | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
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
        : 3306;

  return {
    id: typeof candidate.id === "string" && candidate.id ? candidate.id : crypto.randomUUID(),
    name:
      typeof candidate.name === "string" && candidate.name.trim()
        ? candidate.name.trim()
        : `${user}@${host}`,
    host,
    port: Number.isFinite(portCandidate) && portCandidate > 0 ? portCandidate : 3306,
    user,
    password: typeof candidate.password === "string" ? candidate.password : "",
  };
}

export function toDraft(connection: SavedConnection): ConnectionDraft {
  return {
    name: connection.name,
    host: connection.host,
    port: connection.port,
    user: connection.user,
    password: connection.password,
  };
}

export function summarizeConnection(connection: SavedConnection) {
  return `${connection.user}@${connection.host}:${connection.port}`;
}

export function sanitizeDraft(draft: ConnectionDraft) {
  return {
    ...draft,
    name: draft.name.trim() || `${draft.user}@${draft.host}`,
    host: draft.host.trim(),
    port: Number.isFinite(draft.port) && draft.port > 0 ? draft.port : 3306,
    user: draft.user.trim(),
  };
}

export function toConnectionPayload(connection: SavedConnection | ConnectionConfig, database?: string | null) {
  return {
    host: connection.host.trim(),
    port: Number.isFinite(connection.port) && connection.port > 0 ? connection.port : 3306,
    user: connection.user.trim(),
    password: connection.password,
    database: database?.trim() ? database.trim() : undefined,
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
