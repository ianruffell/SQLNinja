import { FormEvent, useEffect, useId, useMemo, useRef, useState } from "react";

type ConnectionConfig = {
  host: string;
  port: number;
  user: string;
  password: string;
};

type SavedConnection = ConnectionConfig & {
  id: string;
  name: string;
};

type SchemaNode = {
  id: string;
  label: string;
  type: "database" | "group" | "table" | "view" | "column" | "index";
  description?: string;
  reference?: string;
  children?: SchemaNode[];
};

type SchemaPayload = {
  databases: string[];
  tree: SchemaNode;
};

type QueryStatementResult =
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

type CellResult = {
  statements: QueryStatementResult[];
  executedAt: string;
  durationMs: number;
};

type NotebookCell = {
  id: string;
  title: string;
  sql: string;
  status: "idle" | "running" | "success" | "error";
  result?: CellResult;
  error?: string;
};

type NotebookTab = {
  id: string;
  title: string;
  cells: NotebookCell[];
  activeCellId: string | null;
};

type ConnectionStatus = "idle" | "loading" | "ready" | "error";

type WorkspaceState = {
  schema: SchemaNode | null;
  databases: string[];
  selectedDatabase: string | null;
  tabs: NotebookTab[];
  activeTabId: string | null;
  connectionStatus: ConnectionStatus;
  connectionMessage: string;
  aiCollapsed: boolean;
  aiModels: string[];
  aiSelectedModel: string | null;
  aiPrompt: string;
  aiStatus: ConnectionStatus;
  aiMessage: string;
  aiNotes: string;
};

type ConnectionDraft = {
  name: string;
  host: string;
  port: number;
  user: string;
  password: string;
};

type DraftTestState = {
  status: "idle" | "loading" | "success" | "error";
  message: string;
};

type Page = "connections" | "workspace";
type Theme = "dark" | "light";

type PersistedNotebookCell = Pick<NotebookCell, "id" | "title" | "sql">;

type PersistedNotebookTab = {
  id: string;
  title: string;
  cells: PersistedNotebookCell[];
  activeCellId: string | null;
};

type PersistedWorkspaceState = {
  tabs: PersistedNotebookTab[];
  activeTabId: string | null;
  selectedDatabase: string | null;
};

type SqlSuggestionKind = "keyword" | "database" | "table" | "view" | "column";

type SqlAutocompleteContext = {
  databases: string[];
  relations: Array<{
    database: string;
    name: string;
    type: "table" | "view";
    columns: string[];
  }>;
};

type SqlAutocompleteQuery = {
  start: number;
  end: number;
  token: string;
  prefix: string;
  qualifier: string | null;
  previousKeyword: string | null;
};

type SqlSuggestion = {
  kind: SqlSuggestionKind;
  label: string;
  insertText: string;
  detail: string;
  searchText: string;
  priority: number;
};

const STORAGE_KEY = "sqlninja.saved-connections";
const ACTIVE_CONNECTION_STORAGE_KEY = "sqlninja.active-connection-id";
const THEME_STORAGE_KEY = "sqlninja.theme";
const NOTEBOOK_COOKIE_KEY = "sqlninja.notebooks";
const NOTEBOOK_COOKIE_MANIFEST_KEY = "sqlninja.notebooks.chunks";
const NOTEBOOK_COOKIE_MAX_AGE = 60 * 60 * 24 * 365;
const NOTEBOOK_COOKIE_CHUNK_SIZE = 3200;
const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT JOIN",
  "RIGHT JOIN",
  "INNER JOIN",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "INSERT INTO",
  "UPDATE",
  "DELETE",
  "CREATE TABLE",
  "ALTER TABLE",
  "DROP TABLE",
  "USE",
  "SHOW TABLES",
  "SHOW DATABASES",
  "DESCRIBE",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "DISTINCT",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "AND",
  "OR",
  "NOT",
  "IN",
  "EXISTS",
  "AS",
  "ON",
  "VALUES",
  "SET",
];
const RELATION_CONTEXT_KEYWORDS = new Set([
  "FROM",
  "JOIN",
  "UPDATE",
  "INTO",
  "TABLE",
  "DESCRIBE",
  "DESC",
  "TRUNCATE",
  "USE",
]);
const COLUMN_CONTEXT_KEYWORDS = new Set([
  "SELECT",
  "WHERE",
  "AND",
  "OR",
  "ON",
  "SET",
  "BY",
  "HAVING",
  "ORDER",
  "GROUP",
]);

const initialConnection: ConnectionConfig = {
  host: "127.0.0.1",
  port: 3306,
  user: "root",
  password: "",
};

const initialDraft: ConnectionDraft = {
  name: "Local MariaDB",
  ...initialConnection,
};

function createCell(index: number, snapshot?: PersistedNotebookCell): NotebookCell {
  return {
    id: snapshot?.id ?? crypto.randomUUID(),
    title: snapshot?.title?.trim() || `Notebook ${index}`,
    sql: snapshot?.sql ?? "",
    status: "idle",
  };
}

function createTab(index: number, snapshot?: PersistedNotebookTab): NotebookTab {
  const cells = Array.isArray(snapshot?.cells)
    ? snapshot.cells
        .map((cell, cellIndex) => createCell(cellIndex + 1, cell))
        .filter((cell) => typeof cell.id === "string" && cell.id.length > 0)
    : [];
  const firstCell = cells[0] ?? createCell(1);
  return {
    id: snapshot?.id ?? crypto.randomUUID(),
    title: snapshot?.title?.trim() || `Notebook ${index}`,
    cells: cells.length > 0 ? cells : [firstCell],
    activeCellId:
      snapshot?.activeCellId && cells.some((cell) => cell.id === snapshot.activeCellId)
        ? snapshot.activeCellId
        : firstCell.id,
  };
}

function createWorkspaceState(snapshot?: PersistedWorkspaceState): WorkspaceState {
  const tabs = Array.isArray(snapshot?.tabs)
    ? snapshot.tabs
        .map((tab, tabIndex) => createTab(tabIndex + 1, tab))
        .filter((tab) => typeof tab.id === "string" && tab.id.length > 0)
    : [];
  const firstTab = tabs[0] ?? createTab(1);
  return {
    schema: null,
    databases: [],
    selectedDatabase: typeof snapshot?.selectedDatabase === "string" && snapshot.selectedDatabase
      ? snapshot.selectedDatabase
      : null,
    tabs: tabs.length > 0 ? tabs : [firstTab],
    activeTabId:
      snapshot?.activeTabId && tabs.some((tab) => tab.id === snapshot.activeTabId)
        ? snapshot.activeTabId
        : firstTab.id,
    connectionStatus: "idle",
    connectionMessage: "Open a workspace to discover databases on this server.",
    aiCollapsed: false,
    aiModels: [],
    aiSelectedModel: null,
    aiPrompt: "",
    aiStatus: "idle",
    aiMessage: "Connect to Ollama to generate or optimize SQL with AI.",
    aiNotes: "",
  };
}

function getActiveTab(workspace: WorkspaceState) {
  return workspace.tabs.find((tab) => tab.id === workspace.activeTabId) ?? workspace.tabs[0] ?? null;
}

function loadConnections(): SavedConnection[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .map((item) => normalizeSavedConnection(item))
      .filter((item): item is SavedConnection => item !== null);
  } catch {
    return [];
  }
}

function loadActiveConnectionId() {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(ACTIVE_CONNECTION_STORAGE_KEY);
    return raw && raw.length > 0 ? raw : null;
  } catch {
    return null;
  }
}

function loadTheme(): Theme {
  if (typeof window === "undefined") {
    return "dark";
  }

  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "dark" || stored === "light") {
      return stored;
    }
  } catch {
    return "dark";
  }

  return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
}

function getCookie(name: string) {
  if (typeof document === "undefined") {
    return null;
  }

  const prefix = `${name}=`;
  for (const part of document.cookie.split("; ")) {
    if (part.startsWith(prefix)) {
      return part.slice(prefix.length);
    }
  }

  return null;
}

function setCookie(name: string, value: string) {
  if (typeof document === "undefined") {
    return;
  }

  document.cookie = `${name}=${value}; path=/; max-age=${NOTEBOOK_COOKIE_MAX_AGE}; SameSite=Lax`;
}

function deleteCookie(name: string) {
  if (typeof document === "undefined") {
    return;
  }

  document.cookie = `${name}=; path=/; max-age=0; SameSite=Lax`;
}

function encodeCookiePayload(value: string) {
  if (typeof window === "undefined") {
    return "";
  }

  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }

  return btoa(binary);
}

function decodeCookiePayload(value: string) {
  if (typeof window === "undefined") {
    return "";
  }

  const binary = atob(value);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

function chunkValue(value: string, size: number) {
  const chunks: string[] = [];
  for (let index = 0; index < value.length; index += size) {
    chunks.push(value.slice(index, index + size));
  }
  return chunks;
}

function normalizePersistedCell(value: unknown): PersistedNotebookCell | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  if (typeof candidate.id !== "string" || candidate.id.length === 0) {
    return null;
  }

  return {
    id: candidate.id,
    title: typeof candidate.title === "string" && candidate.title.trim() ? candidate.title.trim() : "Notebook 1",
    sql: typeof candidate.sql === "string" ? candidate.sql : "",
  };
}

function normalizePersistedTab(value: unknown): PersistedNotebookTab | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  if (typeof candidate.id !== "string" || candidate.id.length === 0) {
    return null;
  }

  const cells = Array.isArray(candidate.cells)
    ? candidate.cells
        .map((cell) => normalizePersistedCell(cell))
        .filter((cell): cell is PersistedNotebookCell => cell !== null)
    : [];

  return {
    id: candidate.id,
    title: typeof candidate.title === "string" && candidate.title.trim() ? candidate.title.trim() : "Notebook 1",
    cells,
    activeCellId: typeof candidate.activeCellId === "string" && candidate.activeCellId ? candidate.activeCellId : null,
  };
}

function normalizePersistedWorkspace(value: unknown): PersistedWorkspaceState | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  const tabs = Array.isArray(candidate.tabs)
    ? candidate.tabs
        .map((tab) => normalizePersistedTab(tab))
        .filter((tab): tab is PersistedNotebookTab => tab !== null)
    : [];

  return {
    tabs,
    activeTabId: typeof candidate.activeTabId === "string" && candidate.activeTabId ? candidate.activeTabId : null,
    selectedDatabase:
      typeof candidate.selectedDatabase === "string" && candidate.selectedDatabase ? candidate.selectedDatabase : null,
  };
}

function loadPersistedWorkspaces(connections: SavedConnection[]) {
  if (typeof document === "undefined") {
    return {};
  }

  try {
    const chunkCount = Number(getCookie(NOTEBOOK_COOKIE_MANIFEST_KEY) ?? "0");
    if (!Number.isFinite(chunkCount) || chunkCount <= 0) {
      return {};
    }

    let encoded = "";
    for (let index = 0; index < chunkCount; index += 1) {
      const chunk = getCookie(`${NOTEBOOK_COOKIE_KEY}.${index}`);
      if (!chunk) {
        return {};
      }
      encoded += chunk;
    }

    const parsed = JSON.parse(decodeCookiePayload(encoded)) as unknown;
    if (typeof parsed !== "object" || parsed === null) {
      return {};
    }

    const connectionIds = new Set(connections.map((connection) => connection.id));
    return Object.entries(parsed as Record<string, unknown>).reduce<Record<string, WorkspaceState>>((accumulator, [key, value]) => {
      if (!connectionIds.has(key)) {
        return accumulator;
      }

      const normalized = normalizePersistedWorkspace(value);
      if (normalized) {
        accumulator[key] = createWorkspaceState(normalized);
      }
      return accumulator;
    }, {});
  } catch {
    return {};
  }
}

function createPersistedWorkspaces(workspaces: Record<string, WorkspaceState>, connections: SavedConnection[]) {
  return connections.reduce<Record<string, PersistedWorkspaceState>>((accumulator, connection) => {
    const workspace = workspaces[connection.id];
    if (!workspace) {
      return accumulator;
    }

    accumulator[connection.id] = {
      selectedDatabase: workspace.selectedDatabase,
      activeTabId: workspace.activeTabId,
      tabs: workspace.tabs.map((tab) => ({
        id: tab.id,
        title: tab.title,
        activeCellId: tab.activeCellId,
        cells: tab.cells.map((cell) => ({
          id: cell.id,
          title: cell.title,
          sql: cell.sql,
        })),
      })),
    };

    return accumulator;
  }, {});
}

function persistWorkspacesToCookies(workspaces: Record<string, WorkspaceState>, connections: SavedConnection[]) {
  if (typeof document === "undefined") {
    return;
  }

  const serialized = JSON.stringify(createPersistedWorkspaces(workspaces, connections));
  const encoded = encodeCookiePayload(serialized);
  const chunks = chunkValue(encoded, NOTEBOOK_COOKIE_CHUNK_SIZE);
  const previousChunkCount = Number(getCookie(NOTEBOOK_COOKIE_MANIFEST_KEY) ?? "0");

  chunks.forEach((chunk, index) => {
    setCookie(`${NOTEBOOK_COOKIE_KEY}.${index}`, chunk);
  });

  for (let index = chunks.length; index < previousChunkCount; index += 1) {
    deleteCookie(`${NOTEBOOK_COOKIE_KEY}.${index}`);
  }

  if (chunks.length === 0) {
    deleteCookie(NOTEBOOK_COOKIE_MANIFEST_KEY);
    return;
  }

  setCookie(NOTEBOOK_COOKIE_MANIFEST_KEY, String(chunks.length));
}

function normalizeSavedConnection(value: unknown): SavedConnection | null {
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

function toDraft(connection: SavedConnection): ConnectionDraft {
  return {
    name: connection.name,
    host: connection.host,
    port: connection.port,
    user: connection.user,
    password: connection.password,
  };
}

function summarizeConnection(connection: SavedConnection) {
  return `${connection.user}@${connection.host}:${connection.port}`;
}

function sanitizeDraft(draft: ConnectionDraft) {
  return {
    ...draft,
    name: draft.name.trim() || `${draft.user}@${draft.host}`,
    host: draft.host.trim(),
    port: Number.isFinite(draft.port) && draft.port > 0 ? draft.port : 3306,
    user: draft.user.trim(),
  };
}

function toConnectionPayload(connection: SavedConnection | ConnectionConfig, database?: string | null) {
  return {
    host: connection.host.trim(),
    port: Number.isFinite(connection.port) && connection.port > 0 ? connection.port : 3306,
    user: connection.user.trim(),
    password: connection.password,
    database: database?.trim() ? database.trim() : undefined,
  };
}

function formatApiError(payload: unknown, fallback: string) {
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

function normalizeSchemaPayload(payload: unknown): SchemaPayload {
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

function summarizeSchemaForAi(schema: SchemaNode | null, selectedDatabase: string | null) {
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

function stripIdentifierQuotes(value: string) {
  return value.replaceAll("`", "").replaceAll('"', "").trim();
}

function formatIdentifier(value: string) {
  return `\`${value.replaceAll("`", "``")}\``;
}

function buildSqlAutocompleteContext(schema: SchemaNode | null, selectedDatabase: string | null): SqlAutocompleteContext {
  if (!schema?.children) {
    return { databases: [], relations: [] };
  }

  const databases: string[] = [];
  const relations: SqlAutocompleteContext["relations"] = [];

  for (const databaseNode of schema.children) {
    if (databaseNode.type !== "database") {
      continue;
    }

    if (selectedDatabase && databaseNode.label !== selectedDatabase) {
      continue;
    }

    databases.push(databaseNode.label);

    for (const group of databaseNode.children ?? []) {
      if (group.type !== "group") {
        continue;
      }

      for (const relation of group.children ?? []) {
        if (relation.type !== "table" && relation.type !== "view") {
          continue;
        }

        const columnGroup = relation.children?.find((child) => child.id.includes(":columns"));
        relations.push({
          database: databaseNode.label,
          name: relation.label,
          type: relation.type,
          columns: (columnGroup?.children ?? []).map((column) => column.label),
        });
      }
    }
  }

  return { databases, relations };
}

function getSqlAutocompleteQuery(sql: string, caretPosition: number): SqlAutocompleteQuery {
  const safeCaret = Math.max(0, Math.min(caretPosition, sql.length));
  const beforeCaret = sql.slice(0, safeCaret);
  const tokenMatch = beforeCaret.match(/(?:`[^`]*`?|[A-Za-z_][\w$]*)(?:\.(?:`[^`]*`?|[A-Za-z_][\w$]*)?)?$/);
  const token = tokenMatch?.[0] ?? "";
  const start = safeCaret - token.length;
  const beforeToken = beforeCaret.slice(0, start);
  const previousKeywordMatches = beforeToken.match(/[A-Za-z_]+/g) ?? [];
  const previousKeyword = previousKeywordMatches.at(-1)?.toUpperCase() ?? null;
  const tokenParts = token.split(".");
  const qualifier = tokenParts.length > 1 ? stripIdentifierQuotes(tokenParts[0] ?? "") : null;
  const prefix = stripIdentifierQuotes(tokenParts.at(-1) ?? "");

  return {
    start,
    end: safeCaret,
    token,
    prefix,
    qualifier: qualifier ? qualifier.toLowerCase() : null,
    previousKeyword,
  };
}

function scoreSuggestion(suggestion: SqlSuggestion, prefix: string) {
  const query = prefix.trim().toLowerCase();
  if (!query) {
    return suggestion.priority;
  }

  const label = suggestion.label.toLowerCase();
  const detail = suggestion.detail.toLowerCase();

  if (label === query) {
    return suggestion.priority + 120;
  }

  if (label.startsWith(query)) {
    return suggestion.priority + 90;
  }

  if (detail.startsWith(query)) {
    return suggestion.priority + 60;
  }

  if (suggestion.searchText.includes(query)) {
    return suggestion.priority + 30;
  }

  return suggestion.priority;
}

function buildSqlAutocompleteSuggestions(
  context: SqlAutocompleteContext,
  query: SqlAutocompleteQuery,
  forceOpen: boolean,
): SqlSuggestion[] {
  if (!forceOpen && query.token.trim().length === 0) {
    return [];
  }

  const suggestions: SqlSuggestion[] = [];
  const prefix = query.prefix.toLowerCase();
  const relationContext = query.previousKeyword ? RELATION_CONTEXT_KEYWORDS.has(query.previousKeyword) : false;
  const columnContext = query.previousKeyword ? COLUMN_CONTEXT_KEYWORDS.has(query.previousKeyword) : false;

  if (query.qualifier) {
    const matchingRelations = context.relations.filter(
      (relation) =>
        relation.name.toLowerCase() === query.qualifier ||
        `${relation.database}.${relation.name}`.toLowerCase() === query.qualifier,
    );

    for (const relation of matchingRelations) {
      for (const column of relation.columns) {
        suggestions.push({
          kind: "column",
          label: column,
          insertText: `${formatIdentifier(relation.name)}.${formatIdentifier(column)}`,
          detail: `${relation.database}.${relation.name}`,
          searchText: `${column} ${relation.name} ${relation.database}`.toLowerCase(),
          priority: 120,
        });
      }
    }

    const matchingDatabases = context.databases.filter((database) => database.toLowerCase() === query.qualifier);
    for (const database of matchingDatabases) {
      for (const relation of context.relations.filter((item) => item.database === database)) {
        suggestions.push({
          kind: relation.type,
          label: relation.name,
          insertText: `${formatIdentifier(database)}.${formatIdentifier(relation.name)}`,
          detail: database,
          searchText: `${relation.name} ${database}`.toLowerCase(),
          priority: 110,
        });
      }
    }
  } else {
    if (!relationContext || forceOpen) {
      for (const keyword of SQL_KEYWORDS) {
        suggestions.push({
          kind: "keyword",
          label: keyword,
          insertText: keyword,
          detail: "SQL keyword",
          searchText: keyword.toLowerCase(),
          priority: columnContext ? 20 : 80,
        });
      }
    }

    if (query.previousKeyword === "USE" || forceOpen) {
      for (const database of context.databases) {
        suggestions.push({
          kind: "database",
          label: database,
          insertText: formatIdentifier(database),
          detail: "database",
          searchText: database.toLowerCase(),
          priority: 95,
        });
      }
    }

    if (relationContext || !columnContext || forceOpen) {
      for (const relation of context.relations) {
        suggestions.push({
          kind: relation.type,
          label: relation.name,
          insertText: formatIdentifier(relation.name),
          detail: relation.database,
          searchText: `${relation.name} ${relation.database}`.toLowerCase(),
          priority: relationContext ? 130 : 70,
        });
      }
    }

    if (columnContext || !relationContext || forceOpen) {
      for (const relation of context.relations) {
        for (const column of relation.columns) {
          suggestions.push({
            kind: "column",
            label: column,
            insertText: formatIdentifier(column),
            detail: `${relation.database}.${relation.name}`,
            searchText: `${column} ${relation.name} ${relation.database}`.toLowerCase(),
            priority: columnContext ? 125 : 60,
          });
        }
      }
    }
  }

  const uniqueSuggestions = new Map<string, SqlSuggestion>();
  for (const suggestion of suggestions) {
    const key = `${suggestion.kind}:${suggestion.insertText}:${suggestion.detail}`;
    const current = uniqueSuggestions.get(key);
    if (!current || current.priority < suggestion.priority) {
      uniqueSuggestions.set(key, suggestion);
    }
  }

  return [...uniqueSuggestions.values()]
    .filter((suggestion) => {
      if (!prefix) {
        return true;
      }

      return (
        suggestion.label.toLowerCase().includes(prefix) ||
        suggestion.detail.toLowerCase().includes(prefix) ||
        suggestion.searchText.includes(prefix)
      );
    })
    .sort((left, right) => {
      const scoreDifference = scoreSuggestion(right, prefix) - scoreSuggestion(left, prefix);
      if (scoreDifference !== 0) {
        return scoreDifference;
      }

      return left.label.localeCompare(right.label);
    })
    .slice(0, 12);
}

export function App() {
  const [theme, setTheme] = useState<Theme>(() => loadTheme());
  const [page, setPage] = useState<Page>(() => (loadConnections().length > 0 ? "workspace" : "connections"));
  const [connections, setConnections] = useState<SavedConnection[]>(() => loadConnections());
  const [draft, setDraft] = useState<ConnectionDraft>(initialDraft);
  const [draftTestState, setDraftTestState] = useState<DraftTestState>({
    status: "idle",
    message: "",
  });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [activeConnectionId, setActiveConnectionId] = useState<string | null>(() => {
    const existing = loadConnections();
    const storedActiveId = loadActiveConnectionId();
    return existing.some((connection) => connection.id === storedActiveId)
      ? storedActiveId
      : (existing[0]?.id ?? null);
  });
  const [workspaces, setWorkspaces] = useState<Record<string, WorkspaceState>>(() =>
    loadPersistedWorkspaces(loadConnections()),
  );
  const hasReloadedSchemaRef = useRef(false);

  const activeConnection = useMemo(
    () => connections.find((connection) => connection.id === activeConnectionId) ?? null,
    [activeConnectionId, connections],
  );

  const activeWorkspace = activeConnectionId
    ? (workspaces[activeConnectionId] ?? createWorkspaceState())
    : null;
  const activeTab = activeWorkspace ? getActiveTab(activeWorkspace) : null;
  const activeNotebookCell = activeTab
    ? (activeTab.cells.find((cell) => cell.id === activeTab.activeCellId) ?? activeTab.cells[0] ?? null)
    : null;
  const activeSqlAutocompleteContext = useMemo(
    () => buildSqlAutocompleteContext(activeWorkspace?.schema ?? null, activeWorkspace?.selectedDatabase ?? null),
    [activeWorkspace?.schema, activeWorkspace?.selectedDatabase],
  );

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(connections));
  }, [connections]);

  useEffect(() => {
    if (activeConnectionId) {
      window.localStorage.setItem(ACTIVE_CONNECTION_STORAGE_KEY, activeConnectionId);
    } else {
      window.localStorage.removeItem(ACTIVE_CONNECTION_STORAGE_KEY);
    }
  }, [activeConnectionId]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

  useEffect(() => {
    persistWorkspacesToCookies(workspaces, connections);
  }, [connections, workspaces]);

  useEffect(() => {
    setWorkspaces((current) => {
      const next = { ...current };
      let changed = false;

      for (const connection of connections) {
        if (!next[connection.id]) {
          next[connection.id] = createWorkspaceState();
          changed = true;
        }
      }

      for (const connectionId of Object.keys(next)) {
        if (!connections.some((connection) => connection.id === connectionId)) {
          delete next[connectionId];
          changed = true;
        }
      }

      return changed ? next : current;
    });
  }, [connections]);

  useEffect(() => {
    if (!activeConnectionId && connections.length > 0) {
      setActiveConnectionId(connections[0].id);
    }

    if (connections.length === 0) {
      setPage("connections");
    }
  }, [activeConnectionId, connections]);

  useEffect(() => {
    if (connections.length === 0) {
      hasReloadedSchemaRef.current = false;
      return;
    }

    if (!activeConnection || hasReloadedSchemaRef.current) {
      return;
    }

    hasReloadedSchemaRef.current = true;
    void discoverSchemaForConnection(activeConnection, false);
  }, [activeConnection, connections.length]);

  function updateWorkspace(connectionId: string, updater: (workspace: WorkspaceState) => WorkspaceState) {
    setWorkspaces((current) => ({
      ...current,
      [connectionId]: updater(current[connectionId] ?? createWorkspaceState()),
    }));
  }

  function resetDraft() {
    setDraft(initialDraft);
    setDraftTestState({ status: "idle", message: "" });
    setEditingId(null);
  }

  function updateDraft(next: ConnectionDraft) {
    setDraft(next);
    setDraftTestState({ status: "idle", message: "" });
  }

  function saveConnection() {
    const normalized = sanitizeDraft(draft);
    const connectionId = editingId ?? crypto.randomUUID();

    const nextConnection: SavedConnection = {
      id: connectionId,
      ...normalized,
    };

    setConnections((current) => {
      if (editingId) {
        return current.map((connection) => (connection.id === editingId ? nextConnection : connection));
      }

      return [nextConnection, ...current];
    });

    setActiveConnectionId(connectionId);
    setEditingId(connectionId);
    setDraft(toDraft(nextConnection));
    updateWorkspace(connectionId, (workspace) => ({
      ...workspace,
      connectionStatus: "idle",
      connectionMessage: "Connection saved. Test it or open the workspace to discover databases.",
    }));
    return nextConnection;
  }

  async function discoverSchemaForConnection(connection: SavedConnection, openWorkspace = false) {
    const connectionId = connection.id;
    setActiveConnectionId(connectionId);
    if (openWorkspace) {
      setPage("workspace");
    }

    updateWorkspace(connectionId, (workspace) => ({
      ...workspace,
      connectionStatus: "loading",
      connectionMessage: `Connecting to ${connection.name} and discovering databases...`,
    }));

    try {
      const response = await fetch("/api/schema", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(toConnectionPayload(connection)),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, "Connection failed"));
      }

      const schemaPayload = normalizeSchemaPayload(payload);
      updateWorkspace(connectionId, (workspace) => {
        const selectedDatabase =
          workspace.selectedDatabase && schemaPayload.databases.includes(workspace.selectedDatabase)
            ? workspace.selectedDatabase
            : (schemaPayload.databases[0] ?? null);

        return {
          ...workspace,
          schema: schemaPayload.tree,
          databases: schemaPayload.databases,
          selectedDatabase,
          connectionStatus: "ready",
          connectionMessage:
            schemaPayload.databases.length > 0
              ? `Discovered ${schemaPayload.databases.length} database(s) on ${connection.host}.`
              : "Connected, but no non-system databases were discovered.",
        };
      });
      await loadAiModels(connectionId);
    } catch (error) {
      updateWorkspace(connectionId, (workspace) => ({
        ...workspace,
        schema: null,
        databases: [],
        selectedDatabase: null,
        connectionStatus: "error",
        connectionMessage: error instanceof Error ? error.message : "Connection failed",
      }));
    }
  }

  async function loadAiModels(connectionId: string) {
    updateWorkspace(connectionId, (workspace) => ({
      ...workspace,
      aiStatus: "loading",
      aiMessage: "Loading Ollama models...",
    }));

    try {
      const response = await fetch("/api/ai/models");
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, "Failed to load Ollama models"));
      }

      const models = Array.isArray(payload.models)
        ? payload.models.filter((item: unknown): item is string => typeof item === "string")
        : [];

      updateWorkspace(connectionId, (workspace) => ({
        ...workspace,
        aiModels: models,
        aiSelectedModel:
          workspace.aiSelectedModel && models.includes(workspace.aiSelectedModel)
            ? workspace.aiSelectedModel
            : (models[0] ?? null),
        aiStatus: "ready",
        aiMessage:
          models.length > 0
            ? `Ollama connected. ${models.length} model(s) available.`
            : "Ollama reachable, but no models are installed yet.",
      }));
    } catch (error) {
      updateWorkspace(connectionId, (workspace) => ({
        ...workspace,
        aiModels: [],
        aiSelectedModel: null,
        aiStatus: "error",
        aiMessage: error instanceof Error ? error.message : "Failed to load Ollama models",
      }));
    }
  }

  async function discoverSchema(connectionId: string, openWorkspace = false) {
    const connection = connections.find((item) => item.id === connectionId);
    if (!connection) {
      return;
    }

    await discoverSchemaForConnection(connection, openWorkspace);
  }

  async function saveAndOpenWorkspace() {
    const connection = saveConnection();
    await discoverSchemaForConnection(connection, true);
  }

  async function testDraftConnection() {
    setDraftTestState({
      status: "loading",
      message: "Testing server connection...",
    });

    try {
      const response = await fetch("/api/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(toConnectionPayload(sanitizeDraft(draft))),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, "Connection test failed"));
      }

      const databases = Array.isArray(payload.databases)
        ? payload.databases.filter((item: unknown): item is string => typeof item === "string")
        : [];
      const serverVersion =
        typeof payload.serverVersion === "string" && payload.serverVersion.length > 0
          ? payload.serverVersion
          : "unknown";

      setDraftTestState({
        status: "success",
        message: `Connected to server ${draft.host}:${draft.port}. MariaDB/MySQL ${serverVersion}. Accessible databases: ${databases.length}.`,
      });
    } catch (error) {
      setDraftTestState({
        status: "error",
        message: error instanceof Error ? error.message : "Connection test failed",
      });
    }
  }

  function updateCell(connectionId: string, tabId: string, cellId: string, changes: Partial<NotebookCell>) {
    updateWorkspace(connectionId, (workspace) => ({
      ...workspace,
      tabs: workspace.tabs.map((tab) =>
        tab.id === tabId
          ? {
              ...tab,
              cells: tab.cells.map((cell) => (cell.id === cellId ? { ...cell, ...changes } : cell)),
            }
          : tab,
      ),
    }));
  }

  async function runCell(connectionId: string, tabId: string, cellId: string) {
    const connection = connections.find((item) => item.id === connectionId);
    const workspace = workspaces[connectionId] ?? createWorkspaceState();
    const tab = workspace.tabs.find((item) => item.id === tabId);
    const cell = tab?.cells.find((item) => item.id === cellId);

    if (!connection || !cell) {
      return;
    }

    updateCell(connectionId, tabId, cellId, { status: "running", error: undefined });

    try {
      const response = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...toConnectionPayload(connection, workspace.selectedDatabase),
          sql: cell.sql,
        }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, "Query failed"));
      }

      updateCell(connectionId, tabId, cellId, {
        status: "success",
        result: payload,
        error: undefined,
      });
    } catch (error) {
      updateCell(connectionId, tabId, cellId, {
        status: "error",
        error: error instanceof Error ? error.message : "Query failed",
      });
    }
  }

  async function runAiAction(connectionId: string, action: "generate" | "optimize") {
    const connection = connections.find((item) => item.id === connectionId);
    const workspace = workspaces[connectionId] ?? createWorkspaceState();
    const tab = getActiveTab(workspace);
    const cell = tab?.cells.find((item) => item.id === tab.activeCellId) ?? tab?.cells[0] ?? null;

    if (!connection || !workspace.aiSelectedModel || !tab || !cell) {
      return;
    }

    updateWorkspace(connectionId, (current) => ({
      ...current,
      aiStatus: "loading",
      aiMessage: action === "generate" ? "Generating SQL with Ollama..." : "Optimizing SQL with Ollama...",
      aiNotes: "",
    }));

    try {
      const response = await fetch(action === "generate" ? "/api/ai/generate-sql" : "/api/ai/optimize-sql", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: workspace.aiSelectedModel,
          selectedDatabase: workspace.selectedDatabase,
          schemaSummary: summarizeSchemaForAi(workspace.schema, workspace.selectedDatabase),
          prompt: workspace.aiPrompt,
          sql: action === "optimize" ? cell.sql : undefined,
        }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, `Failed to ${action === "generate" ? "generate" : "optimize"} SQL`));
      }

      updateCell(connectionId, tab.id, cell.id, {
        sql: typeof payload.sql === "string" ? payload.sql : cell.sql,
      });

      updateWorkspace(connectionId, (current) => ({
        ...current,
        aiStatus: "ready",
        aiMessage: action === "generate" ? "SQL generated by Ollama." : "SQL optimized by Ollama.",
        aiNotes: typeof payload.notes === "string" ? payload.notes : "",
      }));
    } catch (error) {
      updateWorkspace(connectionId, (current) => ({
        ...current,
        aiStatus: "error",
        aiMessage:
          error instanceof Error
            ? error.message
            : `Failed to ${action === "generate" ? "generate" : "optimize"} SQL`,
        aiNotes: "",
      }));
    }
  }

  function addCell(connectionId: string, tabId: string) {
    updateWorkspace(connectionId, (workspace) => {
      return {
        ...workspace,
        tabs: workspace.tabs.map((tab) => {
          if (tab.id !== tabId) {
            return tab;
          }

          const nextCell = createCell(tab.cells.length + 1);
          return {
            ...tab,
            cells: [...tab.cells, nextCell],
            activeCellId: nextCell.id,
          };
        }),
      };
    });
  }

  function removeCell(connectionId: string, tabId: string, cellId: string) {
    updateWorkspace(connectionId, (workspace) => {
      return {
        ...workspace,
        tabs: workspace.tabs.map((tab) => {
          if (tab.id !== tabId) {
            return tab;
          }

          const remaining = tab.cells.filter((cell) => cell.id !== cellId);
          if (remaining.length === 0) {
            const fallbackCell = createCell(1);
            return {
              ...tab,
              cells: [fallbackCell],
              activeCellId: fallbackCell.id,
            };
          }

          return {
            ...tab,
            cells: remaining,
            activeCellId: tab.activeCellId === cellId ? (remaining[0]?.id ?? null) : tab.activeCellId,
          };
        }),
      };
    });
  }

  function addTab(connectionId: string) {
    updateWorkspace(connectionId, (workspace) => {
      const nextTab = createTab(workspace.tabs.length + 1);
      return {
        ...workspace,
        tabs: [...workspace.tabs, nextTab],
        activeTabId: nextTab.id,
      };
    });
  }

  function removeTab(connectionId: string, tabId: string) {
    updateWorkspace(connectionId, (workspace) => {
      const remaining = workspace.tabs.filter((tab) => tab.id !== tabId);
      if (remaining.length === 0) {
        const fallbackTab = createTab(1);
        return {
          ...workspace,
          tabs: [fallbackTab],
          activeTabId: fallbackTab.id,
        };
      }

      return {
        ...workspace,
        tabs: remaining,
        activeTabId: workspace.activeTabId === tabId ? remaining[0]?.id ?? null : workspace.activeTabId,
      };
    });
  }

  function insertReference(node: SchemaNode) {
    if (!activeConnectionId || !activeTab?.activeCellId) {
      return;
    }

    const currentCell = activeTab.cells.find((cell) => cell.id === activeTab.activeCellId);
    if (!currentCell) {
      return;
    }

    const spacer = currentCell.sql.trim().length > 0 ? " " : "";
    updateCell(activeConnectionId, activeTab.id, activeTab.activeCellId, {
      sql: `${currentCell.sql}${spacer}${node.reference ?? `\`${node.label}\``}`,
    });
  }

  function editConnection(connection: SavedConnection) {
    setDraft(toDraft(connection));
    setDraftTestState({ status: "idle", message: "" });
    setEditingId(connection.id);
    setPage("connections");
  }

  function deleteConnection(connectionId: string) {
    setConnections((current) => {
      const next = current.filter((connection) => connection.id !== connectionId);
      if (activeConnectionId === connectionId) {
        setActiveConnectionId(next[0]?.id ?? null);
      }
      return next;
    });

    if (editingId === connectionId) {
      resetDraft();
    }
  }

  async function openConnectionWorkspace(connectionId: string) {
    await discoverSchema(connectionId, true);
  }

  const activeWorkspaceStatus = activeWorkspace?.connectionStatus ?? "idle";
  const canRunQueries = activeWorkspaceStatus === "ready";

  return (
    <div className="screen-shell">
      <header className="topbar card">
        <div className="topbar-brand">
          <div className="topbar-title-row">
            <img className="brand-logo" src="/sql-ninja-logo.svg" alt="" />
            <p className="eyebrow">SQL Ninja</p>
          </div>
        </div>
        <div className="topbar-actions">
          {page === "workspace" && activeConnection && activeWorkspace ? (
            <div className="topbar-group">
              {activeWorkspace.databases.length > 0 ? (
                <label className="topbar-inline-field" htmlFor="topbar-database-context">
                  <span className="topbar-field-label">Database</span>
                  <select
                    id="topbar-database-context"
                    className="select-input topbar-select-input"
                    value={activeWorkspace.selectedDatabase ?? ""}
                    onChange={(event) =>
                      updateWorkspace(activeConnection.id, (workspace) => ({
                        ...workspace,
                        selectedDatabase: event.target.value || null,
                      }))
                    }
                  >
                    <option value="">No default database</option>
                    {activeWorkspace.databases.map((database) => (
                      <option key={database} value={database}>
                        {database}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}

              <button className="ghost-button" type="button" onClick={() => addTab(activeConnection.id)}>
                Add tab
              </button>
            </div>
          ) : null}
          <div className="topbar-group">
            <button
              className="ghost-button"
              type="button"
              onClick={() => setTheme((current) => (current === "dark" ? "light" : "dark"))}
            >
              {theme === "dark" ? "Light theme" : "Dark theme"}
            </button>
            <button
              className={`ghost-button ${page === "connections" ? "button-selected" : ""}`}
              type="button"
              onClick={() => setPage("connections")}
            >
              Connections
            </button>
          </div>
          {activeConnection ? (
            <div className="topbar-group topbar-status-group">
              <span className={`status-pill status-${activeWorkspaceStatus}`}>
                {activeConnection.name}
              </span>
            </div>
          ) : null}
        </div>
      </header>

      {page === "connections" ? (
        <div className="connections-layout">
          <section className="card intro-card">
            <p className="eyebrow">Connection Hub</p>
            <h2>Connect to a MariaDB or MySQL server instance</h2>
            <p className="muted">
              Save server profiles, test them without choosing a specific database, and open a
              workspace that discovers every accessible database on that instance.
            </p>
          </section>

          <section className="card">
            <ConnectionManagerForm
              draft={draft}
              draftTestState={draftTestState}
              editing={editingId !== null}
              onChange={updateDraft}
              onSubmit={(event) => {
                event.preventDefault();
                saveConnection();
              }}
              onTestConnection={() => void testDraftConnection()}
              onSaveAndOpen={saveAndOpenWorkspace}
              onCancel={editingId ? resetDraft : undefined}
            />
          </section>

          <section className="card connection-list-card">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Saved Profiles</p>
                <h2>Server connections</h2>
              </div>
            </div>

            {connections.length === 0 ? (
              <div className="empty-state">
                <p>No saved connections yet.</p>
                <span>Create one from the form to open your first server workspace.</span>
              </div>
            ) : (
              <div className="connection-grid">
                {connections.map((connection) => {
                  const workspace = workspaces[connection.id] ?? createWorkspaceState();
                  return (
                    <article key={connection.id} className="connection-card">
                      <div className="panel-header">
                        <div>
                          <h3>{connection.name}</h3>
                          <p className="muted">{summarizeConnection(connection)}</p>
                        </div>
                        <span className={`status-pill status-${workspace.connectionStatus}`}>
                          {workspace.connectionStatus}
                        </span>
                      </div>

                      <p className="connection-message">{workspace.connectionMessage}</p>

                      <div className="connection-card-actions">
                        <button
                          className="primary-button"
                          type="button"
                          onClick={() => void openConnectionWorkspace(connection.id)}
                        >
                          Open workspace
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => editConnection(connection)}
                        >
                          Edit
                        </button>
                        <button
                          className="icon-button"
                          type="button"
                          onClick={() => deleteConnection(connection.id)}
                          aria-label={`Delete ${connection.name}`}
                        >
                          x
                        </button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </section>
        </div>
      ) : (
        <div className="app-shell">
          <aside className="left-panel">
            <section className="card brand-card">
              <p className="eyebrow">Active Workspace</p>
              <h2>{activeConnection?.name ?? "No connection selected"}</h2>
              <p className="muted">
                {activeConnection
                  ? summarizeConnection(activeConnection)
                  : "Go to the connections page to create or select a server profile."}
              </p>
            </section>

            <section className="card switcher-card">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Connections</p>
                  <h2>Switch server</h2>
                </div>
                <button className="ghost-button" type="button" onClick={() => setPage("connections")}>
                  Manage
                </button>
              </div>

              <div className="switcher-list">
                {connections.map((connection) => (
                  <button
                    key={connection.id}
                    className={`switcher-item ${connection.id === activeConnectionId ? "active" : ""}`}
                    type="button"
                    onClick={() => void openConnectionWorkspace(connection.id)}
                  >
                    <strong>{connection.name}</strong>
                    <span>{summarizeConnection(connection)}</span>
                  </button>
                ))}
              </div>
            </section>

            <section className="card schema-card">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Schema</p>
                  <h2>Instance tree</h2>
                </div>
                {activeConnection ? (
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void discoverSchema(activeConnection.id)}
                  >
                    Refresh
                  </button>
                ) : null}
              </div>

              {activeWorkspace ? (
                <>
                  <p className="form-message">{activeWorkspace.connectionMessage}</p>
                  {activeWorkspace.schema ? (
                    <SchemaTree
                      node={activeWorkspace.schema}
                      depth={0}
                      selectedDatabase={activeWorkspace.selectedDatabase}
                      onInsertReference={insertReference}
                      onSelectDatabase={(database) =>
                        activeConnectionId
                          ? updateWorkspace(activeConnectionId, (workspace) => ({
                              ...workspace,
                              selectedDatabase: database,
                            }))
                          : undefined
                      }
                    />
                  ) : (
                    <div className="empty-state">
                      <p>No schema loaded yet.</p>
                      <span>Select a connection and open its workspace to discover all databases.</span>
                    </div>
                  )}
                </>
              ) : (
                <div className="empty-state">
                  <p>No active connection.</p>
                  <span>Create or select a saved profile from the connections page.</span>
                </div>
              )}
            </section>
          </aside>

          <main className="workspace">
            {!activeConnection || !activeWorkspace ? (
              <section className="card notebook-card">
                <div className="empty-state">
                  <p>Choose a connection first.</p>
                  <span>The notebook opens once a saved server profile is selected.</span>
                </div>
              </section>
            ) : (
              <>
                <section className="card ai-card">
                  <div className="panel-header">
                    <div>
                      <p className="eyebrow">AI Assistant</p>
                      <h2>Natural language and SQL optimization</h2>
                    </div>
                    <div className="workspace-actions">
                      <label className="inline-select-field" htmlFor={`ai-model-${activeConnection.id}`}>
                        <span>Model</span>
                        <select
                          id={`ai-model-${activeConnection.id}`}
                          className="select-input"
                          value={activeWorkspace.aiSelectedModel ?? ""}
                          onChange={(event) =>
                            updateWorkspace(activeConnection.id, (workspace) => ({
                              ...workspace,
                              aiSelectedModel: event.target.value || null,
                            }))
                          }
                        >
                          <option value="">Select model</option>
                          {activeWorkspace.aiModels.map((model) => (
                            <option key={model} value={model}>
                              {model}
                            </option>
                          ))}
                        </select>
                      </label>
                      <button className="ghost-button" type="button" onClick={() => void loadAiModels(activeConnection.id)}>
                        Refresh models
                      </button>
                      <button
                        className="icon-button ai-toggle-button"
                        type="button"
                        onClick={() =>
                          updateWorkspace(activeConnection.id, (workspace) => ({
                            ...workspace,
                            aiCollapsed: !workspace.aiCollapsed,
                          }))
                        }
                        aria-label={activeWorkspace.aiCollapsed ? "Expand AI Assistant" : "Collapse AI Assistant"}
                        title={activeWorkspace.aiCollapsed ? "Expand AI Assistant" : "Collapse AI Assistant"}
                      >
                        {activeWorkspace.aiCollapsed ? "+" : "-"}
                      </button>
                    </div>
                  </div>

                  {!activeWorkspace.aiCollapsed ? (
                    <>
                      <textarea
                        className="ai-prompt"
                        value={activeWorkspace.aiPrompt}
                        onChange={(event) =>
                          updateWorkspace(activeConnection.id, (workspace) => ({
                            ...workspace,
                            aiPrompt: event.target.value,
                          }))
                        }
                        placeholder="Describe the query you want, or add optimization goals for the current SQL..."
                      />

                      <div className="form-actions">
                        <button
                          className="primary-button"
                          type="button"
                          onClick={() => void runAiAction(activeConnection.id, "generate")}
                          disabled={!activeWorkspace.aiSelectedModel || activeWorkspace.aiStatus === "loading"}
                        >
                          Generate SQL
                        </button>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => void runAiAction(activeConnection.id, "optimize")}
                          disabled={!activeWorkspace.aiSelectedModel || activeWorkspace.aiStatus === "loading" || !activeNotebookCell?.sql.trim()}
                        >
                          Optimize SQL
                        </button>
                      </div>

                      <p className={`test-message test-${activeWorkspace.aiStatus}`}>{activeWorkspace.aiMessage}</p>
                      {activeWorkspace.aiNotes ? <p className="ai-notes">{activeWorkspace.aiNotes}</p> : null}
                    </>
                  ) : null}
                </section>

                <section className="card notebook-shell">
                  <section className="tab-strip">
                    <div className="tab-list">
                      {activeWorkspace.tabs.map((tab) => (
                        <div
                          key={tab.id}
                          className={`notebook-tab ${tab.id === activeWorkspace.activeTabId ? "active" : ""}`}
                        >
                          <button
                            className="notebook-tab-button"
                            type="button"
                            onClick={() =>
                              updateWorkspace(activeConnection.id, (workspace) => ({
                                ...workspace,
                                activeTabId: tab.id,
                              }))
                            }
                          >
                            {tab.title}
                          </button>
                          <button
                            className="notebook-tab-close"
                            type="button"
                            onClick={() => removeTab(activeConnection.id, tab.id)}
                            aria-label={`Close ${tab.title}`}
                          >
                            x
                          </button>
                        </div>
                      ))}
                    </div>
                    {activeTab ? (
                      <button className="ghost-button" type="button" onClick={() => addTab(activeConnection.id)}>
                        Add notebook
                      </button>
                    ) : null}
                  </section>

                  {activeTab && activeNotebookCell ? (
                    <div className="cells">
                      <NotebookCard
                        cell={activeNotebookCell}
                        active
                        canRun={canRunQueries}
                        autocompleteContext={activeSqlAutocompleteContext}
                        onActivate={() => undefined}
                        onChange={(sql) => updateCell(activeConnection.id, activeTab.id, activeNotebookCell.id, { sql })}
                        onRun={() => void runCell(activeConnection.id, activeTab.id, activeNotebookCell.id)}
                        onDelete={() => removeTab(activeConnection.id, activeTab.id)}
                      />
                    </div>
                  ) : null}
                </section>
              </>
            )}
          </main>
        </div>
      )}
    </div>
  );
}

type ConnectionManagerFormProps = {
  draft: ConnectionDraft;
  draftTestState: DraftTestState;
  editing: boolean;
  onChange: (draft: ConnectionDraft) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onTestConnection: () => void;
  onSaveAndOpen: () => void;
  onCancel?: () => void;
};

function ConnectionManagerForm({
  draft,
  draftTestState,
  editing,
  onChange,
  onSubmit,
  onTestConnection,
  onSaveAndOpen,
  onCancel,
}: ConnectionManagerFormProps) {
  return (
    <form className="connection-form" onSubmit={onSubmit}>
      <div className="panel-header">
        <div>
          <p className="eyebrow">Editor</p>
          <h2>{editing ? "Update server connection" : "Create server connection"}</h2>
        </div>
      </div>

      <label>
        Connection name
        <input
          value={draft.name}
          onChange={(event) => onChange({ ...draft, name: event.target.value })}
          placeholder="Reporting cluster"
        />
      </label>

      <label>
        Host
        <input
          value={draft.host}
          onChange={(event) => onChange({ ...draft, host: event.target.value })}
          placeholder="127.0.0.1"
        />
      </label>

      <div className="form-row">
        <label>
          Port
          <input
            type="number"
            value={draft.port}
            onChange={(event) => onChange({ ...draft, port: Number(event.target.value) })}
          />
        </label>
        <label>
          User
          <input
            value={draft.user}
            onChange={(event) => onChange({ ...draft, user: event.target.value })}
          />
        </label>
      </div>

      <label>
        Password
        <input
          type="password"
          value={draft.password}
          onChange={(event) => onChange({ ...draft, password: event.target.value })}
        />
      </label>

      <div className="form-actions">
        <button className="primary-button" type="submit">
          {editing ? "Save changes" : "Save connection"}
        </button>
        <button
          className="ghost-button"
          type="button"
          onClick={onTestConnection}
          disabled={draftTestState.status === "loading"}
        >
          {draftTestState.status === "loading" ? "Testing..." : "Test connection"}
        </button>
        <button className="ghost-button" type="button" onClick={onSaveAndOpen}>
          Save and open workspace
        </button>
        {onCancel ? (
          <button className="ghost-button" type="button" onClick={onCancel}>
            Cancel edit
          </button>
        ) : null}
      </div>

      <p className="muted">
        Database selection happens in the workspace after the server connection succeeds.
      </p>

      {draftTestState.message ? (
        <p className={`test-message test-${draftTestState.status}`}>{draftTestState.message}</p>
      ) : null}
    </form>
  );
}

type SchemaTreeProps = {
  node: SchemaNode;
  depth: number;
  selectedDatabase: string | null;
  onInsertReference: (node: SchemaNode) => void;
  onSelectDatabase: (database: string) => void;
};

function SchemaTree({ node, depth, selectedDatabase, onInsertReference, onSelectDatabase }: SchemaTreeProps) {
  const detailsId = useId();
  const isLeaf = !node.children || node.children.length === 0;
  const defaultOpen = depth < 2;
  const isSelectedDatabase = node.type === "database" && node.label === selectedDatabase;

  if (isLeaf) {
    return (
      <button className={`tree-node type-${node.type}`} type="button" onClick={() => onInsertReference(node)}>
        <span>{node.label}</span>
        {node.description ? <small>{node.description}</small> : null}
      </button>
    );
  }

  return (
    <details className={`tree-group type-${node.type} ${isSelectedDatabase ? "database-selected" : ""}`} open={defaultOpen}>
      <summary
        onClick={() => {
          if (node.type === "database") {
            onSelectDatabase(node.label);
          }
        }}
      >
        <span>{node.label}</span>
        {node.description ? <small id={detailsId}>{node.description}</small> : null}
      </summary>
      <div className="tree-children">
        {node.children?.map((child) => (
          <SchemaTree
            key={child.id}
            node={child}
            depth={depth + 1}
            selectedDatabase={selectedDatabase}
            onInsertReference={onInsertReference}
            onSelectDatabase={onSelectDatabase}
          />
        ))}
      </div>
    </details>
  );
}

type NotebookCardProps = {
  cell: NotebookCell;
  active: boolean;
  canRun: boolean;
  autocompleteContext: SqlAutocompleteContext;
  onActivate: () => void;
  onChange: (sql: string) => void;
  onRun: () => void;
  onDelete: () => void;
};

function NotebookCard({
  cell,
  active,
  canRun,
  autocompleteContext,
  onActivate,
  onChange,
  onRun,
  onDelete,
}: NotebookCardProps) {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const [caretPosition, setCaretPosition] = useState(cell.sql.length);
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(0);
  const [forceAutocomplete, setForceAutocomplete] = useState(false);
  const [isEditorFocused, setIsEditorFocused] = useState(false);
  const autocompleteQuery = useMemo(
    () => getSqlAutocompleteQuery(cell.sql, caretPosition),
    [caretPosition, cell.sql],
  );
  const autocompleteSuggestions = useMemo(
    () => buildSqlAutocompleteSuggestions(autocompleteContext, autocompleteQuery, forceAutocomplete),
    [autocompleteContext, autocompleteQuery, forceAutocomplete],
  );

  useEffect(() => {
    setActiveSuggestionIndex(0);
  }, [autocompleteQuery.prefix, autocompleteSuggestions.length]);

  useEffect(() => {
    setCaretPosition((current) => Math.min(current, cell.sql.length));
  }, [cell.sql.length]);

  function syncCaretPosition(target: HTMLTextAreaElement) {
    setCaretPosition(target.selectionStart ?? target.value.length);
  }

  function applySuggestion(suggestion: SqlSuggestion) {
    const nextSql =
      `${cell.sql.slice(0, autocompleteQuery.start)}${suggestion.insertText}${cell.sql.slice(autocompleteQuery.end)}`;
    const nextCaretPosition = autocompleteQuery.start + suggestion.insertText.length;
    onChange(nextSql);
    setCaretPosition(nextCaretPosition);
    setForceAutocomplete(false);

    requestAnimationFrame(() => {
      const textarea = textareaRef.current;
      if (!textarea) {
        return;
      }

      textarea.focus();
      textarea.setSelectionRange(nextCaretPosition, nextCaretPosition);
    });
  }

  return (
    <article className={`card notebook-card ${active ? "active" : ""}`} onClick={onActivate}>
      <div className="panel-header">
        <div className="cell-actions">
          <span className={`status-pill status-${cell.status}`}>{cell.status}</span>
          <button
            className="ghost-button"
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              onRun();
            }}
            disabled={!canRun || cell.status === "running"}
          >
            Run
          </button>
          <button
            className="icon-button"
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              onDelete();
            }}
            aria-label={`Delete ${cell.title}`}
          >
            x
          </button>
        </div>
      </div>

      <div className="editor-shell">
        <div className="editor-input-wrap">
          <textarea
            ref={textareaRef}
            className="sql-editor"
            spellCheck={false}
            value={cell.sql}
            onFocus={() => setIsEditorFocused(true)}
            onBlur={() => {
              window.setTimeout(() => setIsEditorFocused(false), 120);
              setForceAutocomplete(false);
            }}
            onClick={(event) => syncCaretPosition(event.currentTarget)}
            onKeyUp={(event) => syncCaretPosition(event.currentTarget)}
            onChange={(event) => {
              onChange(event.target.value);
              syncCaretPosition(event.currentTarget);
            }}
            onKeyDown={(event) => {
              if ((event.ctrlKey || event.metaKey) && event.key === " ") {
                event.preventDefault();
                setForceAutocomplete(true);
                syncCaretPosition(event.currentTarget);
                return;
              }

              if (autocompleteSuggestions.length === 0) {
                return;
              }

              if (event.key === "ArrowDown") {
                event.preventDefault();
                setActiveSuggestionIndex((current) => (current + 1) % autocompleteSuggestions.length);
                return;
              }

              if (event.key === "ArrowUp") {
                event.preventDefault();
                setActiveSuggestionIndex((current) =>
                  current === 0 ? autocompleteSuggestions.length - 1 : current - 1,
                );
                return;
              }

              if (event.key === "Escape") {
                setForceAutocomplete(false);
                return;
              }

              if ((event.key === "Tab" || event.key === "Enter") && !event.shiftKey) {
                event.preventDefault();
                applySuggestion(autocompleteSuggestions[activeSuggestionIndex] ?? autocompleteSuggestions[0]);
              }
            }}
            placeholder="Write SQL here..."
          />
        </div>

        {isEditorFocused && autocompleteSuggestions.length > 0 ? (
          <div className="autocomplete-panel autocomplete-dropdown">
            {autocompleteSuggestions.map((suggestion, index) => (
              <button
                key={`${suggestion.kind}-${suggestion.insertText}-${suggestion.detail}`}
                className={`autocomplete-item ${index === activeSuggestionIndex ? "active" : ""}`}
                type="button"
                onMouseDown={(event) => {
                  event.preventDefault();
                  applySuggestion(suggestion);
                }}
              >
                <span className={`autocomplete-kind kind-${suggestion.kind}`}>{suggestion.kind}</span>
                <span className="autocomplete-label">{suggestion.label}</span>
                <span className="autocomplete-detail">{suggestion.detail}</span>
              </button>
            ))}
          </div>
        ) : null}

        <div className="editor-assist">
          <span>Autocomplete uses SQL keywords plus the loaded schema for databases, tables, views, and columns.</span>
          <span>Use `Ctrl+Space`, arrows, and `Tab`.</span>
        </div>
      </div>

      {cell.error ? <div className="result-error">{cell.error}</div> : null}

      {cell.result ? <QueryResult result={cell.result} /> : null}
    </article>
  );
}

function QueryResult({ result }: { result: CellResult }) {
  return (
    <section className="result-panel">
      <div className="result-meta">
        <strong>Last run:</strong> {new Date(result.executedAt).toLocaleString()} | <strong>Execution time:</strong>{" "}
        {formatDuration(result.durationMs)}
      </div>

      {result.statements.map((statement, index) => (
        <div key={index} className="statement-block">
          <div className="statement-meta">Statement {index + 1}</div>
          {statement.kind === "result-set" ? (
            <>
              <div className="result-meta">{statement.rowCount} rows</div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      {statement.columns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {statement.rows.map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {statement.columns.map((column) => (
                          <td key={`${rowIndex}-${column}`}>{formatValue(row[column])}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : null}

          {statement.kind === "command" ? (
            <div className="command-result">
              <span>{statement.affectedRows} affected rows</span>
              <span>Insert ID: {statement.insertId ?? "n/a"}</span>
              <span>Warnings: {statement.warningStatus}</span>
            </div>
          ) : null}

          {statement.kind === "unknown" ? (
            <pre className="unknown-result">{JSON.stringify(statement.value, null, 2)}</pre>
          ) : null}
        </div>
      ))}
    </section>
  );
}

function formatDuration(durationMs: number) {
  if (durationMs < 1000) {
    return `${durationMs.toFixed(durationMs < 10 ? 2 : durationMs < 100 ? 1 : 0)} ms`;
  }

  return `${(durationMs / 1000).toFixed(durationMs < 10_000 ? 2 : 1)} s`;
}

function formatValue(value: unknown) {
  if (value === null) {
    return "NULL";
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}
