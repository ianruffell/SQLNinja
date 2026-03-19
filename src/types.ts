export type DatabaseType = "mysql" | "mariadb" | "postgres" | "oracle" | "sqlserver" | "ignite";

export type ConnectionConfig = {
  type: DatabaseType;
  host: string;
  port: number;
  user: string;
  password: string;
  database?: string;
};

export type SavedConnection = ConnectionConfig & {
  id: string;
  name: string;
};

export type SchemaNode = {
  id: string;
  label: string;
  type: "database" | "group" | "table" | "view" | "column" | "index";
  description?: string;
  reference?: string;
  children?: SchemaNode[];
};

export type SchemaPayload = {
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

export type CellResult = {
  statements: QueryStatementResult[];
  executedAt: string;
  durationMs: number;
};

export type NotebookCell = {
  id: string;
  title: string;
  sql: string;
  status: "idle" | "running" | "success" | "error";
  result?: CellResult;
  error?: string;
};

export type NotebookTab = {
  id: string;
  title: string;
  cells: NotebookCell[];
  activeCellId: string | null;
};

export type ConnectionStatus = "idle" | "loading" | "ready" | "error";

export type AiAssistantTurn = {
  prompt: string;
  sql: string;
  notes: string;
};

export type WorkspaceState = {
  schema: SchemaNode | null;
  databases: string[];
  contextLabel: string;
  selectedDatabase: string | null;
  tabs: NotebookTab[];
  activeTabId: string | null;
  connectionStatus: ConnectionStatus;
  connectionMessage: string;
  aiCollapsed: boolean;
  aiModels: string[];
  aiSelectedModel: string | null;
  aiBaseUrl: string | null;
  aiPrompt: string;
  aiStatus: ConnectionStatus;
  aiMessage: string;
  aiNotes: string;
  aiHistory: AiAssistantTurn[];
};

export type ConnectionDraft = {
  name: string;
  type: DatabaseType;
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
};

export type DraftTestState = {
  status: "idle" | "loading" | "success" | "error";
  message: string;
};

export type Page = "connections" | "workspace";
export type Theme = "dark" | "light";

export type PersistedNotebookCell = Pick<NotebookCell, "id" | "title" | "sql">;

export type PersistedNotebookTab = {
  id: string;
  title: string;
  cells: PersistedNotebookCell[];
  activeCellId: string | null;
};

export type PersistedWorkspaceState = {
  tabs: PersistedNotebookTab[];
  activeTabId: string | null;
  selectedDatabase: string | null;
};

export type SqlSuggestionKind = "keyword" | "database" | "table" | "view" | "column";

export type SqlAutocompleteContext = {
  databases: string[];
  relations: Array<{
    database: string;
    name: string;
    type: "table" | "view";
    columns: string[];
  }>;
};

export type SqlAutocompleteQuery = {
  start: number;
  end: number;
  token: string;
  prefix: string;
  qualifier: string | null;
  previousKeyword: string | null;
};

export type SqlSuggestion = {
  kind: SqlSuggestionKind;
  label: string;
  insertText: string;
  detail: string;
  searchText: string;
  priority: number;
};
