import { useEffect, useMemo, useRef, useState } from "react";
import { AiAssistantPanel } from "./components/AiAssistantPanel";
import { ConnectionManagerForm } from "./components/ConnectionManagerForm";
import { NotebookCard } from "./components/NotebookCard";
import { SchemaTree } from "./components/SchemaTree";
import {
  formatApiError,
  getContextLabel,
  getDatabaseTypeLabel,
  normalizeSavedConnection,
  normalizeSchemaPayload,
  sanitizeDraft,
  summarizeConnection,
  summarizeSchemaForAi,
  toConnectionPayload,
  toDraft,
} from "./lib/schema";
import {
  ACTIVE_CONNECTION_STORAGE_KEY,
  STORAGE_KEY,
  THEME_STORAGE_KEY,
  loadActiveConnectionId,
  loadConnections,
  loadPersistedWorkspaces,
  loadTheme,
  persistWorkspacesToCookies,
} from "./lib/storage";
import { buildSqlAutocompleteContext } from "./lib/sqlAutocomplete";
import { createCell, createTab, createWorkspaceState, getActiveTab } from "./lib/workspace";
import type {
  ConnectionDraft,
  DraftTestState,
  NotebookCell,
  Page,
  SavedConnection,
  SchemaNode,
  Theme,
  WorkspaceState,
} from "./types";

const initialConnection = {
  type: "mariadb" as const,
  host: "127.0.0.1",
  port: 3306,
  user: "root",
  password: "",
  database: "",
};

const initialDraft: ConnectionDraft = {
  name: "Local MariaDB",
  ...initialConnection,
};

function formatOllamaHostLabel(baseUrl: string | null) {
  if (!baseUrl) {
    return null;
  }

  try {
    return new URL(baseUrl).host || baseUrl;
  } catch {
    return baseUrl.replace(/^https?:\/\//, "");
  }
}

async function readApiPayload(response: Response) {
  const contentType = response.headers.get("content-type") ?? "";

  if (contentType.includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }

  try {
    const text = await response.text();
    return text.trim().length > 0 ? { error: text.trim() } : null;
  } catch {
    return null;
  }
}

const MAX_AI_HISTORY_ITEMS = 6;

function getAiBaseUrl(payload: unknown) {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }

  const baseUrl = (payload as { baseUrl?: unknown }).baseUrl;
  return typeof baseUrl === "string" && baseUrl.trim().length > 0 ? baseUrl.trim() : null;
}

function initializeConnections() {
  return loadConnections()
    .map((item) => normalizeSavedConnection(item))
    .filter((item): item is SavedConnection => item !== null);
}

function initializePersistedWorkspaces() {
  const connections = initializeConnections();
  return loadPersistedWorkspaces(connections);
}

export function App() {
  const [theme, setTheme] = useState<Theme>(() => loadTheme());
  const [page, setPage] = useState<Page>(() => (initializeConnections().length > 0 ? "workspace" : "connections"));
  const [connections, setConnections] = useState<SavedConnection[]>(() => initializeConnections());
  const [draft, setDraft] = useState<ConnectionDraft>(initialDraft);
  const [draftTestState, setDraftTestState] = useState<DraftTestState>({
    status: "idle",
    message: "",
  });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [activeConnectionId, setActiveConnectionId] = useState<string | null>(() => {
    const existing = initializeConnections();
    const storedActiveId = loadActiveConnectionId();
    return existing.some((connection) => connection.id === storedActiveId)
      ? storedActiveId
      : (existing[0]?.id ?? null);
  });
  const [workspaces, setWorkspaces] = useState<Record<string, WorkspaceState>>(() => initializePersistedWorkspaces());
  const hasReloadedSchemaRef = useRef(false);

  const activeConnection = useMemo(
    () => connections.find((connection) => connection.id === activeConnectionId) ?? null,
    [activeConnectionId, connections],
  );
  const activeWorkspace = activeConnectionId ? (workspaces[activeConnectionId] ?? createWorkspaceState()) : null;
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
    const wasEditing = editingId !== null;
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
    if (wasEditing) {
      setEditingId(connectionId);
      setDraft(toDraft(nextConnection));
    } else {
      resetDraft();
    }
    updateWorkspace(connectionId, (workspace) => ({
      ...workspace,
      connectionStatus: "idle",
      connectionMessage: "Connection saved. Test it or open the workspace to discover its structure.",
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
      connectionMessage: `Connecting to ${connection.name} and discovering available structure...`,
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
          contextLabel: schemaPayload.contextLabel,
          selectedDatabase,
          connectionStatus: "ready",
          connectionMessage:
            schemaPayload.databases.length > 0
              ? `Discovered ${schemaPayload.databases.length} ${schemaPayload.contextLabel.toLowerCase()}(s) on ${connection.host}.`
              : `Connected, but no ${schemaPayload.contextLabel.toLowerCase()}s were discovered.`,
        };
      });
      await loadAiModels(connectionId);
    } catch (error) {
      updateWorkspace(connectionId, (workspace) => ({
        ...workspace,
        schema: null,
        databases: [],
        contextLabel: getContextLabel(connection.type),
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
      aiMessage: workspace.aiBaseUrl
        ? `Loading Ollama models from ${formatOllamaHostLabel(workspace.aiBaseUrl)}...`
        : "Loading Ollama models...",
    }));

    try {
      const response = await fetch("/api/ai/models");
      const payload = await readApiPayload(response);
      const aiBaseUrl = getAiBaseUrl(payload);
      if (!response.ok) {
        updateWorkspace(connectionId, (workspace) => ({
          ...workspace,
          aiBaseUrl: aiBaseUrl ?? workspace.aiBaseUrl,
          aiModels: [],
          aiSelectedModel: null,
          aiStatus: "error",
          aiMessage: formatApiError(payload, "Failed to load Ollama models"),
        }));
        return;
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
        aiBaseUrl,
        aiStatus: "ready",
        aiMessage:
          models.length > 0
            ? `Ollama connected. ${models.length} model(s) available.`
            : "Ollama reachable, but no models are installed yet.",
      }));
    } catch (error) {
      updateWorkspace(connectionId, (workspace) => ({
        ...workspace,
        aiBaseUrl: workspace.aiBaseUrl,
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
      const contextLabel =
        typeof payload.contextLabel === "string" && payload.contextLabel.trim() ? payload.contextLabel : "Database";
      const serverVersion =
        typeof payload.serverVersion === "string" && payload.serverVersion.length > 0
          ? payload.serverVersion
          : "unknown";
      const dialectLabel =
        typeof payload.dialectLabel === "string" && payload.dialectLabel.length > 0
          ? payload.dialectLabel
          : getDatabaseTypeLabel(draft.type);

      setDraftTestState({
        status: "success",
        message: `Connected to ${dialectLabel} at ${draft.host}:${draft.port}. Version: ${serverVersion}. Accessible ${contextLabel.toLowerCase()}s: ${databases.length}.`,
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

  async function runAiAction(connectionId: string) {
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
      aiMessage: "Generating SQL with Ollama...",
      aiNotes: "",
    }));

    try {
      const response = await fetch("/api/ai/generate-sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: workspace.aiSelectedModel,
          selectedDatabase: workspace.selectedDatabase,
          schemaSummary: summarizeSchemaForAi(workspace.schema, workspace.selectedDatabase),
          prompt: workspace.aiPrompt,
          sql: cell.sql.trim() || undefined,
          history: workspace.aiHistory.slice(-MAX_AI_HISTORY_ITEMS),
          databaseType: connection.type,
        }),
      });

      const payload = await response.json();
      if (!response.ok) {
        throw new Error(formatApiError(payload, "Failed to generate SQL"));
      }

      const nextSql = typeof payload.sql === "string" ? payload.sql : cell.sql;
      const nextNotes = typeof payload.notes === "string" ? payload.notes : "";

      updateCell(connectionId, tab.id, cell.id, {
        sql: nextSql,
      });

      updateWorkspace(connectionId, (current) => ({
        ...current,
        aiStatus: "ready",
        aiMessage: cell.sql.trim().length > 0 ? "SQL updated by Ollama." : "SQL generated by Ollama.",
        aiNotes: nextNotes,
        aiHistory: [
          ...current.aiHistory,
          {
            prompt: workspace.aiPrompt,
            sql: nextSql,
            notes: nextNotes,
          },
        ].slice(-MAX_AI_HISTORY_ITEMS),
      }));
    } catch (error) {
      updateWorkspace(connectionId, (current) => ({
        ...current,
        aiStatus: "error",
        aiMessage: error instanceof Error ? error.message : "Failed to generate SQL",
        aiNotes: "",
      }));
    }
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
      <header className={`topbar topbar-shell ${page === "connections" ? "topbar-connections" : "topbar-workspace"} card`}>
        <div className="topbar-brand">
          <div className="topbar-title-row">
            <img className="brand-logo" src="/sql-ninja-logo.svg" alt="" />
            <p className="topbar-title">SQL Ninja</p>
          </div>
        </div>

        <div className="topbar-actions">
          {page === "workspace" && activeConnection && activeWorkspace ? (
            <div className="topbar-group">
              {activeWorkspace.databases.length > 0 ? (
                <label className="topbar-inline-field" htmlFor="topbar-database-context">
                  <span className="topbar-field-label">{activeWorkspace.contextLabel}</span>
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
                    <option value="">{`No default ${activeWorkspace.contextLabel.toLowerCase()}`}</option>
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
              <span className={`status-pill status-${activeWorkspaceStatus}`}>{activeConnection.name}</span>
            </div>
          ) : null}
        </div>
      </header>

      {page === "connections" ? (
        <div className="connections-layout">
          <section className="card connection-editor-card">
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
                        <button className="ghost-button" type="button" onClick={() => editConnection(connection)}>
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
                  <h2>Catalog tree</h2>
                </div>
                {activeConnection ? (
                  <button className="ghost-button" type="button" onClick={() => void discoverSchema(activeConnection.id)}>
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
                      <span>Select a connection and open its workspace to discover available databases or schemas.</span>
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
                <AiAssistantPanel
                  connectionId={activeConnection.id}
                  workspace={activeWorkspace}
                  onToggleCollapse={() =>
                    updateWorkspace(activeConnection.id, (workspace) => ({
                      ...workspace,
                      aiCollapsed: !workspace.aiCollapsed,
                    }))
                  }
                  onModelChange={(model) =>
                    updateWorkspace(activeConnection.id, (workspace) => ({
                      ...workspace,
                      aiSelectedModel: model,
                    }))
                  }
                  onPromptChange={(prompt) =>
                    updateWorkspace(activeConnection.id, (workspace) => ({
                      ...workspace,
                      aiPrompt: prompt,
                    }))
                  }
                  onRefreshModels={() => void loadAiModels(activeConnection.id)}
                  onGenerate={() => void runAiAction(activeConnection.id)}
                />

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
