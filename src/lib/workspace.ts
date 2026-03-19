import type {
  NotebookCell,
  NotebookTab,
  PersistedNotebookCell,
  PersistedNotebookTab,
  PersistedWorkspaceState,
  WorkspaceState,
} from "../types";

export function createCell(index: number, snapshot?: PersistedNotebookCell): NotebookCell {
  return {
    id: snapshot?.id ?? crypto.randomUUID(),
    title: snapshot?.title?.trim() || `Notebook ${index}`,
    sql: snapshot?.sql ?? "",
    status: "idle",
  };
}

export function createTab(index: number, snapshot?: PersistedNotebookTab): NotebookTab {
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

export function createWorkspaceState(snapshot?: PersistedWorkspaceState): WorkspaceState {
  const tabs = Array.isArray(snapshot?.tabs)
    ? snapshot.tabs
        .map((tab, tabIndex) => createTab(tabIndex + 1, tab))
        .filter((tab) => typeof tab.id === "string" && tab.id.length > 0)
    : [];
  const firstTab = tabs[0] ?? createTab(1);

  return {
    schema: null,
    databases: [],
    contextLabel: "Database",
    selectedDatabase:
      typeof snapshot?.selectedDatabase === "string" && snapshot.selectedDatabase ? snapshot.selectedDatabase : null,
    tabs: tabs.length > 0 ? tabs : [firstTab],
    activeTabId:
      snapshot?.activeTabId && tabs.some((tab) => tab.id === snapshot.activeTabId)
        ? snapshot.activeTabId
        : firstTab.id,
    connectionStatus: "idle",
    connectionMessage: "Open a workspace to discover available databases or schemas for this connection.",
    aiCollapsed: false,
    aiModels: [],
    aiSelectedModel: null,
    aiBaseUrl: null,
    aiPrompt: "",
    aiStatus: "idle",
    aiMessage: "Connect to Ollama to generate or refine SQL with AI.",
    aiNotes: "",
    aiHistory: [],
  };
}

export function getActiveTab(workspace: WorkspaceState) {
  return workspace.tabs.find((tab) => tab.id === workspace.activeTabId) ?? workspace.tabs[0] ?? null;
}
