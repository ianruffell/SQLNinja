import { createWorkspaceState } from "./workspace";
import type {
  PersistedNotebookCell,
  PersistedNotebookTab,
  PersistedWorkspaceState,
  SavedConnection,
  Theme,
  WorkspaceState,
} from "../types";

const STORAGE_KEY = "sqlninja.saved-connections";
const ACTIVE_CONNECTION_STORAGE_KEY = "sqlninja.active-connection-id";
const THEME_STORAGE_KEY = "sqlninja.theme";
const NOTEBOOK_COOKIE_KEY = "sqlninja.notebooks";
const NOTEBOOK_COOKIE_MANIFEST_KEY = "sqlninja.notebooks.chunks";
const NOTEBOOK_COOKIE_MAX_AGE = 60 * 60 * 24 * 365;
const NOTEBOOK_COOKIE_CHUNK_SIZE = 3200;

export { STORAGE_KEY, ACTIVE_CONNECTION_STORAGE_KEY, THEME_STORAGE_KEY };

export function loadConnections() {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function loadActiveConnectionId() {
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

export function loadTheme(): Theme {
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

export function loadPersistedWorkspaces(connections: SavedConnection[]) {
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

export function persistWorkspacesToCookies(workspaces: Record<string, WorkspaceState>, connections: SavedConnection[]) {
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
