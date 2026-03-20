import type { WorkspaceState } from "../types";

function formatOllamaHost(baseUrl: string | null) {
  if (!baseUrl) {
    return null;
  }

  try {
    return new URL(baseUrl).host || baseUrl;
  } catch {
    return baseUrl.replace(/^https?:\/\//, "");
  }
}

function summarizePrompt(prompt: string) {
  const normalized = prompt.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "Untitled request";
  }

  return normalized.length > 88 ? `${normalized.slice(0, 85)}...` : normalized;
}

type AiAssistantPanelProps = {
  connectionId: string;
  workspace: WorkspaceState;
  onToggleCollapse: () => void;
  onModelChange: (model: string | null) => void;
  onPromptChange: (prompt: string) => void;
  onClearContext: () => void;
  onRefreshModels: () => void;
  onGenerate: () => void;
};

export function AiAssistantPanel({
  connectionId,
  workspace,
  onToggleCollapse,
  onModelChange,
  onPromptChange,
  onClearContext,
  onRefreshModels,
  onGenerate,
}: AiAssistantPanelProps) {
  const ollamaHost = formatOllamaHost(workspace.aiBaseUrl);
  const isRefreshingModels = workspace.aiStatus === "loading" && workspace.aiMessage.startsWith("Loading Ollama models");
  const recentHistory = workspace.aiHistory.slice(-3).reverse();
  const canClearContext =
    workspace.aiStatus !== "loading" &&
    (workspace.aiPrompt.trim().length > 0 || workspace.aiNotes.trim().length > 0 || workspace.aiHistory.length > 0);

  return (
    <section className="card ai-card">
      <div className="panel-header">
        <div>
          <p className="eyebrow">AI Assistant</p>
          <h2>Natural language SQL iteration</h2>
        </div>
        <div className="workspace-actions">
          <label className="inline-select-field" htmlFor={`ai-model-${connectionId}`}>
            <span>Model</span>
            <select
              id={`ai-model-${connectionId}`}
              className="select-input"
              value={workspace.aiSelectedModel ?? ""}
              onChange={(event) => onModelChange(event.target.value || null)}
            >
              <option value="">Select model</option>
              {workspace.aiModels.map((model) => (
                <option key={model} value={model}>
                  {model}
                </option>
              ))}
            </select>
          </label>
          <button className="ghost-button" type="button" onClick={onRefreshModels} disabled={isRefreshingModels}>
            {isRefreshingModels ? "Refreshing..." : "Refresh models"}
          </button>
          <button
            className="icon-button ai-toggle-button"
            type="button"
            onClick={onToggleCollapse}
            aria-label={workspace.aiCollapsed ? "Expand AI Assistant" : "Collapse AI Assistant"}
            title={workspace.aiCollapsed ? "Expand AI Assistant" : "Collapse AI Assistant"}
          >
            {workspace.aiCollapsed ? "+" : "-"}
          </button>
        </div>
      </div>

      {!workspace.aiCollapsed ? (
        <>
          <textarea
            className="ai-prompt"
            value={workspace.aiPrompt}
            onChange={(event) => onPromptChange(event.target.value)}
            placeholder="Describe the query you want, or refine the current SQL with follow-up instructions..."
          />
          <p className="ai-helper-copy">Uses the current notebook SQL and your recent AI requests as context.</p>

          <div className="form-actions">
            <button
              className="primary-button"
              type="button"
              onClick={onGenerate}
              disabled={!workspace.aiSelectedModel || workspace.aiStatus === "loading"}
            >
              Apply Request
            </button>
            <button className="ghost-button" type="button" onClick={onClearContext} disabled={!canClearContext}>
              Clear Context
            </button>
          </div>

          <div className={`test-message test-${workspace.aiStatus}`}>
            <div className="ai-status-bar">
              <span className="ai-status-copy">{workspace.aiMessage}</span>
              {ollamaHost ? (
                <span className="status-pill ai-host-pill" title={workspace.aiBaseUrl ?? undefined}>
                  Host {ollamaHost}
                </span>
              ) : null}
            </div>
          </div>
          {workspace.aiNotes ? <p className="ai-notes">{workspace.aiNotes}</p> : null}
          {recentHistory.length > 0 ? (
            <div className="ai-history">
              <div className="ai-history-header">
                <span className="eyebrow">Recent Context</span>
              </div>
              <div className="ai-history-list">
                {recentHistory.map((turn, index) => (
                  <article key={`${turn.prompt}-${turn.sql}-${index}`} className="ai-history-item">
                    <p className="ai-history-title">{summarizePrompt(turn.prompt)}</p>
                    {turn.notes ? <p className="ai-history-notes">{turn.notes}</p> : null}
                  </article>
                ))}
              </div>
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
