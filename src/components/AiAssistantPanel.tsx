import type { WorkspaceState } from "../types";

type AiAssistantPanelProps = {
  connectionId: string;
  workspace: WorkspaceState;
  hasNotebookSql: boolean;
  onToggleCollapse: () => void;
  onModelChange: (model: string | null) => void;
  onPromptChange: (prompt: string) => void;
  onRefreshModels: () => void;
  onGenerate: () => void;
  onOptimize: () => void;
};

export function AiAssistantPanel({
  connectionId,
  workspace,
  hasNotebookSql,
  onToggleCollapse,
  onModelChange,
  onPromptChange,
  onRefreshModels,
  onGenerate,
  onOptimize,
}: AiAssistantPanelProps) {
  return (
    <section className="card ai-card">
      <div className="panel-header">
        <div>
          <p className="eyebrow">AI Assistant</p>
          <h2>Natural language and SQL optimization</h2>
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
          <button className="ghost-button" type="button" onClick={onRefreshModels}>
            Refresh models
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
            placeholder="Describe the query you want, or add optimization goals for the current SQL..."
          />

          <div className="form-actions">
            <button
              className="primary-button"
              type="button"
              onClick={onGenerate}
              disabled={!workspace.aiSelectedModel || workspace.aiStatus === "loading"}
            >
              Generate SQL
            </button>
            <button
              className="ghost-button"
              type="button"
              onClick={onOptimize}
              disabled={!workspace.aiSelectedModel || workspace.aiStatus === "loading" || !hasNotebookSql}
            >
              Optimize SQL
            </button>
          </div>

          <p className={`test-message test-${workspace.aiStatus}`}>{workspace.aiMessage}</p>
          {workspace.aiNotes ? <p className="ai-notes">{workspace.aiNotes}</p> : null}
        </>
      ) : null}
    </section>
  );
}
