import { useEffect, useMemo, useRef, useState } from "react";
import { buildSqlAutocompleteSuggestions, getSqlAutocompleteQuery } from "../lib/sqlAutocomplete";
import type { NotebookCell, SqlAutocompleteContext, SqlSuggestion } from "../types";
import { QueryResult } from "./QueryResult";

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

export function NotebookCard({
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

  const autocompleteQuery = useMemo(() => getSqlAutocompleteQuery(cell.sql, caretPosition), [caretPosition, cell.sql]);
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
    <article
      className={`card notebook-card ${active ? "active" : ""} ${cell.status === "running" ? "running" : ""}`}
      onClick={onActivate}
    >
      <div className="panel-header">
        <div className="cell-actions">
          <span className={`status-pill status-${cell.status}`}>{cell.status}</span>
          {cell.status === "running" ? <span className="notebook-running-indicator">Running query...</span> : null}
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
      </div>

      {cell.error ? <div className="result-error">{cell.error}</div> : null}

      {cell.result ? <QueryResult result={cell.result} /> : null}
    </article>
  );
}
