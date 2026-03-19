import type { FormEvent } from "react";
import {
  DATABASE_TYPE_OPTIONS,
  getConnectionTargetLabel,
  getConnectionTargetPlaceholder,
  getConnectionTypeDescription,
  getDefaultPort,
} from "../lib/schema";
import type { ConnectionDraft, DraftTestState } from "../types";

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

export function ConnectionManagerForm({
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
      <label>
        Database engine
        <select
          value={draft.type}
          onChange={(event) => {
            const nextType = event.target.value as ConnectionDraft["type"];
            const nextPort = draft.port === getDefaultPort(draft.type) ? getDefaultPort(nextType) : draft.port;
            onChange({ ...draft, type: nextType, port: nextPort });
          }}
        >
          {DATABASE_TYPE_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>

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
          <input value={draft.user} onChange={(event) => onChange({ ...draft, user: event.target.value })} />
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

      <label>
        {getConnectionTargetLabel(draft.type)}
        <input
          value={draft.database}
          onChange={(event) => onChange({ ...draft, database: event.target.value })}
          placeholder={getConnectionTargetPlaceholder(draft.type)}
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

      <p className="muted">{getConnectionTypeDescription(draft.type)}</p>

      {draftTestState.message ? (
        <p className={`test-message test-${draftTestState.status}`}>{draftTestState.message}</p>
      ) : null}
    </form>
  );
}
