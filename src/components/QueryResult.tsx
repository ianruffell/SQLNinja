import type { CellResult } from "../types";

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

export function QueryResult({ result }: { result: CellResult }) {
  return (
    <section className="result-panel">
      <div className="result-meta">
        <strong>Last run:</strong> {new Date(result.executedAt).toLocaleString()} | <strong>Execution time:</strong>{" "}
        {formatDuration(result.durationMs)}
      </div>

      {result.statements.map((statement, index) => (
        <div key={index} className="statement-block">
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
