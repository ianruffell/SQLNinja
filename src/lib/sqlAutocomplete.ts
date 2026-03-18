import type { SchemaNode, SqlAutocompleteContext, SqlAutocompleteQuery, SqlSuggestion } from "../types";

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

function stripIdentifierQuotes(value: string) {
  return value.replaceAll("`", "").replaceAll('"', "").trim();
}

function formatIdentifier(value: string) {
  return `\`${value.replaceAll("`", "``")}\``;
}

export function buildSqlAutocompleteContext(schema: SchemaNode | null, selectedDatabase: string | null): SqlAutocompleteContext {
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

export function getSqlAutocompleteQuery(sql: string, caretPosition: number): SqlAutocompleteQuery {
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

export function buildSqlAutocompleteSuggestions(
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
