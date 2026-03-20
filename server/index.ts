import "dotenv/config";
import cors from "cors";
import express from "express";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  ConnectionInput,
  DatabaseType,
  discoverSchema,
  executeSql,
  getDialectLabel,
  testConnection,
} from "./database.js";

const app = express();
const port = Number(process.env.PORT ?? 8787);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const defaultOllamaBaseUrl = "http://127.0.0.1:11434";

function resolveOllamaBaseUrl() {
  const configuredValue = process.env.OLLAMA_HOST?.trim() || process.env.OLLAMA_BASE_URL?.trim();
  if (!configuredValue) {
    return defaultOllamaBaseUrl;
  }

  const candidate = /^[a-z][a-z\d+\-.]*:\/\//i.test(configuredValue)
    ? configuredValue
    : `http://${configuredValue}`;

  try {
    const normalized = new URL(candidate);
    return normalized.toString().replace(/\/+$/, "");
  } catch {
    console.warn(
      `Invalid Ollama host configuration "${configuredValue}". Falling back to ${defaultOllamaBaseUrl}.`,
    );
    return defaultOllamaBaseUrl;
  }
}

const ollamaBaseUrl = resolveOllamaBaseUrl();

const clientDistPathCandidates = [
  join(__dirname, "../client"),
  join(process.cwd(), "dist/client"),
];
const clientDistPath = clientDistPathCandidates.find((candidate) => existsSync(join(candidate, "index.html"))) ?? null;

app.use(cors());
app.use(express.json({ limit: "1mb" }));

if (clientDistPath) {
  app.use(express.static(clientDistPath));
}

const databaseTypeSchema = z.enum(["mysql", "mariadb", "postgres", "oracle", "sqlserver", "ignite2", "ignite3"]);

const optionalNameSchema = z.preprocess(
  (value) => {
    if (typeof value !== "string") {
      return undefined;
    }

    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  },
  z.string().min(1).optional(),
);

const connectionSchema = z
  .object({
    type: databaseTypeSchema,
    host: z.string().trim().min(1),
    port: z.coerce.number().int().positive().max(65535),
    user: z.string().trim().default(""),
    password: z.string().catch(""),
    database: optionalNameSchema,
    selectedDatabase: optionalNameSchema,
  })
  .superRefine((input, context) => {
    if (input.type !== "ignite2" && input.type !== "ignite3" && input.user.trim().length === 0) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["user"],
        message: "User is required for this database type.",
      });
    }

    if (input.type === "oracle" && !input.database) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["database"],
        message: "Oracle connections require a service name.",
      });
    }
  });

const sqlSchema = connectionSchema.extend({
  sql: z.string().trim().min(1),
});

const aiRequestSchema = z.object({
  model: z.string().trim().min(1),
  databaseType: databaseTypeSchema,
  selectedDatabase: optionalNameSchema,
  schemaSummary: z.string().trim().max(25000).default(""),
  prompt: z.string().trim().max(8000).default(""),
  sql: z.string().trim().max(40000).optional(),
  history: z
    .array(
      z.object({
        prompt: z.string().trim().max(8000).default(""),
        sql: z.string().trim().max(40000).default(""),
        notes: z.string().trim().max(4000).default(""),
      }),
    )
    .max(6)
    .default([]),
});

type AiOperation = "generate" | "optimize";

type OllamaTagsResponse = {
  models?: Array<{
    name?: string;
    model?: string;
  }>;
};

type OllamaChatResponse = {
  message?: {
    content?: string;
  };
};

async function fetchOllamaJson<T>(path: string, init?: RequestInit) {
  const response = await fetch(`${ollamaBaseUrl}${path}`, init);
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json") ? ((await response.json()) as T) : null;

  if (!response.ok) {
    const message =
      payload && typeof payload === "object" && payload !== null && "error" in payload
        ? String((payload as { error?: unknown }).error ?? `Ollama request failed with status ${response.status}`)
        : `Ollama request failed with status ${response.status}`;
    throw new Error(message);
  }

  return payload;
}

async function listOllamaModels() {
  const payload = await fetchOllamaJson<OllamaTagsResponse>("/api/tags");
  return (payload?.models ?? [])
    .map((model) => model.name ?? model.model ?? "")
    .filter((name) => name.length > 0);
}

function buildAiSystemPrompt(operation: AiOperation, databaseType: DatabaseType) {
  const dialectLabel = getDialectLabel(databaseType);

  if (operation === "generate") {
    return [
      `You are a senior SQL assistant for ${dialectLabel}.`,
      "Generate a syntactically valid SQL query or set of queries based on the user request.",
      "When prior SQL or earlier assistant context is provided, treat the latest request as an iteration on that work unless the user asks to start over.",
      "Match the target dialect's quoting, built-ins, and catalog conventions.",
      "Prefer explicit column names when reasonable, avoid unsafe destructive statements unless the user clearly asks for them.",
      'Respond only as JSON with keys: sql, notes. notes must be a short plain-English explanation.',
    ].join(" ");
  }

  return [
    `You are a senior SQL performance assistant for ${dialectLabel}.`,
    "Optimize the provided SQL while preserving behavior unless the user request explicitly allows semantic changes.",
    "Keep the rewritten SQL valid for the same dialect.",
    "Favor readable SQL, reduced unnecessary work, and better join/filter placement.",
    'Respond only as JSON with keys: sql, notes. notes must briefly explain the optimization decisions and any assumptions.',
  ].join(" ");
}

function buildAiUserPrompt(operation: AiOperation, input: z.infer<typeof aiRequestSchema>) {
  const sections = [
    operation === "generate"
      ? "Task: Generate SQL from a natural-language request or revise the current SQL using the latest feedback."
      : "Task: Optimize an existing SQL query.",
    `Target dialect: ${getDialectLabel(input.databaseType)}`,
    `Selected context: ${input.selectedDatabase ?? "none"}`,
    `Schema summary:\n${input.schemaSummary || "No schema summary provided."}`,
  ];

  if (input.sql) {
    sections.push(`Current working SQL:\n${input.sql}`);
  }

  if (input.history.length > 0) {
    sections.push(
      `Previous assistant context:\n${input.history
        .map((turn, index) =>
          [
            `Iteration ${index + 1} request: ${turn.prompt || "No prompt recorded."}`,
            `Iteration ${index + 1} SQL:\n${turn.sql || "No SQL recorded."}`,
            `Iteration ${index + 1} notes: ${turn.notes || "No notes recorded."}`,
          ].join("\n"),
        )
        .join("\n\n")}`,
    );
  }

  if (operation === "generate") {
    sections.push(`Latest user request:\n${input.prompt || "No prompt provided."}`);
  } else {
    sections.push(`Existing SQL:\n${input.sql || ""}`);
    sections.push(`Optimization goal:\n${input.prompt || "Improve performance and readability while preserving behavior."}`);
  }

  sections.push('Return JSON exactly like {"sql":"...","notes":"..."}');
  return sections.join("\n\n");
}

function parseAiResponse(content: string | undefined) {
  if (!content) {
    throw new Error("Ollama returned an empty response.");
  }

  const parsed = JSON.parse(content) as { sql?: unknown; notes?: unknown };
  if (typeof parsed.sql !== "string" || parsed.sql.trim().length === 0) {
    throw new Error("Ollama did not return SQL in the expected format.");
  }

  return {
    sql: parsed.sql.trim(),
    notes: typeof parsed.notes === "string" ? parsed.notes.trim() : "",
  };
}

async function runAiTask(operation: AiOperation, input: z.infer<typeof aiRequestSchema>) {
  const payload = await fetchOllamaJson<OllamaChatResponse>("/api/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: input.model,
      stream: false,
      format: "json",
      messages: [
        {
          role: "system",
          content: buildAiSystemPrompt(operation, input.databaseType),
        },
        {
          role: "user",
          content: buildAiUserPrompt(operation, input),
        },
      ],
    }),
  });

  return parseAiResponse(payload?.message?.content);
}

function parseConnectionInput(body: unknown): ConnectionInput {
  const parsed = connectionSchema.safeParse(body);
  if (!parsed.success) {
    const error = new Error("Invalid connection payload.");
    (error as Error & { issues?: unknown }).issues = parsed.error.issues;
    throw error;
  }

  return parsed.data;
}

app.get("/api/health", (_request, response) => {
  response.json({ ok: true });
});

app.post("/api/schema", async (request, response) => {
  try {
    const input = parseConnectionInput(request.body);
    response.json(await discoverSchema(input));
  } catch (error) {
    const issues = (error as Error & { issues?: unknown }).issues;
    response.status(issues ? 400 : 500).json({
      message: issues ? "Invalid connection payload." : "Failed to discover schema.",
      issues,
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/test-connection", async (request, response) => {
  try {
    const input = parseConnectionInput(request.body);
    response.json({
      ok: true,
      ...(await testConnection(input)),
    });
  } catch (error) {
    const issues = (error as Error & { issues?: unknown }).issues;
    response.status(issues ? 400 : 500).json({
      message: issues ? "Invalid connection payload." : "Failed to connect to the database.",
      issues,
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/query", async (request, response) => {
  const parsed = sqlSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid query payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  try {
    response.json(await executeSql(parsed.data, parsed.data.sql));
  } catch (error) {
    response.status(500).json({
      message: "Failed to execute SQL.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.get("/api/ai/models", async (_request, response) => {
  try {
    const models = await listOllamaModels();
    response.json({
      models,
      baseUrl: ollamaBaseUrl,
    });
  } catch (error) {
    response.status(502).json({
      message: "Failed to reach the configured Ollama server.",
      baseUrl: ollamaBaseUrl,
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/ai/generate-sql", async (request, response) => {
  const parsed = aiRequestSchema.safeParse(request.body);
  if (!parsed.success) {
    response.status(400).json({
      message: "Invalid AI request payload.",
      issues: parsed.error.issues,
    });
    return;
  }

  try {
    response.json(await runAiTask("generate", parsed.data));
  } catch (error) {
    response.status(502).json({
      message: "Failed to generate SQL with Ollama.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.post("/api/ai/optimize-sql", async (request, response) => {
  const parsed = aiRequestSchema.safeParse(request.body);
  if (!parsed.success || !parsed.data.sql) {
    response.status(400).json({
      message: "Invalid AI optimization payload.",
      issues: parsed.success ? [{ message: "sql is required" }] : parsed.error.issues,
    });
    return;
  }

  try {
    response.json(await runAiTask("optimize", parsed.data));
  } catch (error) {
    response.status(502).json({
      message: "Failed to optimize SQL with Ollama.",
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.get("/{*path}", (_request, response) => {
  if (!clientDistPath) {
    response.status(404).json({
      message: "Client bundle not found. Run `npm run dev` for the Vite UI or `npm run build` before `npm start`.",
    });
    return;
  }

  response.sendFile(join(clientDistPath, "index.html"));
});

app.listen(port, () => {
  console.log(`SQL Ninja API listening on http://localhost:${port}`);
});
