/**
 * @module log
 *
 * Structured JSON logger. Every line is a parseable JSON object for
 * Datadog, CloudWatch, Grafana Loki, etc. Uses stderr for errors so
 * they're distinguishable in container runtimes.
 */

type Level = "info" | "warn" | "error";

/** Emit a structured JSON log line. */
export function log(level: Level, msg: string, extra?: Record<string, unknown>) {
  const entry = JSON.stringify({
    ts: new Date().toISOString(),
    level,
    msg,
    ...extra,
  });
  if (level === "error" || level === "warn") console.error(entry);
  else console.log(entry);
}
