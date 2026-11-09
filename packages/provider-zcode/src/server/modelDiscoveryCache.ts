// Runtime model discovery cache: the app-server announces the models it
// really serves (levels, default, context window) via state.updated
// model.available, after a model's first sendText. Persisting them lets the
// status probe enrich its config-derived list pre-session, per account scope.
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";

import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";

export interface ZcodeDiscoveredModelInfo {
  readonly levels?: ReadonlyArray<string>;
  readonly defaultLevel?: string;
  readonly contextWindow?: number;
}

/** Model slug (`providerId/modelId`) → what the runtime announced for it. */
export type ZcodeModelDiscoveryCache = Readonly<Record<string, ZcodeDiscoveredModelInfo>>;

interface DiscoveryCacheFile {
  readonly schemaVersion?: number;
  readonly models?: Record<string, unknown>;
}

const SCHEMA_VERSION = 1;
const CACHE_DIRECTORY = "zcode";
const CACHE_FILE_NAME = "model-discovery.json";

// Sessions of one process share a cache file; writes are read-merge-write, so
// a process-wide permit keeps them serialized. Across processes the write is
// a temp-file rename, so a reader never sees a partial file.
const writePermit = Semaphore.makeUnsafe(1);

function parseModelEntry(raw: unknown): ZcodeDiscoveredModelInfo | null {
  if (typeof raw !== "object" || raw === null) return null;
  const entry = raw as Record<string, unknown>;
  // Level ids/labels and the default become provider option choices, whose
  // contracts demand trimmed non-empty strings; a hand-edited cache must not
  // smuggle blanks past the parser.
  const rawLevels = Array.isArray(entry["levels"])
    ? entry["levels"]
        .map((level) => (typeof level === "string" ? level.trim() : ""))
        .filter((level) => level.length > 0)
    : null;
  const levels = rawLevels !== null && rawLevels.length > 0 ? rawLevels : undefined;
  const defaultLevelText =
    typeof entry["defaultLevel"] === "string" ? entry["defaultLevel"].trim() : "";
  const defaultLevel = defaultLevelText.length > 0 ? defaultLevelText : undefined;
  const contextWindow =
    typeof entry["contextWindow"] === "number" ? entry["contextWindow"] : undefined;
  if (levels === undefined && defaultLevel === undefined && contextWindow === undefined)
    return null;
  return {
    ...(levels !== undefined ? { levels } : {}),
    ...(defaultLevel !== undefined ? { defaultLevel } : {}),
    ...(contextWindow !== undefined ? { contextWindow } : {}),
  };
}

/** Pure parser, unit-testable; missing or corrupt files yield an empty cache. */
export function parseZcodeModelDiscoveryCache(raw: string): ZcodeModelDiscoveryCache {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (typeof parsed !== "object" || parsed === null) return {};
  const file = parsed as DiscoveryCacheFile;
  if (file.schemaVersion !== SCHEMA_VERSION) return {};
  const models: Record<string, ZcodeDiscoveredModelInfo> = {};
  for (const [slug, rawEntry] of Object.entries(file.models ?? {})) {
    if (!slug.includes("/")) continue;
    const entry = parseModelEntry(rawEntry);
    if (entry !== null) models[slug] = entry;
  }
  return models;
}

const cacheFilePath = (cacheDir: string, scope: string) =>
  Effect.map(Path.Path, (path) => path.join(cacheDir, CACHE_DIRECTORY, scope, CACHE_FILE_NAME));

/** Pure serializer; kept out of Effect code (JSON.stringify throws). */
function serializeDiscoveryCache(models: ZcodeModelDiscoveryCache): string {
  return `${JSON.stringify({ schemaVersion: SCHEMA_VERSION, models }, null, 2)}\n`;
}

/** Best-effort read: a missing or corrupt cache behaves like no discovery yet. */
export const readZcodeModelDiscoveryCache = Effect.fn("ZcodeModelDiscovery.read")(function* (
  cacheDir: string | null,
  scope: string,
) {
  if (cacheDir === null) return {};
  const fileSystem = yield* FileSystem.FileSystem;
  const raw = yield* fileSystem
    .readFileString(yield* cacheFilePath(cacheDir, scope))
    .pipe(Effect.orElseSucceed(() => ""));
  return parseZcodeModelDiscoveryCache(raw);
});

/**
 * Merges one announcement per model: announced slugs overwrite, sibling
 * entries survive (live processes announce one model at a time, so a
 * per-provider replace would erase alternating models' levels). Cache-only
 * phantoms stay impossible: status lists config-derived models only.
 */
export const writeZcodeModelDiscoveryCache = Effect.fn("ZcodeModelDiscovery.write")(function* (
  cacheDir: string,
  announced: ZcodeModelDiscoveryCache,
  scope: string,
) {
  yield* Effect.gen(function* () {
    const existing = yield* readZcodeModelDiscoveryCache(cacheDir, scope);
    const merged: Record<string, ZcodeDiscoveredModelInfo> = { ...existing };
    for (const [slug, info] of Object.entries(announced)) merged[slug] = info;
    const serialized = serializeDiscoveryCache(merged);
    if (serialized === serializeDiscoveryCache(existing)) return;
    yield* writeFileStringAtomically({
      filePath: yield* cacheFilePath(cacheDir, scope),
      contents: serialized,
    });
  }).pipe(Semaphore.withPermits(writePermit, 1));
});
