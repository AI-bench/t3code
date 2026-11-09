import { pipe } from "effect/Function";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { ProviderReplayEntry, ProviderReplayTranscript } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  makeZcodeRecordingSpawner,
  ZCODE_RPC_REPLAY_PROTOCOL,
  ZcodeOrchestratorReplayHarness,
} from "../src/orchestration-v2/Adapters/ZcodeAdapterV2.testkit.ts";
import * as ZcodeAdapterV2Testkit from "../src/orchestration-v2/Adapters/ZcodeAdapterV2.testkit.ts";
import { ZCODE_PROVIDER } from "@t3tools/provider-zcode/server";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import { materializeFixtureInput } from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  checkpointWorkspace,
  makeCheckpointWorkspace,
} from "@t3tools/provider-testing/replayWorkspace";

const CLOCK_TICK = Duration.millis(20);

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenario = readArgValue("--scenario");
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((entry) => entry.name === scenario);
const variant = fixture?.providers.find((provider) => provider.driver === ZCODE_PROVIDER);
if (fixture === undefined || variant === undefined) {
  const names = ORCHESTRATOR_REPLAY_FIXTURES.filter((entry) =>
    entry.providers.some((provider) => provider.driver === ZCODE_PROVIDER),
  ).map((entry) => entry.name);
  throw new Error(`Pass --scenario with a fixture that registers zcode: ${names.join(", ")}`);
}

const zcodeBinary = process.env.T3_ZCODE_BIN ?? "zcode";
const home = process.env.HOME ?? "";
const debugDumpDir = process.env.T3_ZCODE_DEBUG_DUMP;

const UUID_PATTERN = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const ISO_TIMESTAMP_PATTERN = /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b/gu;
const SESSION_ID_PATTERN = /\bsess_[0-9a-f-]{20,}\b/gu;
const EPOCH_STRING_PATTERN = /\b[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}\b/gu;
const NORMALIZED_EPOCH_MS = DateTime.toEpochMillis(DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"));

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Normalize both directions consistently so echoed ids, paths and times match on replay.
function normalizeEntries(input: {
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
  readonly workspace: string;
}): ReadonlyArray<ProviderReplayEntry> {
  const uuids = new Map<string, string>();
  const sessionIds = new Map<string, string>();
  const epochStrings = new Map<string, string>();
  let timeAnchorMs: number | undefined;
  const shiftTime = (ms: number): number => {
    timeAnchorMs ??= ms;
    return NORMALIZED_EPOCH_MS + (ms - timeAnchorMs);
  };
  const normalizeString = (value: string): string =>
    value
      .replaceAll(input.workspace, "<workspace>")
      .replaceAll(home.length > 1 ? home : "\0", "~")
      .replace(SESSION_ID_PATTERN, (sessionId) => {
        const existing = sessionIds.get(sessionId);
        if (existing !== undefined) return existing;
        const next = `sess_normalized_${sessionIds.size + 1}`;
        sessionIds.set(sessionId, next);
        return next;
      })
      .replace(ISO_TIMESTAMP_PATTERN, (iso) =>
        pipe(
          DateTime.makeUnsafe(iso),
          DateTime.toEpochMillis,
          shiftTime,
          DateTime.makeUnsafe,
          DateTime.formatIso,
        ),
      )
      .replace(UUID_PATTERN, (uuid) => {
        const existing = uuids.get(uuid.toLowerCase());
        if (existing !== undefined) return existing;
        // Plain "uuid-N" (never hex-grouped) so later pattern replacements,
        // notably the log-epoch one, cannot corrupt the placeholder.
        const next = `uuid-${uuids.size + 1}`;
        uuids.set(uuid.toLowerCase(), next);
        return next;
      })
      .replace(EPOCH_STRING_PATTERN, (epoch) => {
        const existing = epochStrings.get(epoch);
        if (existing !== undefined) return existing;
        const next = `epoch-${epochStrings.size + 1}`;
        epochStrings.set(epoch, next);
        return next;
      });
  const normalizeValue = (value: unknown, key?: string): unknown => {
    if (typeof value === "string") return normalizeString(value);
    if (typeof value === "number" && /At$|_at$/u.test(key ?? "") && value > 1e12) {
      return shiftTime(value);
    }
    if (Array.isArray(value)) return value.map((entry) => normalizeValue(entry));
    if (!isRecord(value)) return value;
    return Object.fromEntries(
      Object.entries(value).map(([entryKey, entry]) => [entryKey, normalizeValue(entry, entryKey)]),
    );
  };
  return input.entries.map((entry) =>
    entry.type === "runtime_exit" ? entry : { ...entry, frame: normalizeValue(entry.frame) },
  );
}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function encodeTranscriptNdjson(transcript: ProviderReplayTranscript): string {
  const { entries, ...metadata } = transcript;
  return [
    encodeJson({ type: "transcript_start", ...metadata }),
    ...entries.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
}

// Follow wall time for live zcode timers while retaining replay's TestClock steps.
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const readZcodeVersion = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const output = yield* spawner.string(ChildProcess.make(zcodeBinary, ["--version"]));
  return output.trim();
});

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const zcodeVersion = yield* readZcodeVersion;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const fixtureInput = fixture.buildInput();
  const workspace = yield* Effect.promise(() =>
    makeCheckpointWorkspace(`zcode-record-${fixture.name}`, fixtureInput.workspaceFiles),
  );
  yield* Effect.addFinalizer(() =>
    fs.remove(workspace, { recursive: true, force: true }).pipe(Effect.ignore),
  );

  const entries: Array<ProviderReplayEntry> = [];
  const placeholder: ProviderReplayTranscript = {
    provider: ZCODE_PROVIDER,
    protocol: ZCODE_RPC_REPLAY_PROTOCOL,
    version: zcodeVersion,
    scenario: fixture.name,
    entries: [],
  };
  const layerRecordingSpawner = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
      makeZcodeRecordingSpawner(live, entries),
    ),
  ).pipe(Layer.provide(NodeServices.layer));

  const materializeInput = materializeFixtureInput({
    scenario: fixture.name,
    fixtureInput,
    driver: variant.driver,
    modelSelection: variant.modelSelection,
  }).pipe(Effect.provide(IdAllocator.layer));
  const continuationOptions =
    variant.runContinuationWorker === true ? { runContinuationWorker: true } : {};

  yield* Effect.gen(function* () {
    yield* followWallClock.pipe(Effect.forkScoped);
    const materialized = yield* materializeInput;
    yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/zcode:record`,
        transcript: placeholder,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: workspace },
      },
      {
        driver: ZCODE_PROVIDER,
        decodeTranscript: Effect.succeed,
        makeProviderAdapterRegistryLayer: () =>
          ZcodeAdapterV2Testkit.layer({
            scenario: fixture.name,
            binaryPath: zcodeBinary,
            launchArgs: "",
            environment: [
              {
                name: "ZCODE_PERSONAL_PROVIDER_CONFIG_FILE",
                value: process.env.T3_ZCODE_PROVIDER_CONFIG ?? "",
                sensitive: false,
              },
            ],
            spawner: layerRecordingSpawner,
          }),
      },
      continuationOptions,
    );
  }).pipe(
    Effect.tapError((_cause) => {
      // Failed recordings lose in-memory entries; dump them for diagnosis.
      const dumpDir = debugDumpDir;
      return dumpDir === undefined
        ? Effect.void
        : fs
            .writeFileString(
              `${dumpDir}/record-${scenario}-FAILED-entries.json`,
              encodeJson(entries),
            )
            .pipe(Effect.ignore);
    }),
    Effect.scoped,
    provideDeterministicTestRuntime,
  );

  const transcript = {
    ...placeholder,
    metadata: {
      generatedBy: "live-zcode-recorder",
      zcodeVersion,
      model: variant.modelSelection.model,
    },
    entries: normalizeEntries({ entries, workspace }),
  } satisfies ProviderReplayTranscript;

  // Optional diagnosis aid: dump the raw and normalized entries before verify.
  if (debugDumpDir !== undefined) {
    yield* fs
      .writeFileString(`${debugDumpDir}/record-${scenario}-entries.json`, encodeJson(entries))
      .pipe(Effect.ignore);
    yield* fs
      .writeFileString(
        `${debugDumpDir}/record-${scenario}-normalized.json`,
        encodeJson(transcript.entries),
      )
      .pipe(Effect.ignore);
  }

  // Assert on normalized replay; the live projection has raw paths. Failed assertions preserve the fixture.
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
    const materialized = yield* materializeInput;
    const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/zcode:verify`,
        transcript: yield* ZcodeOrchestratorReplayHarness.decodeTranscript(transcript),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      ZcodeOrchestratorReplayHarness,
      continuationOptions,
    );
    if (debugDumpDir !== undefined) {
      yield* fs
        .writeFileString(
          `${debugDumpDir}/record-${scenario}-replay-result.json`,
          encodeJson(replayResult),
        )
        .pipe(Effect.ignore);
    }
    variant.assertOutput(replayResult, transcript);
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  yield* fs.makeDirectory(path.dirname(outputPath), { recursive: true });
  yield* fs.writeFileString(outputPath, encodeTranscriptNdjson(transcript));
  yield* Console.log(`Wrote ${transcript.entries.length} zcode replay entries to ${outputPath}`);
});

await Effect.runPromise(record.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
