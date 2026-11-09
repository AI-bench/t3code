import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderReplayEntry,
  ProviderInstanceId,
  type ResolvedProviderInstanceEnvironment,
  type ProviderReplayTranscript,
  type ProviderReplayEntry as ProviderReplayEntryType,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import type { OrchestratorV2ProviderReplayHarness } from "../testkit/ProviderReplayHarness.ts";
import { ZCODE_PROVIDER, ZcodeAdapterV2Driver } from "@t3tools/provider-zcode/server";

export const ZCODE_RPC_REPLAY_PROTOCOL = "zcode.rpc-jsonl";

const ZcodeRpcReplayTranscript = Schema.Struct({
  provider: Schema.Literal(ZCODE_PROVIDER),
  protocol: Schema.Literal(ZCODE_RPC_REPLAY_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
type ZcodeRpcReplayTranscript = typeof ZcodeRpcReplayTranscript.Type;
const decodeZcodeRpcReplayTranscript = Schema.decodeUnknownEffect(ZcodeRpcReplayTranscript);

class ZcodeReplayTranscriptDecodeError extends Schema.TaggedError<ZcodeReplayTranscriptDecodeError>()(
  "ZcodeReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode zcode replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

class ZcodeReplayMismatchError extends Schema.TaggedError<ZcodeReplayMismatchError>()(
  "ZcodeReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    process: Schema.Number,
    label: Schema.optional(Schema.String),
    method: Schema.optional(Schema.String),
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `zcode replay frame mismatch at cursor ${this.cursor} (expected ${this.label ?? "<none>"}, received ${this.method ?? "<untyped>"} from process ${this.process}) in scenario ${this.scenario}.`;
  }
}

class ZcodeReplayIncompleteError extends Schema.TaggedError<ZcodeReplayIncompleteError>()(
  "ZcodeReplayIncompleteError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    remaining: Schema.Number,
    nextLabel: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return `zcode replay ended with ${this.remaining} unconsumed entries at cursor ${this.cursor} (next ${this.nextLabel ?? "<none>"}) in scenario ${this.scenario}.`;
  }
}

function entryLabel(entry: ProviderReplayEntryType | undefined): string | undefined {
  if (entry === undefined) return undefined;
  return entry.type === "runtime_exit" ? "runtime_exit" : (entry.label ?? entry.type);
}

const ZcodeOrchestratorReplayHarnessError = Schema.Union([
  ZcodeReplayTranscriptDecodeError,
  ProviderAdapterDriverCreateError,
]);
type ZcodeOrchestratorReplayHarnessError = typeof ZcodeOrchestratorReplayHarnessError.Type;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Adapter request ids and v4 command ids are random per run; rebinding keeps
// transcripts stable. Session ids (sess_*) echo back and are normalized at
// record time instead.
const ADAPTER_REQUEST_ID = /^t3-\d+$/u;
const COMMAND_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;
// Recorder placeholders that stand for values the replay run cannot repeat
// verbatim: the checkpoint workspace differs per run, and normalized session /
// log-epoch ids flow through frames untouched unless rebound.
const WORKSPACE_MARKER = "<workspace>";
const HOME_MARKER = "~";
const SESSION_MARKER = /^sess_normalized_\d+$/u;
const EPOCH_MARKER = /^epoch-\d+$/u;
const UUID_MARKER = /^uuid-\d+$/u;

function isRebindable(value: unknown): value is string {
  return (
    typeof value === "string" &&
    (ADAPTER_REQUEST_ID.test(value) ||
      COMMAND_ID.test(value) ||
      value === WORKSPACE_MARKER ||
      value === HOME_MARKER ||
      SESSION_MARKER.test(value) ||
      EPOCH_MARKER.test(value) ||
      UUID_MARKER.test(value))
  );
}

/** Structural match that collects rebindable leaf pairs (expected → actual). */
function matchCollecting(
  expected: unknown,
  actual: unknown,
  pairs: Map<string, string>,
  key?: string,
): boolean {
  // Path markers stand for run-specific locations; any non-empty actual path
  // binds (the recorder normalized whatever the recording run used).
  if (expected === WORKSPACE_MARKER || expected === HOME_MARKER) {
    if (typeof actual !== "string" || actual.length === 0) return false;
    const existing = pairs.get(expected);
    if (existing !== undefined && existing !== actual) return false;
    pairs.set(expected, actual);
    return true;
  }
  if (isRebindable(expected)) {
    if (!isRebindable(actual)) return false;
    const existing = pairs.get(expected);
    if (existing !== undefined && existing !== actual) return false;
    pairs.set(expected, actual);
    return true;
  }
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((entry, index) => matchCollecting(entry, actual[index], pairs))
    );
  }
  if (isRecord(expected)) {
    if (!isRecord(actual)) return false;
    const keys = new Set([...Object.keys(expected), ...Object.keys(actual)]);
    return [...keys].every((field) =>
      matchCollecting(expected[field], actual[field], pairs, field),
    );
  }
  // Timestamps under *At fields follow each run's own clock (TestClock in the
  // harness, wall clock when recorded live); only key shape is comparable.
  if (typeof expected === "number" && typeof actual === "number" && /At$/u.test(key ?? "")) {
    return true;
  }
  return Object.is(expected, actual);
}

/** Deep-replace recorded rebindable ids with the adapter's actual ids. */
function rebind(value: unknown, pairs: Map<string, string>): unknown {
  if (typeof value === "string") {
    const mapped = pairs.get(value);
    return mapped === undefined ? value : mapped;
  }
  if (Array.isArray(value)) return value.map((entry) => rebind(entry, pairs));
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, rebind(entry, pairs)]),
    );
  }
  return value;
}

function startsTurn(entry: ProviderReplayEntryType): boolean {
  if (entry.type !== "expect_outbound") return false;
  const method = isRecord(entry.frame) ? entry.frame.method : undefined;
  if (method !== "v4/command") return false;
  const params = isRecord(entry.frame) ? entry.frame.params : undefined;
  const type = isRecord(params) ? params.type : undefined;
  return type === "sendText" || type === "compact" || type === "createSession";
}

function processStartFrame(command: ChildProcess.Command) {
  const args = ChildProcess.isStandardCommand(command) ? command.args : [];
  return { type: "process_start", args: [...args] };
}

function recordLabel(frame: unknown, process: number): string | undefined {
  const base = !isRecord(frame)
    ? undefined
    : typeof frame.method === "string"
      ? frame.method
      : frame.type === "process_start"
        ? "process_start"
        : undefined;
  if (base === undefined || process === 1) return base;
  return `${base}@p${process}`;
}

const decodeJsonLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function makeLineSplitter() {
  let buffer = "";
  const decoder = new TextDecoder();
  return (chunk: Uint8Array): ReadonlyArray<string> => {
    buffer += decoder.decode(chunk, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    return lines.map((line) => line.replace(/\r$/u, "")).filter((line) => line.length > 0);
  };
}

interface ZcodeReplayProcess {
  readonly ordinal: number;
  readonly stdout: Queue.Queue<Uint8Array, Cause.Done>;
  /** recorded id → adapter id, learned from matched outbound writes. */
  readonly ids: Map<string, string>;
  open: boolean;
}

// Method-bearing frames get @pN labels, but JSON-RPC responses carry no
// method and stay unlabeled; derive every entry's process from the last
// process_start marker at or before it instead of its label.
function computeProcessOf(entries: ReadonlyArray<ProviderReplayEntryType>): ReadonlyArray<number> {
  let current = 0;
  return entries.map((entry) => {
    if (
      entry.type === "expect_outbound" &&
      isRecord(entry.frame) &&
      entry.frame.type === "process_start"
    ) {
      current += 1;
    }
    return Math.max(current, 1);
  });
}

/** Adapter fibers can reorder stdin writes; hold matches within the turn until reached, and fail unmatched writes. */
class ZcodeReplayController {
  private cursor = 0;
  private failure: unknown = null;
  private readonly consumed = new Set<number>();
  private readonly held: Array<{ readonly process: ZcodeReplayProcess; readonly actual: unknown }> =
    [];
  private readonly processes: Array<ZcodeReplayProcess> = [];
  private readonly transcript: ZcodeRpcReplayTranscript;
  private readonly processOf: ReadonlyArray<number>;

  constructor(transcript: ZcodeRpcReplayTranscript) {
    this.transcript = transcript;
    this.processOf = computeProcessOf(transcript.entries);
  }

  spawn(
    command: ChildProcess.Command,
    stdout: Queue.Queue<Uint8Array, Cause.Done>,
  ): ZcodeReplayProcess {
    const process: ZcodeReplayProcess = {
      ordinal: this.processes.length + 1,
      stdout,
      ids: new Map(),
      open: true,
    };
    this.processes.push(process);
    this.receive(process, processStartFrame(command));
    return process;
  }

  receive(process: ZcodeReplayProcess, actual: unknown): void {
    if (this.failure !== null) return;
    const entries = this.transcript.entries;
    let possible = false;
    for (let index = this.cursor; index < entries.length && !possible; index += 1) {
      const entry = entries[index]!;
      const pending = entry.type === "expect_outbound" && !this.consumed.has(index);
      if (!pending) continue;
      possible =
        this.processOf[index] === process.ordinal &&
        matchCollecting(entry.frame, actual, process.ids);
      if (!possible && startsTurn(entry)) break;
    }
    if (!possible) {
      const expected = entries[this.cursor];
      const label = entryLabel(expected);
      const method =
        isRecord(actual) && typeof actual.method === "string" ? actual.method : undefined;
      this.fail(
        new ZcodeReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor,
          process: process.ordinal,
          ...(label === undefined ? {} : { label }),
          ...(method === undefined ? {} : { method }),
          actual,
        }),
      );
      return;
    }
    this.held.push({ process, actual });
    this.advance();
  }

  close(process: ZcodeReplayProcess): void {
    process.open = false;
    Queue.endUnsafe(process.stdout);
  }

  assertComplete(): void {
    if (this.failure !== null) throw this.failure;
    if (this.cursor !== this.transcript.entries.length || this.held.length > 0) {
      const nextLabel = entryLabel(this.transcript.entries[this.cursor]);
      throw new ZcodeReplayIncompleteError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        remaining: this.transcript.entries.length - this.cursor,
        ...(nextLabel === undefined ? {} : { nextLabel }),
      });
    }
  }

  private advance(): void {
    let progressed = true;
    while (progressed && this.failure === null) {
      progressed = this.drainInbound();
      for (const [heldIndex, write] of this.held.entries()) {
        if (this.consumePending(write.process, write.actual)) {
          this.held.splice(heldIndex, 1);
          progressed = true;
          break;
        }
      }
    }
  }

  private consumePending(process: ZcodeReplayProcess, actual: unknown): boolean {
    const entries = this.transcript.entries;
    for (let index = this.cursor; index < entries.length; index += 1) {
      const entry = entries[index]!;
      if (entry.type !== "expect_outbound") return false;
      if (this.consumed.has(index) || this.processOf[index] !== process.ordinal) continue;
      if (!matchCollecting(entry.frame, actual, process.ids)) continue;
      this.consumed.add(index);
      while (this.consumed.has(this.cursor)) this.cursor += 1;
      return true;
    }
    return false;
  }

  private drainInbound(): boolean {
    let emitted = false;
    while (this.failure === null) {
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type === "runtime_exit") {
        this.cursor += 1;
        emitted = true;
        const process = this.processes.at(-1);
        if (process !== undefined) this.close(process);
        continue;
      }
      if (entry?.type !== "emit_inbound") return emitted;
      const process = this.processes[(this.processOf[this.cursor] ?? 1) - 1];
      if (process === undefined) return emitted;
      this.cursor += 1;
      emitted = true;
      if (!process.open) continue;
      const frame = rebind(entry.frame, process.ids);
      Queue.offerUnsafe(process.stdout, new TextEncoder().encode(`${encodeJsonLine(frame)}\n`));
    }
    return emitted;
  }

  private fail(cause: unknown): void {
    if (this.failure !== null) return;
    this.failure = cause;
    // Surface the first divergence immediately; scenario-level errors would
    // otherwise mask it until the harness finalizer runs.
    globalThis.process?.stderr?.write(`[zcode-replay] controller failed: ${String(cause)}\n`);
    for (const process of this.processes) Queue.endUnsafe(process.stdout);
  }
}

/** Deliberately outside the valid pid range so ZcodeRpc's group kill never lands. */
const REPLAY_PID = 999_999_998;

function makeZcodeReplaySpawner(
  controller: ZcodeReplayController,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const process = controller.spawn(command, yield* Queue.unbounded<Uint8Array, Cause.Done>());
      yield* Effect.addFinalizer(() => Effect.sync(() => controller.close(process)));
      const split = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(REPLAY_PID),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() => {
            for (const line of split(chunk)) controller.receive(process, decodeJsonLine(line));
          }),
        ),
        stdout: Stream.fromQueue(process.stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
}

export function layer<E, R>(input: {
  readonly scenario: string;
  readonly spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner, E, R>;
  readonly binaryPath: string;
  readonly launchArgs: string;
  readonly environment?: ResolvedProviderInstanceEnvironment;
}) {
  return ProviderAdapterRegistry.layerFromAdaptersEffect(
    ZcodeAdapterV2Driver.create({
      instanceId: ProviderInstanceId.make(ZCODE_PROVIDER),
      environment: input.environment ?? [],
      displayName: undefined,
      enabled: true,
      config: {
        ...ZcodeAdapterV2Driver.defaultConfig(),
        binaryPath: input.binaryPath,
        launchArgs: input.launchArgs,
      },
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(input.spawner),
    Layer.provide(
      Layer.mergeAll(
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
        IdAllocator.layer,
      ),
    ),
  );
}

function metadataString(transcript: ZcodeRpcReplayTranscript, key: string): string {
  const value = transcript.metadata?.[key];
  return typeof value === "string" ? value : "";
}

export const ZcodeOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  ZcodeRpcReplayTranscript,
  ZcodeOrchestratorReplayHarnessError
> = {
  driver: ZCODE_PROVIDER,
  decodeTranscript: (transcript: ProviderReplayTranscript) =>
    decodeZcodeRpcReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new ZcodeReplayTranscriptDecodeError({
            driver: transcript.provider,
            protocol: transcript.protocol,
            scenario: transcript.scenario,
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (transcript) =>
    layer({
      scenario: transcript.scenario,
      binaryPath: "zcode",
      launchArgs: metadataString(transcript, "launchArgs"),
      spawner: Layer.effect(
        ChildProcessSpawner.ChildProcessSpawner,
        Effect.gen(function* () {
          const controller = new ZcodeReplayController(transcript);
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.assertComplete()));
          return makeZcodeReplaySpawner(controller);
        }),
      ),
    }),
};

export function makeZcodeRecordingSpawner(
  live: ChildProcessSpawner.ChildProcessSpawner["Service"],
  entries: Array<ProviderReplayEntryType>,
): ChildProcessSpawner.ChildProcessSpawner["Service"] {
  let spawned = 0;
  return ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const handle = yield* live.spawn(command);
      const process = ++spawned;
      const record = (type: "expect_outbound" | "emit_inbound", frame: unknown) => {
        const label = recordLabel(frame, process);
        entries.push({ type, ...(label === undefined ? {} : { label }), frame });
      };
      const recordLines = (
        type: "expect_outbound" | "emit_inbound",
        lines: ReadonlyArray<string>,
      ) => {
        for (const line of lines) {
          try {
            record(type, decodeJsonLine(line));
          } catch {
            // ZcodeRpc drops non-JSON stdout lines, so the transcript does too.
          }
        }
      };
      record("expect_outbound", processStartFrame(command));
      const splitIn = makeLineSplitter();
      const splitOut = makeLineSplitter();
      return ChildProcessSpawner.makeHandle({
        ...handle,
        stdin: handle.stdin.pipe(
          Sink.mapInput((chunk: Uint8Array) => {
            recordLines("expect_outbound", splitIn(chunk));
            return chunk;
          }),
        ),
        stdout: handle.stdout.pipe(
          Stream.tap((chunk) => Effect.sync(() => recordLines("emit_inbound", splitOut(chunk)))),
        ),
      });
    }),
  );
}
