// Runtime discovery: a state.updated model.available announcement must land
// in the provider status cache and steer later sendText model selections,
// including the level-less first-turn resend and the steer's capability wait.
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DEFAULT_SERVER_SETTINGS,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { ZcodeAdapterV2Driver } from "./adapter.ts";
import { resolveZcodeInstanceStatePaths } from "./instanceState.ts";
import { ZCODE_PROVIDER } from "./status.ts";

const layerTest = Layer.mergeAll(NodeServices.layer, IdAllocator.layer);

const ZCODE_INSTANCE_ID = ProviderInstanceId.make(ZCODE_PROVIDER);
const THREAD_ID = ThreadId.make("thread-zcode-discovery-test");
const SESSION_ID = ProviderSessionId.make("provider-session-zcode-discovery-test");
const NATIVE_SESSION_ID = "sess-discovery-1";
const ACCOUNT_PROVIDER = "account:zai-individual-coding-plan";
const ACCOUNT_MODEL = `${ACCOUNT_PROVIDER}/GLM-5.3`;
const LOCAL_MODEL = "ollama-local/gpt-oss:20b";
/** Deliberately outside the valid pid range so ZcodeRpc's group kill never lands. */
const FAKE_PID_BASE = 999_999_960;

const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection = (model: string, effort?: string): ModelSelection => ({
  instanceId: ZCODE_INSTANCE_ID,
  model,
  ...(effort === undefined ? {} : { options: [{ id: "effort", value: effort }] }),
});

// ── Fake app-server (one healthy process) ────────────────────────────────────

interface FakeZcodeRequest {
  readonly id: string | number;
  readonly method: string;
  readonly params: Record<string, unknown>;
}

type FakeZcodeReply =
  | { readonly result: unknown }
  | { readonly error: { readonly code: number; readonly message: string } };

type FakeZcodeNotification = { readonly method: string; readonly params: unknown };

const makeFakeZcodeAppServer = (
  script: (
    request: FakeZcodeRequest,
    notify: (notification: FakeZcodeNotification) => void,
  ) => FakeZcodeReply,
) => {
  const encoder = new TextEncoder();
  const seen: Array<FakeZcodeRequest> = [];
  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      yield* Effect.addFinalizer(() => Effect.sync(() => Queue.endUnsafe(stdout)));
      const notify = (notification: FakeZcodeNotification) => {
        Queue.offerUnsafe(stdout, encoder.encode(`${JSON.stringify(notification)}\n`));
      };
      let buffer = "";
      const handleLine = (line: string) => {
        const record = JSON.parse(line) as Record<string, unknown>;
        if (typeof record["id"] !== "string" && typeof record["id"] !== "number") return;
        if (typeof record["method"] !== "string") return;
        const request: FakeZcodeRequest = {
          id: record["id"] as string | number,
          method: record["method"] as string,
          params: (record["params"] ?? {}) as Record<string, unknown>,
        };
        seen.push(request);
        const reply = script(request, notify);
        const frame =
          "error" in reply
            ? { id: request.id, error: reply.error }
            : { id: request.id, result: reply.result };
        Queue.offerUnsafe(stdout, encoder.encode(`${JSON.stringify(frame)}\n`));
      };
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID_BASE + 1),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.sync(() => {
            buffer += new TextDecoder().decode(chunk);
            while (true) {
              const newline = buffer.indexOf("\n");
              if (newline === -1) return;
              const line = buffer.slice(0, newline).replace(/\r$/u, "");
              buffer = buffer.slice(newline + 1);
              if (line.length > 0) handleLine(line);
            }
          }),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  return { spawner, requests: () => [...seen] };
};

const conversationTopic = `conversation/${NATIVE_SESSION_ID}`;

const ackResult = (request: FakeZcodeRequest, ack: Record<string, unknown>) => ({
  result: { commandId: request.params["commandId"], revisionAtDecision: 0, ...ack },
});

const conversationFrame = (
  ordinal: number,
  deltas: ReadonlyArray<unknown>,
): FakeZcodeNotification => ({
  method: "v4/conversation/frame",
  params: {
    wireVersion: 3,
    kind: "complete",
    deliveryKind: "online",
    logicalFrameId: `lf-${ordinal}`,
    logicalFrameOrdinal: ordinal,
    topic: conversationTopic,
    subscriptionId: "sub-p1",
    frame: {
      topic: conversationTopic,
      subscriptionId: "sub-p1",
      sentAt: 1_000,
      fromSeq: ordinal - 1,
      toSeq: ordinal,
      payload: { kind: "deltas", deltas },
    },
  },
});

const turnHeaderRow = (rowId: number, commandId: string, state: string) => ({
  rowId,
  entityId: `turn-${rowId}`,
  kind: "turnHeader",
  sourceCommandId: commandId,
  state,
  startedAt: 1_000,
  ...(state === "completedSuccess" ? { endedAt: 2_000 } : {}),
});

const assistantTextRow = (rowId: number, text: string) => ({
  rowId,
  entityId: `msg-${rowId}`,
  kind: "assistantText",
  text,
  state: "completed",
});

/** The capability announcement a healthy process emits after the first sendText. */
const modelAvailableNotification = (): FakeZcodeNotification => ({
  method: "state.updated",
  params: {
    patch: {
      model: {
        available: [
          {
            ref: { providerId: ACCOUNT_PROVIDER, modelId: "GLM-5.3" },
            label: "GLM-5.3",
            contextWindow: 200_000,
            reasoning: {
              levels: [
                { value: "low", label: "low" },
                { value: "high", label: "high" },
                { value: "max", label: "max" },
              ],
              defaultLevel: "max",
            },
          },
        ],
        current: { providerId: ACCOUNT_PROVIDER, modelId: "GLM-5.3" },
      },
    },
    reason: "prompt_failed",
    revision: 1,
  },
});

// ── Test host with a temp cache dir ──────────────────────────────────────────

const makeDiscoveryHost = (paths: ProviderHost.ProviderHostPaths) =>
  ProviderHost.ProviderHost.of({
    paths,
    settings: {
      get: Effect.succeed(DEFAULT_SERVER_SETTINGS),
      withSnapshot: (use) => use(DEFAULT_SERVER_SETTINGS),
      changes: Stream.empty,
      subscribe: Effect.succeed(Stream.empty),
    },
    shouldRunBackgroundWork: () => Effect.succeed(true),
    resolveAttachmentPath: () => null,
    credentials: () =>
      Effect.succeed({
        binding: { owner: "t3" as const, key: "zcode-model-discovery-test" },
        get: Effect.succeed(Option.none<Uint8Array>()),
        set: (_credentials: Uint8Array) => Effect.void,
        remove: Effect.void,
      }),
  });

// ── Runtime plumbing ─────────────────────────────────────────────────────────

const openRuntime = Effect.fnUntraced(function* (
  fake: ReturnType<typeof makeFakeZcodeAppServer>,
  host: ProviderHost.ProviderHost["Service"],
  model: string,
  home: string,
  discoveryFs: FileSystem.FileSystem,
) {
  const adapter = yield* ZcodeAdapterV2Driver.create({
    instanceId: ZCODE_INSTANCE_ID,
    displayName: undefined,
    enabled: true,
    environment: [],
    config: { ...ZcodeAdapterV2Driver.defaultConfig(), binaryPath: "zcode", launchArgs: "" },
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provideService(HostProcess.HomeDirectory, home),
    Effect.provideService(ProviderHost.ProviderHost, host),
    Effect.provideService(FileSystem.FileSystem, discoveryFs),
  );
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection: modelSelection(model),
    runtimePolicy,
  });
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: modelSelection(model),
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = <E extends ProviderAdapter.ProviderAdapterV2Event>(
    predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => event is E,
  ): Effect.Effect<E> =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    });
  return { runtime, providerThread, takeEvent };
});

const startTurnInput = (
  providerThread: OrchestrationV2ProviderThread,
  model: string,
  effort: string | undefined,
  ordinal: number,
): ProviderAdapter.ProviderAdapterV2TurnInput => {
  const runId = RunId.make(`run:${THREAD_ID}:${ordinal}`);
  const now = DateTime.makeUnsafe(0);
  const appThread: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:zcode" as OrchestrationV2AppThread["projectId"],
    title: "Zcode discovery test thread",
    providerInstanceId: ZCODE_INSTANCE_ID,
    modelSelection: modelSelection(model, effort),
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return {
    appThread,
    threadId: THREAD_ID,
    runId,
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${ordinal}` as never,
      text: "fixture discovery ok",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: modelSelection(model, effort),
    runtimePolicy,
  };
};

const sendTextPayloads = (fake: ReturnType<typeof makeFakeZcodeAppServer>) =>
  fake
    .requests()
    .filter((request) => request.method === "v4/command" && request.params["type"] === "sendText")
    .map((request) => (request.params["payload"] as Record<string, unknown>) ?? {});

// ── Scenarios ────────────────────────────────────────────────────────────────

// A cache from mixed earlier runs: outdated levels for GLM-5.3 (must be
// overwritten by the announcement), a sibling account model the runtime
// announced earlier (must survive — alternating models must not erase each
// other), and an unrelated provider (must survive).
const STALE_CACHE = JSON.stringify({
  schemaVersion: 1,
  models: {
    [ACCOUNT_MODEL]: { levels: ["stale"], defaultLevel: "stale" },
    [`${ACCOUNT_PROVIDER}/GLM-5.3-Flash`]: {
      levels: ["low", "high", "max"],
      defaultLevel: "max",
      contextWindow: 200_000,
    },
    "ollama-local/gpt-oss:20b": { contextWindow: 8192 },
  },
});

describe("ZcodeAdapterV2 runtime model discovery cache", () => {
  it.effect("persists model.available to the cache and steers later selections", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-zcode-discovery-",
      });
      const cacheDir = path.join(baseDir, "caches");
      const instanceState = yield* resolveZcodeInstanceStatePaths({}, baseDir);
      const cacheFile = path.join(
        cacheDir,
        "zcode",
        instanceState.discoveryCacheScope,
        "model-discovery.json",
      );
      yield* fileSystem.makeDirectory(path.dirname(cacheFile), { recursive: true });
      yield* fileSystem.writeFileString(cacheFile, STALE_CACHE);
      // The discovery flush runs forked off the frame pump, so the test
      // observes the atomic write through its rename instead of assuming it
      // landed before the turn's terminal event.
      const cacheWriteLanded = yield* Deferred.make<void>();
      const observedFs = FileSystem.make({
        ...fileSystem,
        rename: (from: string, to: string) =>
          Effect.tap(fileSystem.rename(from, to), () =>
            to === cacheFile ? Deferred.succeed(cacheWriteLanded, void 0) : Effect.void,
          ),
      });
      const host = makeDiscoveryHost({
        cwd: baseDir,
        baseDir,
        stateDir: baseDir,
        providerStatusCacheDir: cacheDir,
        attachmentsDir: path.join(baseDir, "attachments"),
      });

      let frameOrdinal = 0;
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          const commandId = String(request.params["commandId"]);
          const rowBase = frameOrdinal * 10;
          frameOrdinal += 1;
          if (frameOrdinal === 1) notify(modelAvailableNotification());
          notify(
            conversationFrame(frameOrdinal, [
              { op: "row.appended", row: turnHeaderRow(rowBase + 1, commandId, "running") },
              { op: "row.appended", row: assistantTextRow(rowBase + 2, "fixture discovery ok") },
              {
                op: "row.upserted",
                row: turnHeaderRow(rowBase + 1, commandId, "completedSuccess"),
              },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(
        fake,
        host,
        ACCOUNT_MODEL,
        baseDir,
        observedFs,
      );

      // Turn 1: no levels known yet, so the send carries no reasoning level.
      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 1));
      const first = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(first.status, "completed");

      // The announcement that followed the first send is persisted in a forked
      // flush: await its atomic rename, then assert the same model's stale
      // entry is overwritten while sibling/other-provider entries survive.
      yield* Deferred.await(cacheWriteLanded);
      const cached = JSON.parse(yield* fileSystem.readFileString(cacheFile)) as {
        models: Record<string, unknown>;
      };
      assert.deepStrictEqual(cached.models, {
        [ACCOUNT_MODEL]: {
          levels: ["low", "high", "max"],
          defaultLevel: "max",
          contextWindow: 200_000,
        },
        [`${ACCOUNT_PROVIDER}/GLM-5.3-Flash`]: {
          levels: ["low", "high", "max"],
          defaultLevel: "max",
          contextWindow: 200_000,
        },
        "ollama-local/gpt-oss:20b": { contextWindow: 8192 },
      });

      // Turn 2 (no pick): the discovered default level rides the send.
      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 2));
      const second = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal" && event.runOrdinal === 2,
      );
      assert.equal(second.status, "completed");

      // Turn 3 (Effort pick "low"): the picker's choice beats the default.
      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, "low", 3));
      const third = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal" && event.runOrdinal === 3,
      );
      assert.equal(third.status, "completed");

      const sends = sendTextPayloads(fake).map(
        (payload) => payload["modelSelection"] as Record<string, unknown> | undefined,
      );
      assert.equal(sends.length, 3);
      assert.deepStrictEqual(sends[0], {
        providerId: ACCOUNT_PROVIDER,
        modelId: "GLM-5.3",
      });
      assert.deepStrictEqual(sends[1]?.["options"], { reasoningLevel: "max" });
      assert.deepStrictEqual(sends[2]?.["options"], { reasoningLevel: "low" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

// ── Level-less first-turn resend and steer gating ────────────────────────────

const isProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);

/** Opens a runtime against a throwaway cache dir (no cache assertions needed). */
const openFreshRuntime = Effect.fnUntraced(function* (
  fake: ReturnType<typeof makeFakeZcodeAppServer>,
  model: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3-zcode-resend-",
  });
  const host = makeDiscoveryHost({
    cwd: baseDir,
    baseDir,
    stateDir: baseDir,
    providerStatusCacheDir: path.join(baseDir, "caches"),
    attachmentsDir: path.join(baseDir, "attachments"),
  });
  return yield* openRuntime(fake, host, model, baseDir, fileSystem);
});

const steerTurnInput = (
  providerThread: OrchestrationV2ProviderThread,
  ordinal = 1,
): ProviderAdapter.ProviderAdapterV2SteerInput => ({
  threadId: THREAD_ID,
  runId: RunId.make(`run:${THREAD_ID}:${ordinal}`),
  providerThread,
  providerTurnId: ProviderTurnId.make(`provider-turn:${THREAD_ID}:${ordinal}`),
  message: {
    messageId: `message:${THREAD_ID}:steer:${ordinal}` as never,
    text: "fixture steer",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
  },
});

describe("ZcodeAdapterV2 level-less first-turn resend", () => {
  it.effect("resends with the discovered default level once the announcement lands", () =>
    Effect.gen(function* () {
      let sends = 0;
      let frameOrdinal = 0;
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          sends += 1;
          const commandId = String(request.params["commandId"]);
          if (sends === 1) {
            // zcode accepts the level-less send, then fails the turn demanding
            // a reasoning level; the capability announcement lands right
            // behind the failure, which is what releases the resend's wait.
            notify(
              conversationFrame((frameOrdinal += 1), [
                { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
                { op: "row.upserted", row: turnHeaderRow(1, commandId, "failed") },
              ]),
            );
            notify(modelAvailableNotification());
            return ackResult(request, { status: "accepted" });
          }
          notify(
            conversationFrame((frameOrdinal += 1), [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
              { op: "row.appended", row: assistantTextRow(2, "fixture resend ok") },
              { op: "row.upserted", row: turnHeaderRow(1, commandId, "completedSuccess") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openFreshRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 1));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(terminal.status, "completed");

      const [first, second] = sendTextPayloads(fake).map(
        (payload) => payload["modelSelection"] as Record<string, unknown> | undefined,
      );
      assert.equal(sends, 2);
      assert.isDefined(second);
      assert.deepStrictEqual(first, { providerId: ACCOUNT_PROVIDER, modelId: "GLM-5.3" });
      assert.deepStrictEqual(second?.["options"], { reasoningLevel: "max" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails the level-less turn when no capability announcement lands", () =>
    Effect.gen(function* () {
      let sends = 0;
      let frameOrdinal = 0;
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          sends += 1;
          const commandId = String(request.params["commandId"]);
          notify(
            conversationFrame((frameOrdinal += 1), [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
              { op: "row.upserted", row: turnHeaderRow(1, commandId, "failed") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openFreshRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 1));
      // The resend's wait is bounded by a 500ms clock sleep; advancing the
      // TestClock past it settles the turn failed without a second send.
      yield* TestClock.adjust(Duration.millis(600));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(terminal.status, "failed");
      assert.equal(sends, 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("holds a steer until the model capability announcement lands", () =>
    Effect.gen(function* () {
      // The fake rejects a guide that arrives before the capability
      // announcement was notified, so a steer that skips the capability wait
      // fails instead of landing a level-less guide during the bootstrap.
      let announcementNotified = false;
      const trail: Array<string> = [];
      let frameOrdinal = 0;
      let turnCommandId = "";
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          trail.push("createSession");
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          const commandId = String(request.params["commandId"]);
          if (
            (request.params["payload"] as Record<string, unknown>)?.["requestedDelivery"] ===
            "guide"
          ) {
            trail.push("guide");
            if (!announcementNotified) {
              return { error: { code: -32_000, message: "steer landed before capabilities" } };
            }
            notify(
              conversationFrame((frameOrdinal += 1), [
                { op: "row.appended", row: assistantTextRow(2, "fixture steer ok") },
                { op: "row.upserted", row: turnHeaderRow(1, turnCommandId, "completedSuccess") },
              ]),
            );
            return ackResult(request, { status: "accepted" });
          }
          trail.push("turn-send");
          turnCommandId = commandId;
          notify(
            conversationFrame((frameOrdinal += 1), [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        if (request.method === "v4/command" && request.params["type"] === "compact") {
          trail.push("compact-announce");
          announcementNotified = true;
          notify(modelAvailableNotification());
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openFreshRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 1));
      const steerFiber = yield* Effect.forkChild(runtime.steerTurn(steerTurnInput(providerThread)));
      // Scheduling flush: the steer's slice runs while no announcement exists,
      // so a steer honoring the capability wait parks here.
      yield* Effect.yieldNow;
      const compact = runtime.compactThread;
      assert.isDefined(compact);
      const compactFiber = yield* Effect.forkChild(
        compact(startTurnInput(providerThread, ACCOUNT_MODEL, undefined, 1)),
      );
      // The compact request is what notifies the announcement that releases
      // the parked steer; flush so it runs while the steer stays parked.
      yield* Effect.yieldNow;
      // The steer only completes once its guide landed (or failed), so its
      // exit doubles as the delivery milestone; a permit-less wait shows up
      // as a failed exit here.
      const steerExit = yield* Fiber.await(steerFiber);
      assert.isTrue(Exit.isSuccess(steerExit));
      const compactExit = yield* Fiber.await(compactFiber);
      assert.isTrue(Exit.isSuccess(compactExit));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(terminal.status, "completed");
      assert.deepEqual(trail, ["createSession", "turn-send", "compact-announce", "guide"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("ZcodeAdapterV2 switchModelConfig propagation", () => {
  it.effect("fails the turn and keeps the session model when zcode rejects the switch", () =>
    Effect.gen(function* () {
      const OTHER_MODEL = "other-provider/other:model";
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "switchModelConfig") {
          return ackResult(request, {
            status: "failed",
            reasonCode: "modelUnavailable",
            message: "Provider Registry 中不存在 Model: other:model",
          });
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread } = yield* openFreshRuntime(fake, LOCAL_MODEL);

      const failure = yield* runtime
        .startTurn(startTurnInput(providerThread, OTHER_MODEL, undefined, 1))
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /switchModelConfig/u);
      assert.match(failure.detail, /modelUnavailable/u);
      // The rejected switch must not leak into the session model, and no turn
      // text may be sent against a model zcode never applied.
      assert.equal(runtime.providerSession.model, LOCAL_MODEL);
      assert.equal(sendTextPayloads(fake).length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
