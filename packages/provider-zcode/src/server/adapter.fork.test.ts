// The orchestrator forks on a fresh session opened for the target thread, so the
// source comes from forkInput.sourceProviderThread; a requested turn resolves to
// its native turnHeader row through nativeTurnRef, never silently the latest.
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { ZcodeAdapterV2Driver } from "./adapter.ts";
import { ZCODE_PROVIDER } from "./status.ts";

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

const ZCODE_INSTANCE_ID = ProviderInstanceId.make(ZCODE_PROVIDER);
const THREAD_ID = ThreadId.make("thread-zcode-fork-test");
const SESSION_ID = ProviderSessionId.make("provider-session-zcode-fork-test");
const NATIVE_SESSION_ID = "sess-fork-src";
const FORKED_SESSION_ID = "sess-fork-child";
const LOCAL_MODEL = "ollama-local/gpt-oss:20b";
/** Deliberately outside the valid pid range so ZcodeRpc's group kill never lands. */
const FAKE_PID = 999_999_970;

const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection = (model: string): ModelSelection => ({
  instanceId: ZCODE_INSTANCE_ID,
  model,
});

const isProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);

// ── Fake app-server ──────────────────────────────────────────────────────────

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
        const notifications: Array<FakeZcodeNotification> = [];
        const reply = script(request, (notification) => notifications.push(notification));
        const frame =
          "error" in reply
            ? { id: request.id, error: reply.error }
            : { id: request.id, result: reply.result };
        Queue.offerUnsafe(stdout, encoder.encode(`${JSON.stringify(frame)}\n`));
        // The recorded wire order is the ack first, then the initial frame.
        for (const notification of notifications) notify(notification);
      };
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
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

// ── Wire shapes ──────────────────────────────────────────────────────────────

const conversationTopic = `conversation/${NATIVE_SESSION_ID}`;

const ackResult = (request: FakeZcodeRequest, ack: Record<string, unknown>) => ({
  result: { commandId: request.params["commandId"], revisionAtDecision: 0, ...ack },
});

const turnHeaderRow = (rowId: number) => ({
  rowId,
  entityId: `turn-${rowId}`,
  kind: "turnHeader",
  sourceCommandId: `command-${rowId}`,
  state: "completedSuccess",
  startedAt: 1_000,
  endedAt: 2_000,
});

const assistantTextRow = (rowId: number, text: string) => ({
  rowId,
  entityId: `msg-${rowId}`,
  kind: "assistantText",
  text,
  state: "completed",
});

/** The persisted source conversation: turn headers at rows 1 and 3. */
const SOURCE_REVISION = 7;
const SOURCE_LOG_EPOCH = "epoch-src";
const sourceSnapshotFrame = (): FakeZcodeNotification => ({
  method: "v4/conversation/frame",
  params: {
    wireVersion: 3,
    kind: "complete",
    deliveryKind: "initial",
    logicalFrameId: "lf-source-snapshot",
    logicalFrameOrdinal: 1,
    topic: conversationTopic,
    subscriptionId: "sub-source",
    frame: {
      topic: conversationTopic,
      subscriptionId: "sub-source",
      sentAt: 1_000,
      fromSeq: 0,
      toSeq: 4,
      payload: {
        kind: "snapshot",
        snapshot: {
          sessionId: NATIVE_SESSION_ID,
          logEpoch: SOURCE_LOG_EPOCH,
          seq: 4,
          revision: SOURCE_REVISION,
          rows: {
            window: [
              turnHeaderRow(1),
              assistantTextRow(2, "first answer"),
              turnHeaderRow(3),
              assistantTextRow(4, "second answer"),
            ],
            totalCount: 4,
            firstRowId: 1,
          },
        },
      },
    },
  },
});

// ── Runtime plumbing ─────────────────────────────────────────────────────────

/** A session as the orchestrator opens it for a fork target: no ensureThread. */
const openFreshRuntime = Effect.fnUntraced(function* (
  fake: ReturnType<typeof makeFakeZcodeAppServer>,
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
  );
  return yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection: modelSelection(LOCAL_MODEL),
    runtimePolicy,
  });
});

const makeAppThread = (): OrchestrationV2AppThread => {
  const now = DateTime.makeUnsafe(0);
  return {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:zcode" as OrchestrationV2AppThread["projectId"],
    title: "Zcode fork test thread",
    providerInstanceId: ZCODE_INSTANCE_ID,
    modelSelection: modelSelection(LOCAL_MODEL),
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
  } satisfies OrchestrationV2AppThread;
};

const startTurnInput = (
  providerThread: OrchestrationV2ProviderThread,
  ordinal: number,
): ProviderAdapter.ProviderAdapterV2TurnInput => {
  const runId = RunId.make(`run:${THREAD_ID}:${ordinal}`);
  return {
    appThread: makeAppThread(),
    threadId: THREAD_ID,
    runId,
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${ordinal}` as never,
      text: `fixture fork turn ${ordinal}`,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: modelSelection(LOCAL_MODEL),
    runtimePolicy,
  };
};

const forkAssistantRequests = (fake: ReturnType<typeof makeFakeZcodeAppServer>) =>
  fake
    .requests()
    .filter(
      (request) => request.method === "v4/command" && request.params["type"] === "forkAssistant",
    );

const forkTarget = (request: FakeZcodeRequest) =>
  (request.params["payload"] as Record<string, unknown>)["target"];

const strongRef = (nativeId: string) => ({
  driver: ZCODE_PROVIDER,
  nativeId,
  strength: "strong" as const,
});

/** The source provider thread as the orchestrator reads it back from its projection. */
const sourceProviderThread = (nativeThreadId: string | null): OrchestrationV2ProviderThread => {
  const now = DateTime.makeUnsafe(0);
  return {
    id: ProviderThreadId.make("provider-thread:zcode-fork-source"),
    driver: ZCODE_PROVIDER,
    providerInstanceId: ZCODE_INSTANCE_ID,
    providerSessionId: ProviderSessionId.make("provider-session-zcode-fork-source"),
    appThreadId: THREAD_ID,
    ownerNodeId: null,
    nativeThreadRef: nativeThreadId === null ? null : strongRef(nativeThreadId),
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
};

const sourceTurn = (ordinal: number, nativeTurnId: string | null): OrchestrationV2ProviderTurn => {
  const now = DateTime.makeUnsafe(0);
  return {
    id: ProviderTurnId.make(`provider-turn:zcode-fork-source:${ordinal}`),
    providerThreadId: sourceProviderThread(NATIVE_SESSION_ID).id,
    nodeId: NodeId.make(`node:zcode-fork-source:${ordinal}`),
    runAttemptId: null,
    nativeTurnRef: nativeTurnId === null ? null : strongRef(nativeTurnId),
    ordinal,
    status: "completed",
    startedAt: now,
    completedAt: now,
  };
};

/** Serves the persisted source snapshot on subscribe, like a fresh zcode process. */
const makeForkFake = () =>
  makeFakeZcodeAppServer((request, notify) => {
    if (request.method === "v4/conversation/subscribe") {
      if (request.params["topic"] === conversationTopic) notify(sourceSnapshotFrame());
      return { result: { ack: { subscriptionId: "sub-1" } } };
    }
    if (request.method === "v4/command" && request.params["type"] === "forkAssistant") {
      return ackResult(request, {
        status: "accepted",
        result: { sessionId: FORKED_SESSION_ID },
      });
    }
    return { error: { code: -32_601, message: `unexpected ${request.method}` } };
  });

// ── Scenarios ────────────────────────────────────────────────────────────────

describe("ZcodeAdapterV2 forkThread on a fresh session", () => {
  const turn1 = sourceTurn(1, "turn-1");
  const turn2 = sourceTurn(2, "turn-3");

  it.effect("forks from the requested first turn, not the latest", () =>
    Effect.gen(function* () {
      const fake = makeForkFake();
      const runtime = yield* openFreshRuntime(fake);

      const forked = yield* runtime.forkThread({
        sourceProviderThread: sourceProviderThread(NATIVE_SESSION_ID),
        sourceProviderTurns: [turn1, turn2],
        providerTurnId: turn1.id,
        targetThreadId: ThreadId.make("thread-zcode-fork-child"),
      });

      const [request] = forkAssistantRequests(fake);
      assert.isDefined(request);
      assert.equal(request.params["sessionId"], NATIVE_SESSION_ID);
      assert.equal(request.params["baseRevision"], SOURCE_REVISION);
      assert.equal(request.params["baseLogEpoch"], SOURCE_LOG_EPOCH);
      assert.deepStrictEqual(forkTarget(request), { rowId: 1, entityId: "turn-1" });
      assert.equal(forked.nativeThreadRef?.nativeId, FORKED_SESSION_ID);
      assert.deepStrictEqual(forked.forkedFrom, {
        providerThreadId: sourceProviderThread(NATIVE_SESSION_ID).id,
        providerTurnId: turn1.id,
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("forks from the latest turn when no turn is requested", () =>
    Effect.gen(function* () {
      const fake = makeForkFake();
      const runtime = yield* openFreshRuntime(fake);

      yield* runtime.forkThread({
        sourceProviderThread: sourceProviderThread(NATIVE_SESSION_ID),
        sourceProviderTurns: [turn1, turn2],
        targetThreadId: ThreadId.make("thread-zcode-fork-child"),
      });

      const [request] = forkAssistantRequests(fake);
      assert.isDefined(request);
      assert.deepStrictEqual(forkTarget(request), { rowId: 3, entityId: "turn-3" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails when the requested turn has no native turn reference", () =>
    Effect.gen(function* () {
      const fake = makeForkFake();
      const runtime = yield* openFreshRuntime(fake);

      const failure = yield* runtime
        .forkThread({
          sourceProviderThread: sourceProviderThread(NATIVE_SESSION_ID),
          sourceProviderTurns: [sourceTurn(1, null), turn2],
          providerTurnId: turn1.id,
          targetThreadId: ThreadId.make("thread-zcode-fork-child"),
        })
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /no native turn reference/u);
      assert.equal(forkAssistantRequests(fake).length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails when the source provider thread has no native session reference", () =>
    Effect.gen(function* () {
      const fake = makeForkFake();
      const runtime = yield* openFreshRuntime(fake);

      const failure = yield* runtime
        .forkThread({
          sourceProviderThread: sourceProviderThread(null),
          sourceProviderTurns: [turn1, turn2],
          targetThreadId: ThreadId.make("thread-zcode-fork-child"),
        })
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /no native session reference/u);
      assert.equal(fake.requests().length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails without forking when the source conversation cannot be loaded", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => ({
        error: { code: -32_602, message: `unknown session for ${request.method}` },
      }));
      const runtime = yield* openFreshRuntime(fake);

      const failure = yield* runtime
        .forkThread({
          sourceProviderThread: sourceProviderThread(NATIVE_SESSION_ID),
          sourceProviderTurns: [turn1, turn2],
          targetThreadId: ThreadId.make("thread-zcode-fork-child"),
        })
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /subscribe failed/u);
      assert.equal(forkAssistantRequests(fake).length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("ZcodeAdapterV2 forkThread on a session with a turn", () => {
  it.effect("refuses to fork while its own turn is active", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: "sub-1" } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const runtime = yield* openFreshRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection(LOCAL_MODEL),
        runtimePolicy,
      });
      yield* runtime.startTurn(startTurnInput(providerThread, 1));

      const failure = yield* runtime
        .forkThread({
          sourceProviderThread: providerThread,
          sourceProviderTurns: [],
          targetThreadId: ThreadId.make("thread-zcode-fork-child"),
        })
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /active turn/u);
      assert.equal(forkAssistantRequests(fake).length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
