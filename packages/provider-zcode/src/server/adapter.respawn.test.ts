// Born-broken app-server recovery: account-model sendText rejected
// modelUnavailable means the process never registered the account registry;
// the adapter must respawn once and resend, and surface anything else.
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
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
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
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
const THREAD_ID = ThreadId.make("thread-zcode-respawn-test");
const SESSION_ID = ProviderSessionId.make("provider-session-zcode-respawn-test");
const NATIVE_SESSION_ID = "sess-respawn-1";
const ACCOUNT_MODEL = "account:zai-individual-coding-plan/GLM-5.3";
const LOCAL_MODEL = "ollama-local/gpt-oss:20b";
/** Deliberately outside the valid pid range so ZcodeRpc's group kill never lands. */
const FAKE_PID_BASE = 999_999_980;

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

// ── Fake multi-process app-server ────────────────────────────────────────────

interface FakeZcodeRequest {
  readonly process: number;
  readonly id: string | number;
  readonly method: string;
  readonly params: Record<string, unknown>;
}

type FakeZcodeReply =
  | { readonly result: unknown }
  | { readonly error: { readonly code: number; readonly message: string } };

type FakeZcodeNotification = { readonly method: string; readonly params: unknown };

type FakeZcodeScript = (
  request: FakeZcodeRequest,
  notify: (notification: FakeZcodeNotification) => void,
) => FakeZcodeReply | Effect.Effect<FakeZcodeReply>;

interface FakeZcodeServer {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly spawnCount: () => number;
  readonly requests: () => ReadonlyArray<FakeZcodeRequest>;
  /** End the newest process's stdout, as when the app-server dies. */
  readonly closeStdout: () => void;
}

const makeFakeZcodeAppServer = (
  script: FakeZcodeScript,
  options: { readonly onSpawned?: (ordinal: number) => Effect.Effect<void> } = {},
): FakeZcodeServer => {
  const encoder = new TextEncoder();
  const seen: Array<FakeZcodeRequest> = [];
  let spawns = 0;
  let liveStdout: Queue.Queue<Uint8Array, Cause.Done> | null = null;

  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const ordinal = (spawns += 1);
      if (options.onSpawned !== undefined) yield* options.onSpawned(ordinal);
      const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      liveStdout = stdout;
      yield* Effect.addFinalizer(() => Effect.sync(() => Queue.endUnsafe(stdout)));
      const notify = (notification: FakeZcodeNotification) => {
        Queue.offerUnsafe(stdout, encoder.encode(`${JSON.stringify(notification)}\n`));
      };
      let buffer = "";
      const handleLine = (line: string): Effect.Effect<void> => {
        const record = JSON.parse(line) as Record<string, unknown>;
        if (typeof record["id"] !== "string" && typeof record["id"] !== "number") {
          return Effect.void;
        }
        if (typeof record["method"] !== "string") return Effect.void;
        const request: FakeZcodeRequest = {
          process: ordinal,
          id: record["id"] as string | number,
          method: record["method"] as string,
          params: (record["params"] ?? {}) as Record<string, unknown>,
        };
        seen.push(request);
        const queueReply = (reply: FakeZcodeReply) => {
          const frame =
            "error" in reply
              ? { id: request.id, error: reply.error }
              : { id: request.id, result: reply.result };
          Queue.offerUnsafe(stdout, encoder.encode(`${JSON.stringify(frame)}\n`));
        };
        const reply = script(request, notify);
        return Effect.isEffect(reply)
          ? Effect.map(reply, queueReply)
          : Effect.sync(() => queueReply(reply));
      };
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID_BASE + ordinal),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.gen(function* () {
            buffer += new TextDecoder().decode(chunk);
            while (true) {
              const newline = buffer.indexOf("\n");
              if (newline === -1) return;
              const line = buffer.slice(0, newline).replace(/\r$/u, "");
              buffer = buffer.slice(newline + 1);
              if (line.length > 0) yield* handleLine(line);
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

  return {
    spawner,
    spawnCount: () => spawns,
    requests: () => [...seen],
    closeStdout: () => {
      if (liveStdout !== null) Queue.endUnsafe(liveStdout);
    },
  } satisfies FakeZcodeServer;
};

// ── Wire shapes (minimal forms of the recorded fixtures) ─────────────────────

const conversationTopic = `conversation/${NATIVE_SESSION_ID}`;

const ackResult = (request: FakeZcodeRequest, ack: Record<string, unknown>) => ({
  result: { commandId: request.params["commandId"], revisionAtDecision: 0, ...ack },
});

const modelUnavailableAck = (request: FakeZcodeRequest, model: string) =>
  ackResult(request, {
    status: "failed",
    reasonCode: "modelUnavailable",
    message: `Provider Registry 中不存在 Model: ${model}`,
  });

const conversationFrame = (
  subscriptionId: string,
  ordinal: number,
  deltas: ReadonlyArray<unknown>,
  topic: string = conversationTopic,
): FakeZcodeNotification => ({
  method: "v4/conversation/frame",
  params: {
    wireVersion: 3,
    kind: "complete",
    deliveryKind: "online",
    logicalFrameId: `lf-${ordinal}`,
    logicalFrameOrdinal: ordinal,
    topic,
    subscriptionId,
    frame: {
      topic,
      subscriptionId,
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

// ── Runtime plumbing ─────────────────────────────────────────────────────────

const openRuntime = Effect.fnUntraced(function* (fake: FakeZcodeServer, model: string) {
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

const makeAppThread = (model: string): OrchestrationV2AppThread => {
  const now = DateTime.makeUnsafe(0);
  return {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:zcode" as OrchestrationV2AppThread["projectId"],
    title: "Zcode respawn test thread",
    providerInstanceId: ZCODE_INSTANCE_ID,
    modelSelection: modelSelection(model),
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
  model: string,
  ordinal = 1,
): ProviderAdapter.ProviderAdapterV2TurnInput => {
  const runId = RunId.make(`run:${THREAD_ID}:${ordinal}`);
  return {
    appThread: makeAppThread(model),
    threadId: THREAD_ID,
    runId,
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${ordinal}` as never,
      text: "fixture respawn ok",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: modelSelection(model),
    runtimePolicy,
  };
};

const sendTextRequests = (fake: FakeZcodeServer) =>
  fake
    .requests()
    .filter((request) => request.method === "v4/command" && request.params["type"] === "sendText");

const sendTextPayload = (request: FakeZcodeRequest) =>
  (request.params["payload"] as Record<string, unknown> | undefined) ?? {};

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
    text: "steer mid-respawn",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
  },
});

const commandTrail = (fake: FakeZcodeServer) =>
  fake.requests().map((request) => ({
    process: request.process,
    kind: (request.params["type"] as string | undefined) ?? request.method,
  }));

// ── Scenarios ────────────────────────────────────────────────────────────────

describe("ZcodeAdapterV2 app-server respawn on account-model modelUnavailable", () => {
  it.effect("respawns the wedged process once, resubscribes, and retries the send", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          // Process 1 was born without the account registry and rejects every
          // account send; process 2 is healthy and completes the turn.
          if (request.process === 1) return modelUnavailableAck(request, ACCOUNT_MODEL);
          const commandId = String(request.params["commandId"]);
          notify(
            conversationFrame(`sub-p${request.process}`, 1, [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
              { op: "row.appended", row: assistantTextRow(2, "fixture respawn ok") },
              { op: "row.upserted", row: turnHeaderRow(1, commandId, "completedSuccess") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );

      assert.equal(terminal.status, "completed");
      assert.equal(fake.spawnCount(), 2);
      assert.deepStrictEqual(commandTrail(fake), [
        { process: 1, kind: "createSession" },
        { process: 1, kind: "v4/conversation/subscribe" },
        { process: 1, kind: "sendText" },
        { process: 2, kind: "v4/conversation/subscribe" },
        { process: 2, kind: "sendText" },
      ]);
      // The respawn re-subscribed the same native conversation on process 2…
      const resubscribe = fake
        .requests()
        .find((request) => request.process === 2 && request.method === "v4/conversation/subscribe");
      assert.isDefined(resubscribe);
      assert.equal((resubscribe?.params["topic"] as string | undefined) ?? "", conversationTopic);
      // …and the retried send targeted the account model under a fresh command id.
      const sends = sendTextRequests(fake).map((request) => ({
        process: request.process,
        commandId: String(request.params["commandId"]),
        providerId: (
          (request.params["payload"] as Record<string, unknown> | undefined)?.["modelSelection"] as
            | Record<string, unknown>
            | undefined
        )?.["providerId"] as string | undefined,
      }));
      assert.equal(sends.length, 2);
      assert.equal(sends[0]?.providerId, "account:zai-individual-coding-plan");
      assert.equal(sends[1]?.providerId, "account:zai-individual-coding-plan");
      assert.notEqual(sends[0]?.commandId, sends[1]?.commandId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("surfaces the error after a second modelUnavailable on the respawned process", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          return modelUnavailableAck(request, ACCOUNT_MODEL);
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread } = yield* openRuntime(fake, ACCOUNT_MODEL);

      const failure = yield* runtime
        .startTurn(startTurnInput(providerThread, ACCOUNT_MODEL))
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /v4\/command sendText was failed \(modelUnavailable\)/u);
      // Exactly one respawn: the retry on process 2 failed, so no process 3.
      assert.equal(fake.spawnCount(), 2);
      assert.deepStrictEqual(commandTrail(fake), [
        { process: 1, kind: "createSession" },
        { process: 1, kind: "v4/conversation/subscribe" },
        { process: 1, kind: "sendText" },
        { process: 2, kind: "v4/conversation/subscribe" },
        { process: 2, kind: "sendText" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not respawn for a modelUnavailable on a non-account model", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          return modelUnavailableAck(request, LOCAL_MODEL);
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread } = yield* openRuntime(fake, LOCAL_MODEL);

      const failure = yield* runtime
        .startTurn(startTurnInput(providerThread, LOCAL_MODEL))
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /modelUnavailable/u);
      assert.equal(fake.spawnCount(), 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not respawn for other send errors (e.g. token login failure)", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          return ackResult(request, {
            status: "failed",
            reasonCode: "project_token_login_failed",
            message: "Project token login failed; please retry",
          });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread } = yield* openRuntime(fake, ACCOUNT_MODEL);

      const failure = yield* runtime
        .startTurn(startTurnInput(providerThread, ACCOUNT_MODEL))
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /project_token_login_failed/u);
      assert.equal(fake.spawnCount(), 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("re-binds to a fresh draft when the fresh process cannot subscribe the old draft", () =>
    Effect.gen(function* () {
      // The brand-new-thread case: registerThread created a draft session on
      // the born-broken process, the first sendText rejects modelUnavailable,
      // and the respawned process never persisted that draft — its
      // re-subscribe is refused. The thread must recover, not wedge.
      const freshNativeSessionId = "sess-respawn-2";
      const oldTopic = `conversation/${NATIVE_SESSION_ID}`;
      const freshTopic = `conversation/${freshNativeSessionId}`;
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: {
              sessionId: request.process === 1 ? NATIVE_SESSION_ID : freshNativeSessionId,
            },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          // Process 2 never persisted process 1's in-memory draft session.
          if (request.process === 2 && request.params["topic"] === oldTopic) {
            return { error: { code: -32_001, message: `unknown conversation ${oldTopic}` } };
          }
          return {
            result: {
              ack: { subscriptionId: `sub-p${request.process}-${request.params["topic"]}` },
            },
          };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          if (request.process === 1) return modelUnavailableAck(request, ACCOUNT_MODEL);
          const subscriptionId = `sub-p2-${freshTopic}`;
          const commandId = String(request.params["commandId"]);
          notify(
            conversationFrame(
              subscriptionId,
              1,
              [
                { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
                { op: "row.appended", row: assistantTextRow(2, "fixture respawn ok") },
                { op: "row.upserted", row: turnHeaderRow(1, commandId, "completedSuccess") },
              ],
              freshTopic,
            ),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );

      assert.equal(terminal.status, "completed");
      assert.equal(fake.spawnCount(), 2);
      assert.deepStrictEqual(commandTrail(fake), [
        { process: 1, kind: "createSession" },
        { process: 1, kind: "v4/conversation/subscribe" },
        { process: 1, kind: "sendText" },
        { process: 2, kind: "v4/conversation/subscribe" },
        { process: 2, kind: "createSession" },
        { process: 2, kind: "v4/conversation/subscribe" },
        { process: 2, kind: "sendText" },
      ]);
      // The retried send targeted the re-bound draft session…
      const sends = sendTextRequests(fake);
      const retrySend = sends[sends.length - 1];
      assert.equal(retrySend?.process, 2);
      assert.equal(retrySend?.params["sessionId"], freshNativeSessionId);
      // …and the thread's native ref moved with it, so later turns (and a
      // resume after a restart) use the session the live process knows.
      const reEnsured = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection(ACCOUNT_MODEL),
        runtimePolicy,
      });
      assert.equal(reEnsured.nativeThreadRef?.nativeId, freshNativeSessionId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("surfaces a re-subscribe failure for a conversation that already ran", () =>
    Effect.gen(function* () {
      // A conversation with history must not be silently re-bound to an empty
      // draft: when the fresh process cannot re-hydrate it, the turn fails
      // with the subscribe error and the native session id is preserved.
      let sendCount = 0;
      const fake = makeFakeZcodeAppServer((request, notify) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          if (request.process === 2) {
            return { error: { code: -32_001, message: "conversation not found" } };
          }
          return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "sendText") {
          sendCount += 1;
          if (request.process === 1 && sendCount === 1) {
            const commandId = String(request.params["commandId"]);
            notify(
              conversationFrame("sub-p1", 1, [
                { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
                { op: "row.appended", row: assistantTextRow(2, "first turn ok") },
                { op: "row.upserted", row: turnHeaderRow(1, commandId, "completedSuccess") },
              ]),
            );
            return ackResult(request, { status: "accepted" });
          }
          return modelUnavailableAck(request, ACCOUNT_MODEL);
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, ACCOUNT_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, 1));
      const firstTerminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(firstTerminal.status, "completed");

      const failure = yield* runtime
        .startTurn(startTurnInput(providerThread, ACCOUNT_MODEL, 2))
        .pipe(Effect.flip);

      assert.ok(isProtocolError(failure));
      assert.match(failure.detail, /v4\/conversation\/subscribe failed/u);
      assert.equal(fake.spawnCount(), 2);
      const reEnsured = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection(ACCOUNT_MODEL),
        runtimePolicy,
      });
      assert.equal(reEnsured.nativeThreadRef?.nativeId, NATIVE_SESSION_ID);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("holds a steer that lands inside the respawn window for the fresh process", () =>
    Effect.gen(function* () {
      // The steer fires mid-respawn: process 1 terminated, process 2's spawn
      // held open. The milestones keep that window closed until the steer has
      // run its slice, so a permit-less steer fails against the dead
      // transport, and the gated subscribe rejects any mid-window guide.
      const spawn2Started = yield* Deferred.make<void>();
      const holdSpawn2 = yield* Deferred.make<void>();
      const subscribe2Entered = yield* Deferred.make<void>();
      const releaseSubscribe2 = yield* Deferred.make<void>();
      const guideArrived = yield* Deferred.make<void>();
      let subscribe2Answered = false;
      let guideSeensRetry = false;
      const stateUpdatedAnnouncement = {
        method: "state.updated",
        params: {
          patch: {
            model: {
              available: [
                {
                  ref: {
                    providerId: "account:zai-individual-coding-plan",
                    modelId: "GLM-5.3",
                  },
                  reasoning: {
                    levels: [{ value: "low" }, { value: "high" }],
                    defaultLevel: "high",
                  },
                },
              ],
            },
          },
        },
      } as const;
      let retryCommandId = "";
      const fake = makeFakeZcodeAppServer(
        (request, notify) => {
          if (request.method === "v4/command" && request.params["type"] === "createSession") {
            return ackResult(request, {
              status: "accepted",
              result: { sessionId: NATIVE_SESSION_ID },
            });
          }
          if (request.method === "v4/conversation/subscribe") {
            // Pre-teach process 1's model capabilities so the steer is not
            // held back by the capability wait when it fires.
            if (request.process === 1) notify(stateUpdatedAnnouncement);
            if (request.process === 2) {
              // Park the respawn inside its critical section until the test
              // releases it, so the steer's park precedes the window opening.
              return Effect.map(
                Effect.andThen(
                  Deferred.succeed(subscribe2Entered, void 0),
                  Deferred.await(releaseSubscribe2),
                ),
                () => {
                  subscribe2Answered = true;
                  return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
                },
              );
            }
            return { result: { ack: { subscriptionId: `sub-p${request.process}` } } };
          }
          if (request.method === "v4/command" && request.params["type"] === "sendText") {
            const subscriptionId = `sub-p${request.process}`;
            if (request.process === 1) {
              const commandId = String(request.params["commandId"]);
              notify(
                conversationFrame(`sub-p${request.process}`, 1, [
                  { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
                ]),
              );
              return modelUnavailableAck(request, ACCOUNT_MODEL);
            }
            if (sendTextPayload(request)["requestedDelivery"] === "guide") {
              // Witness: the guide must not be deliverable while the respawn
              // critical section is still inside its subscribe.
              if (!subscribe2Answered) {
                return { error: { code: -32_000, message: "steer landed inside respawn" } };
              }
              // The queued guide landed on the fresh process; finish the turn.
              if (retryCommandId !== "") {
                notify(
                  conversationFrame(subscriptionId, 2, [
                    { op: "row.appended", row: assistantTextRow(2, "fixture respawn ok") },
                    {
                      op: "row.upserted",
                      row: turnHeaderRow(1, retryCommandId, "completedSuccess"),
                    },
                  ]),
                );
              } else {
                guideSeensRetry = true;
              }
              return Effect.as(
                Deferred.succeed(guideArrived, void 0),
                ackResult(request, { status: "accepted" }),
              );
            }
            retryCommandId = String(request.params["commandId"]);
            notify(
              conversationFrame(subscriptionId, 1, [
                { op: "row.appended", row: turnHeaderRow(1, retryCommandId, "running") },
              ]),
            );
            if (guideSeensRetry) {
              notify(
                conversationFrame(subscriptionId, 2, [
                  { op: "row.appended", row: assistantTextRow(2, "fixture respawn ok") },
                  { op: "row.upserted", row: turnHeaderRow(1, retryCommandId, "completedSuccess") },
                ]),
              );
            }
            return ackResult(request, { status: "accepted" });
          }
          return { error: { code: -32_601, message: `unexpected ${request.method}` } };
        },
        {
          onSpawned: (ordinal) =>
            ordinal === 2
              ? Effect.andThen(Deferred.succeed(spawn2Started, void 0), Deferred.await(holdSpawn2))
              : Effect.void,
        },
      );
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, ACCOUNT_MODEL);

      const turnFiber = yield* Effect.forkChild(
        runtime.startTurn(startTurnInput(providerThread, ACCOUNT_MODEL)),
      );
      // Milestone: the respawn terminated process 1 and holds process 2's
      // spawn open — the steer below fires inside that window.
      yield* Deferred.await(spawn2Started);
      const steerFiber = yield* Effect.forkChild(runtime.steerTurn(steerTurnInput(providerThread)));
      // Scheduling flush: the steer's slice runs while process 1 is dead and
      // process 2 unspawned, so a permit-less steer fails right here and a
      // permitted one parks. The waker milestones then prove it stayed parked
      // until the fresh process answered its subscribe.
      yield* Effect.yieldNow;
      yield* Deferred.succeed(holdSpawn2, void 0);
      yield* Deferred.await(subscribe2Entered);
      yield* Deferred.succeed(releaseSubscribe2, void 0);
      // A permit-less steer fails before the guide can land, so race the guide
      // milestone against steer completion to surface that failure fast.
      yield* Effect.raceFirst(Deferred.await(guideArrived), Fiber.join(steerFiber));

      const steerExit = yield* Fiber.await(steerFiber);
      assert.isTrue(Exit.isSuccess(steerExit));
      const turnExit = yield* Fiber.await(turnFiber);
      assert.isTrue(Exit.isSuccess(turnExit));
      const terminal = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      );
      assert.equal(terminal.status, "completed");

      const guideSend = sendTextRequests(fake).find(
        (request) => sendTextPayload(request)["requestedDelivery"] === "guide",
      );
      assert.isDefined(guideSend);
      assert.equal(guideSend?.process, 2);
      assert.equal(guideSend?.params["sessionId"], NATIVE_SESSION_ID);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

const snapshotFrame = (
  subscriptionId: string,
  seq: number,
  rows: ReadonlyArray<unknown>,
): FakeZcodeNotification => ({
  method: "v4/conversation/frame",
  params: {
    wireVersion: 3,
    kind: "complete",
    deliveryKind: "resync",
    logicalFrameId: `lf-snapshot-${seq}`,
    logicalFrameOrdinal: seq,
    topic: conversationTopic,
    subscriptionId,
    frame: {
      topic: conversationTopic,
      subscriptionId,
      sentAt: 1_000,
      fromSeq: 0,
      toSeq: seq,
      payload: {
        kind: "snapshot",
        snapshot: {
          sessionId: NATIVE_SESSION_ID,
          logEpoch: "epoch-1",
          seq,
          revision: seq,
          rows: { window: rows, totalCount: rows.length, firstRowId: 1 },
        },
      },
    },
  },
});

const isTurnTerminal = (
  event: ProviderAdapter.ProviderAdapterV2Event,
): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
  event.type === "turn.terminal";

describe("ZcodeAdapterV2 transport loss and frame gaps", () => {
  it.effect("fails the active turn and the session when the app-server dies mid-turn", () =>
    Effect.gen(function* () {
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
          notify(
            conversationFrame("sub-p1", 1, [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, LOCAL_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, LOCAL_MODEL));
      fake.closeStdout();
      const terminal = yield* takeEvent(isTurnTerminal);
      const sessionError = yield* takeEvent(
        (
          event,
        ): event is Extract<
          ProviderAdapter.ProviderAdapterV2Event,
          { type: "provider_session.updated" }
        > => event.type === "provider_session.updated" && event.providerSession.status === "error",
      );

      assert.equal(terminal.status, "failed");
      assert.equal(terminal.failure?.class, "transport_error");
      assert.equal(sessionError.providerSession.lastError, "ZCode process exited unexpectedly.");
      assert.equal(fake.spawnCount(), 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resyncs on a sequence gap and settles the turn from the snapshot", () =>
    Effect.gen(function* () {
      let commandId = "";
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
          commandId = String(request.params["commandId"]);
          notify(
            conversationFrame("sub-p1", 1, [
              { op: "row.appended", row: turnHeaderRow(1, commandId, "running") },
            ]),
          );
          // Frames (1, 2] went missing: this one starts at 2, not at 1.
          notify(
            conversationFrame("sub-p1", 3, [
              { op: "row.upserted", row: turnHeaderRow(1, commandId, "completedSuccess") },
            ]),
          );
          return ackResult(request, { status: "accepted" });
        }
        if (request.method === "v4/conversation/resync") {
          notify(
            snapshotFrame("sub-p1", 3, [
              { ...turnHeaderRow(1, commandId, "completedSuccess"), turnId: "native-turn-1" },
              {
                ...assistantTextRow(2, "recovered by resync"),
                turnId: "native-turn-1",
              },
            ]),
          );
          return { result: { ack: { subscriptionId: "sub-p1" } } };
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake, LOCAL_MODEL);

      yield* runtime.startTurn(startTurnInput(providerThread, LOCAL_MODEL));
      const message = yield* takeEvent(
        (
          event,
        ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "message.updated" }> =>
          event.type === "message.updated",
      );
      const terminal = yield* takeEvent(isTurnTerminal);

      assert.equal(message.message.text, "recovered by resync");
      assert.equal(terminal.status, "completed");
      const resyncs = fake
        .requests()
        .filter((request) => request.method === "v4/conversation/resync");
      assert.equal(resyncs.length, 1);
      assert.deepStrictEqual(resyncs[0]?.params, {
        topic: conversationTopic,
        connectionId: `t3-${ZCODE_INSTANCE_ID}`,
        subscriptionId: "sub-p1",
        base: null,
        forceSnapshot: true,
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
