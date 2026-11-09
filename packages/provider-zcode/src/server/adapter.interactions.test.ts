// Reverse-request lifecycle: a zcode interaction approval must reach the
// orchestrator as a runtime_request event, answer the parked reverse channel
// on resolve, reconcile snapshot pendingInteractions, and fail loudly when
// resolveInteraction cannot be sent.
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
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
const THREAD_ID = ThreadId.make("thread-zcode-interactions-test");
const SESSION_ID = ProviderSessionId.make("provider-session-zcode-interactions-test");
const NATIVE_SESSION_ID = "sess-interactions-1";
const SUBSCRIPTION_ID = "sub-interactions-1";
const CONVERSATION_TOPIC = `conversation/${NATIVE_SESSION_ID}`;
/** Deliberately outside the valid pid range so ZcodeRpc's group kill never lands. */
const FAKE_PID_BASE = 999_999_970;

const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "approval-required",
  interactionMode: "default",
  cwd: null,
});

const modelSelection: ModelSelection = {
  instanceId: ZCODE_INSTANCE_ID,
  model: "ollama-local/gpt-oss:20b",
};

const isRuntimeRequestResponseError = Schema.is(
  ProviderAdapter.ProviderAdapterRuntimeRequestResponseError,
);

// ── Fake app-server with a reverse channel ───────────────────────────────────

interface FakeZcodeRequest {
  readonly id: string | number;
  readonly method: string;
  readonly params: Record<string, unknown>;
}

interface FakeZcodeServer {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly requests: () => ReadonlyArray<FakeZcodeRequest>;
  /** Adapter replies to parked reverse requests, in write order: {id, result}. */
  readonly reverseReplies: () => ReadonlyArray<Record<string, unknown>>;
  /** Write one raw NDJSON record onto the stdout wire (notification or reverse request). */
  readonly sendLine: (record: unknown) => void;
}

const makeFakeZcodeAppServer = (
  script: (request: FakeZcodeRequest) => Record<string, unknown>,
): FakeZcodeServer => {
  const encoder = new TextEncoder();
  const seen: Array<FakeZcodeRequest> = [];
  const replies: Array<Record<string, unknown>> = [];
  let wire: Queue.Queue<Uint8Array, Cause.Done> | null = null;
  let echoOrdinal = 0;
  // The server owns conversation seq: deltas account (seq, seq + 1], snapshots (0, seq].
  let seq = 0;
  const stampSeq = (record: unknown): unknown => {
    const params = (record as { method?: unknown; params?: unknown }).params;
    const frame = (params as { frame?: Record<string, unknown> } | undefined)?.frame;
    if ((record as { method?: unknown }).method !== "v4/conversation/frame" || !frame) {
      return record;
    }
    const payload = frame["payload"] as { kind?: unknown } | undefined;
    const [fromSeq, toSeq] = payload?.kind === "snapshot" ? [0, seq] : [seq, ++seq];
    return {
      ...(record as object),
      params: { ...(params as object), frame: { ...frame, fromSeq, toSeq } },
    };
  };

  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      yield* Effect.addFinalizer(() => Effect.sync(() => Queue.endUnsafe(stdout)));
      wire = stdout;
      const notify = (method: string, params: unknown) => {
        Queue.offerUnsafe(
          stdout,
          encoder.encode(`${JSON.stringify(stampSeq({ method, params }))}\n`),
        );
      };
      // Adapter replies to parked reverse requests carry no method; echo a
      // state patch afterwards so tests get a causally-after pump milestone.
      const notifyEcho = () => {
        echoOrdinal += 1;
        const ordinal = echoOrdinal;
        notify("v4/conversation/frame", {
          wireVersion: 3,
          kind: "complete",
          deliveryKind: "online",
          logicalFrameId: `lf-echo-${ordinal}`,
          logicalFrameOrdinal: ordinal,
          topic: CONVERSATION_TOPIC,
          subscriptionId: SUBSCRIPTION_ID,
          frame: {
            topic: CONVERSATION_TOPIC,
            subscriptionId: SUBSCRIPTION_ID,
            sentAt: 1_000,
            fromSeq: 0,
            toSeq: 0,
            payload: {
              kind: "deltas",
              deltas: [{ op: "state.updated", patch: { revision: 100 + ordinal } }],
            },
          },
        });
      };
      let buffer = "";
      const handleLine = (line: string) => {
        const record = JSON.parse(line) as Record<string, unknown>;
        if (record["method"] === undefined && record["id"] !== undefined) {
          replies.push(record);
          notifyEcho();
          return;
        }
        if (typeof record["id"] !== "string" && typeof record["id"] !== "number") return;
        if (typeof record["method"] !== "string") return;
        const request: FakeZcodeRequest = {
          id: record["id"] as string | number,
          method: record["method"] as string,
          params: (record["params"] ?? {}) as Record<string, unknown>,
        };
        seen.push(request);
        const reply = script(request);
        Queue.offerUnsafe(
          stdout,
          encoder.encode(`${JSON.stringify({ id: request.id, ...reply })}\n`),
        );
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

  return {
    spawner,
    requests: () => [...seen],
    reverseReplies: () => [...replies],
    sendLine: (record) => {
      const output = wire;
      if (output === null) return;
      Queue.offerUnsafe(output, encoder.encode(`${JSON.stringify(stampSeq(record))}\n`));
    },
  } satisfies FakeZcodeServer;
};

// ── Wire shapes ──────────────────────────────────────────────────────────────

const ackResult = (request: FakeZcodeRequest, ack: Record<string, unknown>) => ({
  result: { commandId: request.params["commandId"], revisionAtDecision: 0, ...ack },
});

const requestPermissionParams = (
  requestId: string,
  toolCallId: string,
): Record<string, unknown> => ({
  requestId,
  sessionId: NATIVE_SESSION_ID,
  toolCallId,
  toolName: "Bash",
  input: { command: "pnpm test" },
  reason: "Run tests",
  options: [
    { optionId: "allow_once", kind: "allow_once", name: "Allow once" },
    { optionId: "deny", kind: "deny", name: "Deny" },
  ],
});

const requestUserInputParams = (requestId: string): Record<string, unknown> => ({
  requestId,
  sessionId: NATIVE_SESSION_ID,
  toolCallId: `call-${requestId}`,
  toolName: "AskUserQuestion",
  prompt: "Which database?",
  questions: [
    {
      question: "Which database?",
      header: "Database",
      options: [{ label: "Postgres", value: "Postgres", description: "Relational" }],
    },
  ],
});

const frameNotification = (
  ordinal: number,
  payload: Record<string, unknown>,
): { readonly method: string; readonly params: unknown } => ({
  method: "v4/conversation/frame",
  params: {
    wireVersion: 3,
    kind: "complete",
    deliveryKind: "online",
    logicalFrameId: `lf-${ordinal}`,
    logicalFrameOrdinal: ordinal,
    topic: CONVERSATION_TOPIC,
    subscriptionId: SUBSCRIPTION_ID,
    frame: {
      topic: CONVERSATION_TOPIC,
      subscriptionId: SUBSCRIPTION_ID,
      sentAt: 1_000,
      fromSeq: 0,
      toSeq: 0,
      payload,
    },
  },
});

const snapshotFrame = (ordinal: number, pendingInteractions: ReadonlyArray<unknown>) =>
  frameNotification(ordinal, {
    kind: "snapshot",
    snapshot: {
      sessionId: NATIVE_SESSION_ID,
      logEpoch: "epoch-1",
      seq: ordinal,
      revision: ordinal,
      pendingInteractions,
      backgroundWorks: [],
      rows: { window: [], totalCount: 0, firstRowId: null },
    },
  });

const statePatchFrame = (ordinal: number, revision: number) =>
  frameNotification(ordinal, {
    kind: "deltas",
    deltas: [{ op: "state.updated", patch: { revision } }],
  });

/** Permission interaction exactly as the CLI's v4 projection announces it. */
const permissionInteraction = (interactionId: string): Record<string, unknown> => ({
  interactionId,
  kind: "permission",
  anchorRowId: null,
  payload: {
    kind: "permission",
    toolCallId: `call-${interactionId}`,
    toolName: "Bash",
    summary: "Run tests",
    options: [
      { optionId: "allowOnce", label: "Allow once", kind: "allowOnce" },
      { optionId: "allowAlways", label: "Always allow in this project", kind: "allowAlways" },
      { optionId: "deny", label: "Deny", kind: "deny" },
    ],
  },
});

const sendFrame = (
  fake: FakeZcodeServer,
  frame: { readonly method: string; readonly params: unknown },
) => Effect.sync(() => fake.sendLine(frame));

// ── Runtime plumbing ─────────────────────────────────────────────────────────

const openRuntime = Effect.fnUntraced(function* (fake: FakeZcodeServer) {
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
    modelSelection,
    runtimePolicy,
  });
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection,
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

const isRuntimeRequestEvent = (
  event: ProviderAdapter.ProviderAdapterV2Event,
): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "runtime_request.updated" }> =>
  event.type === "runtime_request.updated";

const isThreadEvent = (
  event: ProviderAdapter.ProviderAdapterV2Event,
): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "provider_thread.updated" }> =>
  event.type === "provider_thread.updated";

const isAnyEvent = (
  event: ProviderAdapter.ProviderAdapterV2Event,
): event is ProviderAdapter.ProviderAdapterV2Event => Boolean(event);

const resolveInteractionPayloads = (fake: FakeZcodeServer) =>
  fake
    .requests()
    .filter(
      (request) =>
        request.method === "v4/command" && request.params["type"] === "resolveInteraction",
    )
    .map((request) => request.params["payload"]);

// ── Scenarios ────────────────────────────────────────────────────────────────

describe("ZcodeAdapterV2 reverse interactions", () => {
  it.effect("emits the pending request, dedupes re-announces, and answers the parked channel", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);

      fake.sendLine({
        id: "server-1",
        method: "interaction/requestPermission",
        params: requestPermissionParams("req-1", "call-1"),
      });
      const pending = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(pending.runtimeRequest.nativeRequestRef?.nativeId, "req-1");
      assert.equal(pending.runtimeRequest.kind, "permission");
      assert.equal(pending.runtimeRequest.status, "pending");

      // zcode re-announces pending interactions ~1/s under a fresh protocol id.
      fake.sendLine({
        id: "server-2",
        method: "interaction/requestPermission",
        params: requestPermissionParams("req-1", "call-1"),
      });

      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        decision: "accept",
      });

      // Drain: the resolved event for req-1, then both parked-channel echoes.
      let resolvedSeen = false;
      let echoes = 0;
      let extraPending = 0;
      while (!resolvedSeen || echoes < 2) {
        const event = yield* takeEvent(isAnyEvent);
        if (
          isRuntimeRequestEvent(event) &&
          event.runtimeRequest.nativeRequestRef?.nativeId === "req-1"
        ) {
          if (event.runtimeRequest.status === "pending") extraPending += 1;
          if (event.runtimeRequest.status === "resolved") resolvedSeen = true;
        }
        if (isThreadEvent(event)) echoes += 1;
      }
      // The re-announce must not have produced a second pending request.
      assert.equal(extraPending, 0);

      assert.deepStrictEqual(resolveInteractionPayloads(fake), [
        { interactionId: "req-1", answer: { optionId: "allowOnce" } },
      ]);
      // Both parked incarnations answered with the legacy permission response.
      assert.deepStrictEqual(fake.reverseReplies(), [
        { id: "server-1", result: { decision: "allow" } },
        { id: "server-2", result: { decision: "allow" } },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("projects snapshot pendingInteractions and answers through the v4 option ids", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);

      yield* sendFrame(fake, snapshotFrame(1, [permissionInteraction("req-snap")]));
      const pending = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(pending.runtimeRequest.nativeRequestRef?.nativeId, "req-snap");
      assert.equal(pending.runtimeRequest.kind, "permission");

      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        decision: "acceptAlways",
      });

      const resolved = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(resolved.runtimeRequest.status, "resolved");
      assert.equal(resolved.runtimeRequest.decision, "acceptAlways");
      // The v4 projection's allowAlways optionId reached resolveInteraction.
      assert.deepStrictEqual(resolveInteractionPayloads(fake), [
        { interactionId: "req-snap", answer: { optionId: "allowAlways" } },
      ]);
      // No reverse channel was parked for a snapshot-born interaction.
      assert.deepStrictEqual(fake.reverseReplies(), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "reconciles a snapshot that carries the reverse-born interaction id without doubling",
    () =>
      Effect.gen(function* () {
        const fake = makeFakeZcodeAppServer((request) => {
          if (request.method === "v4/command" && request.params["type"] === "createSession") {
            return ackResult(request, {
              status: "accepted",
              result: { sessionId: NATIVE_SESSION_ID },
            });
          }
          if (request.method === "v4/conversation/subscribe") {
            return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
          }
          if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
            return ackResult(request, { status: "accepted" });
          }
          return { error: { code: -32_601, message: `unexpected ${request.method}` } };
        });
        const { runtime, takeEvent } = yield* openRuntime(fake);

        fake.sendLine({
          id: "server-1",
          method: "interaction/requestPermission",
          params: requestPermissionParams("req-1", "call-1"),
        });
        const pending = yield* takeEvent(isRuntimeRequestEvent);
        assert.equal(pending.runtimeRequest.nativeRequestRef?.nativeId, "req-1");

        // The snapshot announces the same business requestId: no second event,
        // and the original request id stays resolvable.
        yield* sendFrame(fake, snapshotFrame(1, [permissionInteraction("req-1")]));
        yield* takeEvent(isThreadEvent);

        yield* runtime.respondToRuntimeRequest({
          requestId: pending.runtimeRequest.id,
          decision: "accept",
        });
        const resolved = yield* takeEvent(isRuntimeRequestEvent);
        assert.equal(resolved.runtimeRequest.status, "resolved");
        assert.equal(resolved.runtimeRequest.id, pending.runtimeRequest.id);
        assert.deepStrictEqual(resolveInteractionPayloads(fake), [
          { interactionId: "req-1", answer: { optionId: "allowOnce" } },
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a reverse-born interaction pending while the snapshot has not caught up", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);

      fake.sendLine({
        id: "server-1",
        method: "interaction/requestPermission",
        params: requestPermissionParams("req-1", "call-1"),
      });
      const pending = yield* takeEvent(isRuntimeRequestEvent);

      // A subscribe-time snapshot that predates the permission event must not
      // resolve the reverse-born interaction out from under the user.
      yield* sendFrame(fake, snapshotFrame(1, []));
      yield* sendFrame(fake, statePatchFrame(2, 2));
      yield* takeEvent(isThreadEvent);
      yield* takeEvent(isThreadEvent);

      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        decision: "accept",
      });
      const resolved = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(resolved.runtimeRequest.status, "resolved");
      assert.deepStrictEqual(resolveInteractionPayloads(fake), [
        { interactionId: "req-1", answer: { optionId: "allowOnce" } },
      ]);
      yield* takeEvent(isThreadEvent);
      assert.deepStrictEqual(fake.reverseReplies(), [
        { id: "server-1", result: { decision: "allow" } },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resolves externally settled interactions and drains their parked channel", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { takeEvent } = yield* openRuntime(fake);

      fake.sendLine({
        id: "server-1",
        method: "interaction/requestPermission",
        params: requestPermissionParams("req-1", "call-1"),
      });
      yield* takeEvent(isRuntimeRequestEvent);
      yield* sendFrame(fake, snapshotFrame(1, [permissionInteraction("req-1")]));
      yield* takeEvent(isThreadEvent);

      // The interaction vanishes from the snapshot: settled on zcode's side.
      yield* sendFrame(fake, snapshotFrame(2, []));
      const resolved = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(resolved.runtimeRequest.nativeRequestRef?.nativeId, "req-1");
      assert.equal(resolved.runtimeRequest.status, "resolved");

      // One thread event for the vanishing snapshot, one for the reply echo.
      yield* takeEvent(isThreadEvent);
      yield* takeEvent(isThreadEvent);
      assert.deepStrictEqual(resolveInteractionPayloads(fake), []);
      assert.deepStrictEqual(fake.reverseReplies(), [
        { id: "server-1", result: { decision: "deny" } },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("user input requests resolve with an accept action carrying the answers", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return ackResult(request, { status: "accepted" });
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);

      fake.sendLine({
        id: "server-1",
        method: "interaction/requestUserInput",
        params: requestUserInputParams("q-1"),
      });
      const pending = yield* takeEvent(isRuntimeRequestEvent);
      assert.equal(pending.runtimeRequest.nativeRequestRef?.nativeId, "q-1");
      assert.equal(pending.runtimeRequest.kind, "user_input");

      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        answers: { "Which database?": "Postgres" },
      });

      let echoes = 0;
      while (echoes < 1) {
        const event = yield* takeEvent(isAnyEvent);
        if (isThreadEvent(event)) echoes += 1;
      }
      assert.deepStrictEqual(resolveInteractionPayloads(fake), [
        {
          interactionId: "q-1",
          answer: { action: "accept", content: { answers: { "Which database?": "Postgres" } } },
        },
      ]);
      assert.deepStrictEqual(fake.reverseReplies(), [
        {
          id: "server-1",
          result: { action: "accept", content: { answers: { "Which database?": "Postgres" } } },
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("surfaces resolveInteraction failures and keeps the request pending", () =>
    Effect.gen(function* () {
      const fake = makeFakeZcodeAppServer((request) => {
        if (request.method === "v4/command" && request.params["type"] === "createSession") {
          return ackResult(request, {
            status: "accepted",
            result: { sessionId: NATIVE_SESSION_ID },
          });
        }
        if (request.method === "v4/conversation/subscribe") {
          return { result: { ack: { subscriptionId: SUBSCRIPTION_ID } } };
        }
        if (request.method === "v4/command" && request.params["type"] === "resolveInteraction") {
          return { error: { code: -32_000, message: "session is closing" } };
        }
        return { error: { code: -32_601, message: `unexpected ${request.method}` } };
      });
      const { runtime, providerThread, takeEvent } = yield* openRuntime(fake);

      fake.sendLine({
        id: "server-1",
        method: "interaction/requestPermission",
        params: requestPermissionParams("req-1", "call-1"),
      });
      const pending = yield* takeEvent(isRuntimeRequestEvent);

      const failure = yield* runtime
        .respondToRuntimeRequest({ requestId: pending.runtimeRequest.id, decision: "accept" })
        .pipe(Effect.flip);
      assert.isTrue(isRuntimeRequestResponseError(failure));

      // The command was attempted; the failure is surfaced, nothing was
      // reported resolved, and the parked channel was left untouched.
      assert.equal(resolveInteractionPayloads(fake).length, 1);
      assert.deepStrictEqual(fake.reverseReplies(), []);
      const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(snapshot.runtimeRequests.length, 1);
      assert.equal(snapshot.runtimeRequests[0]?.status, "pending");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
