// ZcodeRpc connection regressions: teardown must still deliver error replies
// for parked reverse requests, and must kill the process group even after the
// main child exited on its own. The peer is a fake ChildProcessSpawner handle,
// as in the adapter-level tests (no real zcode binary).
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { vi } from "vite-plus/test";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { ZCODE_PARK_REVERSE } from "./protocol.ts";
import { makeZcodeRpcConnection } from "./rpc.ts";

/** Deliberately outside the valid pid range so probes and kills hit ESRCH. */
const FAKE_PID = 999_999_982;
const PARK_METHOD = "interaction/requestPermission";

interface FakeZcodePeer {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  /** Write one NDJSON record into the fake child's stdout. */
  readonly writeRecord: (record: unknown) => Effect.Effect<void>;
  /** Every stdin record the connection wrote, in write order. */
  readonly stdinRecords: Queue.Queue<Record<string, unknown>>;
  readonly exit: (code: number) => Effect.Effect<void>;
}

const makeFakeZcodePeer = Effect.fnUntraced(function* (): Effect.fn.Return<
  FakeZcodePeer,
  never,
  never
> {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const stdinRecords = yield* Queue.unbounded<Record<string, unknown>>();
  const exitRef = yield* Deferred.make<number>();
  let buffer = "";
  const handle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(FAKE_PID),
    exitCode: Deferred.await(exitRef) as unknown as Effect.Effect<
      ChildProcessSpawner.ExitCode,
      never
    >,
    isRunning: Effect.succeed(true),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.forEach((chunk: Uint8Array) =>
      Effect.sync(() => {
        buffer += decoder.decode(chunk);
        for (;;) {
          const newline = buffer.indexOf("\n");
          if (newline === -1) return;
          const line = buffer.slice(0, newline).replace(/\r$/u, "");
          buffer = buffer.slice(newline + 1);
          if (line.length > 0) {
            Queue.offerUnsafe(stdinRecords, JSON.parse(line) as Record<string, unknown>);
          }
        }
      }),
    ),
    stdout: Stream.fromQueue(stdout),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
  return {
    spawner: ChildProcessSpawner.make(() => Effect.succeed(handle)),
    writeRecord: (record: unknown) =>
      Queue.offer(stdout, encoder.encode(`${JSON.stringify(record)}\n`)).pipe(Effect.asVoid),
    stdinRecords,
    exit: (code: number) => Deferred.succeed(exitRef, code),
  };
});

const makeConnection = Effect.fnUntraced(function* (peer: FakeZcodePeer) {
  return yield* makeZcodeRpcConnection({
    command: "zcode",
    args: [],
    cwd: undefined,
    env: {},
    reverseRequests: {
      onRequest: (method) =>
        method === PARK_METHOD ? Effect.succeed(ZCODE_PARK_REVERSE) : Effect.undefined,
    },
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, peer.spawner));
});

describe("ZcodeRpc teardown", () => {
  it.effect("sends parked reverse requests an error reply when terminating", () =>
    Effect.gen(function* () {
      const peer = yield* makeFakeZcodePeer();
      const connection = yield* makeConnection(peer);
      // The reverse request parks via ZCODE_PARK_REVERSE; the trailing
      // notification proves the reader finished parking it before teardown.
      yield* peer.writeRecord({ id: "cli-1", method: PARK_METHOD, params: { requestId: "r1" } });
      yield* peer.writeRecord({ method: "state.updated", params: {} });
      const notification = yield* Queue.take(connection.notifications);
      assert.equal(notification.method, "state.updated");

      const terminated = yield* Effect.forkChild(connection.terminate);
      yield* TestClock.adjust(Duration.seconds(2));
      yield* Fiber.join(terminated);
      // The parked request's error reply still reached the peer even though
      // the outgoing queue ends during teardown.
      const reply = yield* Queue.take(peer.stdinRecords);
      assert.deepStrictEqual(reply, {
        id: "cli-1",
        error: { code: -32003, message: "t3 transport stopped" },
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reaps the group once at main exit and not again on scope close", () =>
    Effect.gen(function* () {
      const signals: Array<{ pid: number; signal: string | number | undefined }> = [];
      const kill = vi.spyOn(process, "kill").mockImplementation(((
        pid: number,
        signal?: string | number,
      ) => {
        signals.push({ pid, signal });
      }) as typeof process.kill);
      try {
        const groupKills = () =>
          signals.filter((entry) => entry.pid === -FAKE_PID && entry.signal === "SIGKILL").length;
        const killsAfterExit = yield* Effect.gen(function* () {
          const peer = yield* makeFakeZcodePeer();
          const connection = yield* makeConnection(peer);
          // The main child exits on its own; the exit watcher reaps the
          // group before settling `exited`, and awaiting `exited` proves it.
          yield* peer.exit(0);
          yield* connection.exited;
          return groupKills();
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

        assert.equal(killsAfterExit, 1);
        // A late killpg could hit a recycled pgid, so scope close must not repeat it.
        assert.equal(groupKills(), 1);
      } finally {
        kill.mockRestore();
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
