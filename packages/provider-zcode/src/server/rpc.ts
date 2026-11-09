// ZCode app-server speaks LF-delimited NDJSON JSON-RPC over stdio.
// Framing, teardown, and logging discipline follow the Pi RPC transport:
// lengths only in logs, oversized records dropped, SIGTERM→SIGKILL escalation.
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  MAX_TOOL_OUTPUT_IMAGES,
  MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH,
} from "@t3tools/shared/toolOutput";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";

import {
  ZCODE_PARK_REVERSE,
  ZCODE_PROVIDER_HEADERS_DECLINE,
  ZCODE_REVERSE_METHODS,
  ZCODE_RUNTIME_PREFERENCES_RESPONSE,
  type ZcodeRecord,
} from "./protocol.ts";

export class ZcodeRpcError extends Schema.TaggedError<ZcodeRpcError>()("ZcodeRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  code: Schema.optional(Schema.Number),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `ZCode RPC ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export class ZcodeRpcTimeoutError extends Schema.TaggedError<ZcodeRpcTimeoutError>()(
  "ZcodeRpcTimeoutError",
  {
    operation: Schema.String,
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  },
) {
  override get message(): string {
    return `ZCode RPC ${this.operation} failed: timed out after ${this.timeoutMs}ms.`;
  }
}

export interface ZcodeRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
  /**
   * Server→client requests. Answered inline; default answers cover the
   * requests the CLI hard-requires (runtime preferences, provider headers).
   */
  readonly reverseRequests?: {
    /**
     * The third argument is the protocol request id: a handler that parks via
     * ZCODE_PARK_REVERSE needs it later for respondReverse.
     */
    readonly onRequest?: (
      method: string,
      params: unknown,
      requestId: string | number,
    ) => Effect.Effect<unknown, ZcodeRpcError>;
  };
}

export interface ZcodeRpcNotification {
  readonly method: string;
  readonly params: unknown;
}

export interface ZcodeRpcConnection {
  readonly send: (record: ZcodeRecord) => Effect.Effect<void, ZcodeRpcError>;
  readonly request: (
    method: string,
    params: unknown,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, ZcodeRpcError | ZcodeRpcTimeoutError>;
  /** Notifications from the CLI, in arrival order (includes frames). */
  readonly notifications: Queue.Queue<ZcodeRpcNotification, ZcodeRpcError>;
  /** Answer a parked reverse request (parked via ZCODE_PARK_REVERSE). */
  readonly respondReverse: (
    id: string | number,
    result: unknown,
  ) => Effect.Effect<void, ZcodeRpcError>;
  readonly exited: Effect.Effect<number, ZcodeRpcError>;
  readonly terminate: Effect.Effect<void>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const TERMINATION_GRACE = Duration.seconds(1);

interface PendingZcodeRequest {
  readonly deferred: Deferred.Deferred<unknown, ZcodeRpcError>;
  readonly method: string;
}

// MCP duplicates images in model content and CallToolResult; budget both plus bounded overhead.
const MAX_ZCODE_RECORD_CHARS =
  2 * MAX_TOOL_OUTPUT_IMAGES * MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH + 8 * 1024 * 1024;

function makeJsonlFramer() {
  let buffer = "";
  let dropping = false;
  return (chunk: string) => {
    const lines: string[] = [];
    const droppedLengths: number[] = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      const end = newline < 0 ? chunk.length : newline;
      if (!dropping) {
        if (buffer.length + end - start > MAX_ZCODE_RECORD_CHARS) {
          droppedLengths.push(buffer.length + end - start);
          buffer = "";
          dropping = true;
        } else {
          buffer += chunk.slice(start, end);
        }
      }
      if (newline < 0) break;
      if (!dropping && buffer.length > 0) {
        lines.push(buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer);
      }
      buffer = "";
      dropping = false;
      start = newline + 1;
    }
    return { lines, droppedLengths };
  };
}

const logDroppedZcodeRecords = (droppedLengths: ReadonlyArray<number>) =>
  Effect.forEach(
    droppedLengths,
    (recordLength) =>
      Effect.logWarning("Dropping oversized zcode stdout record.", {
        recordLength,
        maxRecordLength: MAX_ZCODE_RECORD_CHARS,
      }),
    { discard: true },
  );

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonLine = Schema.decodeSync(UnknownFromJsonString);
const encodeJsonLine = Schema.encodeSync(UnknownFromJsonString);

const ZCODE_ERROR_DETAIL_MAX_CHARS = 200;

function summarizeZcodeError(error: unknown): string {
  const text = typeof error === "string" ? error : JSON.stringify(error);
  if (text === undefined) return "unknown error";
  return text.length > ZCODE_ERROR_DETAIL_MAX_CHARS
    ? `${text.slice(0, ZCODE_ERROR_DETAIL_MAX_CHARS)}…`
    : text;
}

function describeZcodeStdoutClosure(exitCode: number | undefined): string {
  if (exitCode === undefined || !Number.isFinite(exitCode)) {
    return "zcode process closed stdout";
  }
  return `zcode process exited with code ${exitCode}`;
}

function parseZcodeRecord(line: string): ZcodeRecord | undefined {
  try {
    const parsed: unknown = decodeJsonLine(line);
    return Predicate.isObject(parsed) ? (parsed as ZcodeRecord) : undefined;
  } catch {
    return undefined;
  }
}

function isResponse(record: ZcodeRecord): boolean {
  return (
    (record["result"] !== undefined || record["error"] !== undefined) &&
    record["method"] === undefined
  );
}

function reverseRequestDefaultResult(method: string): unknown {
  switch (method) {
    case ZCODE_REVERSE_METHODS.requestRuntimePreferences:
      return ZCODE_RUNTIME_PREFERENCES_RESPONSE;
    case ZCODE_REVERSE_METHODS.requestProviderRuntimeHeaders:
      return ZCODE_PROVIDER_HEADERS_DECLINE;
    default:
      return {};
  }
}

/**
 * `hasExited` is checked before each signal because a gone group's pgid can be
 * recycled by an unrelated process; the exit watcher reaps surviving members
 * at exit time.
 */
const terminateZcodeProcess = Effect.fn("ZcodeRpc.terminateZcodeProcess")(function* (
  kill: (signal: NodeJS.Signals) => boolean,
  hasExited: () => boolean,
) {
  if (hasExited()) return;
  if (!kill("SIGTERM")) return;
  yield* Effect.sleep(TERMINATION_GRACE);
  if (hasExited()) return;
  kill("SIGKILL");
});

export const makeZcodeRpcConnection = Effect.fnUntraced(function* (options: ZcodeRpcSpawnOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const scope = yield* Effect.scope;

  const spawnCommand = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new ZcodeRpcError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawnCommand.shell,
        detached: platform !== "win32",
        // stdin EOF is a hard stop boundary for the CLI; keep it open until teardown.
        stdin: { stream: "pipe", endOnDone: false },
      }),
    )
    .pipe(Effect.mapError((cause) => new ZcodeRpcError({ operation: "spawn", cause })));

  let childExited = false;
  let diagnosingStdoutClose = false;

  const killProcessGroup = (signal: NodeJS.Signals): boolean => {
    try {
      if (platform === "win32") {
        process.kill(Number(child.pid), signal);
      } else {
        signalProcessGroup(Number(child.pid), signal);
      }
      return true;
    } catch {
      return false;
    }
  };

  const hasExited = (): boolean => {
    if (childExited) return true;
    try {
      if (platform === "win32") process.kill(Number(child.pid), 0);
      else signalProcessGroup(Number(child.pid), 0);
      return false;
    } catch {
      return true;
    }
  };

  // Windows process.kill leaves extension children and pipes alive; taskkill /T reaps the tree.
  const terminateWindowsTree = Effect.gen(function* () {
    if (hasExited()) return;
    const taskkill = yield* spawner.spawn(
      ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
    );
    yield* taskkill.exitCode;
  }).pipe(Effect.scoped, Effect.ignore);

  const terminateProcess =
    platform === "win32"
      ? terminateWindowsTree
      : terminateZcodeProcess(killProcessGroup, hasExited);

  // Register teardown immediately after spawn so interrupted setup cannot leak a detached process.
  yield* Scope.addFinalizer(scope, terminateProcess.pipe(Effect.ignore, Effect.uninterruptible));

  const pendingRequests = new Map<string, PendingZcodeRequest>();
  const parkedReverse = new Map<string | number, string>();
  const notifications = yield* Queue.unbounded<ZcodeRpcNotification, ZcodeRpcError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, ZcodeRpcError>();
  const transportDown = yield* Deferred.make<never, ZcodeRpcError>();
  const exitDeferred = yield* Deferred.make<number, ZcodeRpcError>();
  let nextRequestId = 0;

  const failTransport = Effect.fn("ZcodeRpc.failTransport")(function* (error: ZcodeRpcError) {
    const claimed = yield* Deferred.fail(transportDown, error);
    if (!claimed) return;
    for (const [key, pending] of pendingRequests) {
      pendingRequests.delete(key);
      yield* Deferred.fail(pending.deferred, error);
    }
    yield* Queue.fail(outgoing, error);
    yield* Queue.fail(notifications, error);
  });

  const sendRecord = Effect.fnUntraced(function* (
    record: ZcodeRecord,
  ): Effect.fn.Return<void, ZcodeRpcError> {
    const accepted = yield* Queue.offer(
      outgoing,
      new TextEncoder().encode(`${encodeJsonLine(record)}\n`),
    );
    if (!accepted) {
      return yield* Deferred.await(transportDown);
    }
  });

  const routeRecord =
    /* Protocol records and writes include streaming deltas; avoid a span per record. */
    Effect.fnUntraced(function* (record: ZcodeRecord) {
      if (isResponse(record)) {
        const id =
          typeof record["id"] === "string" || typeof record["id"] === "number"
            ? String(record["id"])
            : undefined;
        if (id !== undefined) {
          const pending = pendingRequests.get(id);
          if (pending !== undefined) {
            pendingRequests.delete(id);
            const error = record["error"];
            if (Predicate.isObject(error)) {
              yield* Deferred.fail(
                pending.deferred,
                new ZcodeRpcError({
                  operation: pending.method,
                  detail: summarizeZcodeError(error),
                  ...(typeof (error as ZcodeRecord)["code"] === "number"
                    ? { code: (error as ZcodeRecord)["code"] as number }
                    : {}),
                }),
              );
            } else {
              yield* Deferred.succeed(pending.deferred, record["result"]);
            }
            return;
          }
        }
        // Response to a reverse request we originated is impossible here; drop.
        if (record["id"] !== undefined) return;
        yield* Queue.offer(notifications, { method: "", params: record });
        return;
      }
      const method =
        typeof record["method"] === "string" ? (record["method"] as string) : undefined;
      if (method === undefined) return;
      if (record["id"] !== undefined) {
        // Reverse request: CLI → controller. Answer with the handler result or default.
        const id = record["id"];
        const idKey = typeof id === "string" || typeof id === "number" ? id : String(id);
        const params = record["params"];
        const respond = (result: unknown) =>
          sendRecord({ id, result }).pipe(
            Effect.tap(() => Effect.sync(() => parkedReverse.delete(idKey))),
            Effect.catch(() => Effect.sync(() => parkedReverse.delete(idKey))),
          );
        const respondError = (code: number, message: string) =>
          sendRecord({ id, error: { code, message } }).pipe(
            Effect.tap(() => Effect.sync(() => parkedReverse.delete(idKey))),
            Effect.catch(() => Effect.sync(() => parkedReverse.delete(idKey))),
          );
        const handled = yield* (
          options.reverseRequests?.onRequest?.(method, params, idKey) ??
          Effect.succeed(reverseRequestDefaultResult(method))
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ZcodeRpcError({
                operation: `reverse:${method}`,
                detail: summarizeZcodeError(cause),
              }),
          ),
          Effect.result,
        );
        if (Result.isFailure(handled)) {
          yield* respondError(-32003, `t3 failed to answer ${method}`);
          return;
        }
        if (handled.success === ZCODE_PARK_REVERSE) {
          parkedReverse.set(idKey, method);
          return;
        }
        yield* respond(handled.success ?? reverseRequestDefaultResult(method));
        return;
      }
      yield* Queue.offer(notifications, { method, params: record["params"] });
    });

  // Watch exit before reading stdout so immediate crashes populate exitDeferred for diagnosis.
  yield* child.exitCode.pipe(
    // The group leader is gone but detached members survive it; kill before
    // settling the exit deferred so awaiting `exited` proves the reap ran.
    Effect.andThen(Effect.sync(() => killProcessGroup("SIGKILL"))),
    Effect.matchEffect({
      onFailure: (cause) =>
        Deferred.fail(exitDeferred, new ZcodeRpcError({ operation: "exit", cause })),
      onSuccess: (code) =>
        Effect.suspend(() => {
          childExited = true;
          return Deferred.succeed(exitDeferred, Number(code));
        }),
    }),
    Effect.forkIn(scope),
  );

  yield* Effect.gen(function* () {
    const frame = makeJsonlFramer();
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          const { lines, droppedLengths } = frame(chunk);
          yield* logDroppedZcodeRecords(droppedLengths);
          for (const line of lines) {
            const record = parseZcodeRecord(line);
            if (record === undefined) {
              yield* Effect.logDebug("Dropping non-JSON zcode stdout line.", {
                lineLength: line.length,
              });
              continue;
            }
            yield* routeRecord(record);
          }
        }),
      ),
    );
    const flushed = frame("\n");
    yield* logDroppedZcodeRecords(flushed.droppedLengths);
    for (const line of flushed.lines) {
      const trailing = parseZcodeRecord(line);
      if (trailing !== undefined) yield* routeRecord(trailing);
    }
  }).pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) => failTransport(new ZcodeRpcError({ operation: "read", cause })),
      onSuccess: Effect.fnUntraced(function* () {
        // Claim stdout-close diagnosis before waiting so broken stdin cannot replace the exit error.
        diagnosingStdoutClose = true;
        const polled = yield* Deferred.poll(exitDeferred);
        const maybeExitCode = Option.isSome(polled)
          ? yield* polled.value.pipe(
              Effect.asSome,
              Effect.orElseSucceed(() => Option.none<number>()),
            )
          : yield* Deferred.await(exitDeferred).pipe(
              Effect.timeoutOption(Duration.millis(250)),
              // Stdout-close waits need wall time; TestClock would strand Stop-with-restart without an exit.
              Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
              Effect.orElseSucceed(() => Option.none<number>()),
            );
        return yield* failTransport(
          new ZcodeRpcError({
            operation: "read",
            detail: describeZcodeStdoutClosure(
              Option.isSome(maybeExitCode) ? maybeExitCode.value : undefined,
            ),
          }),
        );
      }),
    }),
    Effect.forkIn(scope),
  );

  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      chunk.trim().length === 0
        ? Effect.void
        : // Log stderr length only; unbounded text may contain credentials or prompts.
          Effect.logDebug("zcode stderr", { stderrLength: chunk.length }),
    ),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  // Start the reader first so stdout-close diagnosis can win over broken-pipe stdin.
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) =>
      diagnosingStdoutClose
        ? Effect.void
        : failTransport(new ZcodeRpcError({ operation: "write", cause })),
    ),
    Effect.forkIn(scope),
  );

  const request = Effect.fn("ZcodeRpc.request")(function* (
    method: string,
    params: unknown,
    timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  ): Effect.fn.Return<unknown, ZcodeRpcError | ZcodeRpcTimeoutError> {
    const id = `t3-${nextRequestId++}`;
    const deferred = yield* Deferred.make<unknown, ZcodeRpcError>();
    pendingRequests.set(id, { deferred, method });
    yield* sendRecord({ id, method, params }).pipe(
      Effect.tapError(() => Effect.sync(() => pendingRequests.delete(id))),
    );
    return yield* Effect.raceFirst(Deferred.await(deferred), Deferred.await(transportDown)).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(timeoutMs),
        orElse: () =>
          Effect.fail(
            new ZcodeRpcTimeoutError({
              operation: method,
              timeoutMs,
            }),
          ),
      }),
      Effect.onInterrupt(() => Effect.sync(() => pendingRequests.delete(id))),
      Effect.onError(() => Effect.sync(() => pendingRequests.delete(id))),
    );
  });

  const respondReverseRecord = (id: string | number, result: unknown) =>
    Effect.suspend(() => {
      if (!parkedReverse.has(id)) return Effect.void;
      return sendRecord({ id, result }).pipe(
        Effect.tap(() => Effect.sync(() => parkedReverse.delete(id))),
        Effect.asVoid,
        Effect.catch(() => Effect.sync(() => parkedReverse.delete(id)).pipe(Effect.asVoid)),
      );
    });

  return {
    send: sendRecord,
    request,
    notifications,
    respondReverse: respondReverseRecord,
    exited: Deferred.await(exitDeferred),
    // Parked replies must be offered before the transport fails: offers to an
    // ended queue are refused, so failing first silently dropped them. The
    // parked set is snapshotted per run, so terminate must suspend.
    terminate: Effect.suspend(() =>
      Effect.forEach(
        [...parkedReverse.keys()],
        (id) =>
          sendRecord({ id, error: { code: -32003, message: "t3 transport stopped" } }).pipe(
            Effect.ignore,
          ),
        { discard: true },
      ).pipe(
        Effect.andThen(() =>
          failTransport(
            new ZcodeRpcError({ operation: "terminate", detail: "zcode process was stopped" }),
          ),
        ),
        Effect.andThen(terminateProcess),
        Effect.ignore,
        Effect.uninterruptible,
      ),
    ),
  } satisfies ZcodeRpcConnection;
});
