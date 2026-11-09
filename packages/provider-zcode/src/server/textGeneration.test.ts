import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { ZcodeSettings } from "../settings.ts";
import { makeZcodeTextGeneration } from "./textGeneration.ts";

const decodeSettings = Schema.decodeSync(ZcodeSettings);
const encoder = new TextEncoder();

function processHandle(input: { readonly stdout?: string; readonly exitCode?: number }) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_003),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: bytes(input.stdout),
    stderr: bytes(undefined),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

const recordingSpawner = (stdout: string, spawnCalls: Array<ChildProcess.Command>, exitCode = 0) =>
  ChildProcessSpawner.make((command) => {
    spawnCalls.push(command);
    return Effect.succeed(processHandle({ stdout, exitCode }));
  });

describe("ZcodeTextGeneration", () => {
  it.effect("runs one-shot prompts in plan mode and decodes the reply", () =>
    Effect.gen(function* () {
      const spawnCalls: Array<ChildProcess.Command> = [];
      const stdout = JSON.stringify({
        response: JSON.stringify({ title: "Pin one-shot permission mode" }),
      });
      const generation = yield* makeZcodeTextGeneration(decodeSettings({ enabled: true }), {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          recordingSpawner(stdout, spawnCalls),
        ),
        Effect.provide(NodeServices.layer),
      );

      const generated = yield* generation.generateThreadTitle({
        cwd: "/repo",
        message: "Summarize this thread.",
        modelSelection: {
          instanceId: ProviderInstanceId.make("zcode"),
          model: "default",
        },
      });

      assert.strictEqual(generated.title, "Pin one-shot permission mode");
      assert.lengthOf(spawnCalls, 1);
      const spawn = spawnCalls[0];
      assert.strictEqual(spawn?._tag, "StandardCommand");
      const args = spawn?._tag === "StandardCommand" ? [...spawn.args].map(String) : [];
      assert.strictEqual(args[0], "--prompt");
      assert.deepStrictEqual(args.slice(2), [
        "--cwd",
        "/repo",
        "--mode",
        "plan",
        "--output-format",
        "json",
        "--no-color",
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails with TextGenerationError when the one-shot exits nonzero", () =>
    Effect.gen(function* () {
      const spawnCalls: Array<ChildProcess.Command> = [];
      const generation = yield* makeZcodeTextGeneration(decodeSettings({ enabled: true }), {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          recordingSpawner("", spawnCalls, 1),
        ),
        Effect.provide(NodeServices.layer),
      );

      const failure = yield* Effect.flip(
        generation.generateThreadTitle({
          cwd: "/repo",
          message: "Summarize this thread.",
          modelSelection: {
            instanceId: ProviderInstanceId.make("zcode"),
            model: "default",
          },
        }),
      );

      assert.strictEqual(failure._tag, "TextGenerationError");
      assert.include(failure.detail, "exited with code 1");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
