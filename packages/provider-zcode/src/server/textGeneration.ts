// Text generation via zcode's one-shot headless mode:
// `zcode --prompt <text> --cwd <dir> --mode plan --output-format json`.
// Plan mode denies write tools without a prompt, so the one-shot cannot
// block or mutate the checkout. No --model flag: zcode's default selection applies.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { TextGenerationError } from "@t3tools/contracts";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";
import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import type { ZcodeSettings } from "../settings.ts";
import { zcodeLaunchEnvironment } from "./adapter.ts";

const ZCODE_ONESHOT_TIMEOUT_MS = 180_000;

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonText = Schema.decodeUnknownOption(UnknownFromJsonString);

const oneshotSummaryText = (summary: unknown): string | null => {
  if (typeof summary !== "object" || summary === null) return null;
  const response = (summary as { response?: unknown }).response;
  if (typeof response === "string") return response;
  if (typeof response === "object" && response !== null) {
    const text = (response as { text?: unknown }).text;
    if (typeof text === "string") return text;
  }
  return null;
};

export const makeZcodeTextGeneration = Effect.fn("makeZcodeTextGeneration")(function* (
  settings: ZcodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const env = zcodeLaunchEnvironment(environment);

  const runOneShot: TextGenerationOperations.Runner = Effect.fn("ZcodeTextGeneration.runOneShot")(
    function* (request) {
      const binaryPath = settings.binaryPath?.trim() || "zcode";
      const args = [
        "--prompt",
        request.prompt,
        "--cwd",
        request.cwd,
        "--mode",
        "plan",
        "--output-format",
        "json",
        "--no-color",
      ];
      const command = yield* resolveSpawnCommand(binaryPath, args, { env });
      const result = yield* spawnAndCollect(
        binaryPath,
        ChildProcess.make(command.command, [...command.args], {
          cwd: request.cwd,
          env,
          extendEnv: false,
          shell: command.shell,
          stdin: "ignore",
        }),
      ).pipe(
        Effect.timeout(ZCODE_ONESHOT_TIMEOUT_MS),
        Effect.mapError(
          () =>
            new TextGenerationError({
              operation: request.operation,
              detail: "zcode one-shot prompt failed or timed out.",
            }),
        ),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      if (result.code !== 0) {
        return yield* new TextGenerationError({
          operation: request.operation,
          detail: `zcode one-shot prompt exited with code ${result.code}.`,
        });
      }
      const summary = decodeJsonText(result.stdout);
      if (Option.isNone(summary)) {
        return yield* new TextGenerationError({
          operation: request.operation,
          detail: "zcode one-shot prompt returned non-JSON output.",
        });
      }
      const text = oneshotSummaryText(summary.value);
      if (text === null) {
        return yield* new TextGenerationError({
          operation: request.operation,
          detail: "zcode one-shot prompt returned no response text.",
        });
      }
      return yield* TextGenerationOperations.decodeJsonReply(
        { operation: request.operation, outputSchema: request.outputSchema },
        "zcode",
        text,
      );
    },
  );

  return TextGenerationOperations.fromRunner(
    "zcode",
    TextGenerationOperations.withoutInvestigation("zcode", runOneShot),
  );
});
