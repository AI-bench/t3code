import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "../shared.ts";

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";

function field(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null ? Reflect.get(value, key) : undefined;
}

/**
 * After the idle release a fresh `zcode app-server` must re-subscribe to the
 * first session's conversation topic (v4 hydrates the persisted transcript
 * into a snapshot) instead of creating a second session, and the resumed
 * model still sees the first exchange.
 */
export function assertZcodeProviderThreadResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  const processStarts = transcript.entries.filter(
    (entry) => entry.type === "expect_outbound" && field(entry.frame, "type") === "process_start",
  );
  assert.lengthOf(processStarts, 2, "the idle release must retire the first zcode process");

  const commands = transcript.entries.flatMap((entry) => {
    if (entry.type !== "expect_outbound" || field(entry.frame, "method") !== "v4/command") {
      return [];
    }
    return [field(field(entry.frame, "params"), "type")];
  });
  assert.equal(
    commands.filter((type) => type === "createSession").length,
    1,
    "the respawned process must resume the first session, not start another",
  );

  const topics = transcript.entries.flatMap((entry) => {
    if (
      entry.type !== "expect_outbound" ||
      field(entry.frame, "method") !== "v4/conversation/subscribe"
    ) {
      return [];
    }
    return [field(field(entry.frame, "params"), "topic")];
  });
  assert.lengthOf(topics, 2, "each zcode process must subscribe exactly once");
  assert.include(topics[0], "conversation/", "subscribe targets a conversation topic");
  assert.equal(topics[1], topics[0], "the respawned process re-subscribes to the same topic");

  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assert.lengthOf(projection.providerThreads, 1, "resume must keep one provider thread");
  // The respawned process must serve the first turn's transcript from
  // persisted facts: the resume snapshot re-delivers the first answer.
  // (The recorded model saw the history but answered with only the second
  // marker, so the echo itself cannot be asserted.)
  const processTwoFrames = transcript.entries
    .filter((entry) => entry.type === "emit_inbound" && (entry.label ?? "").endsWith("@p2"))
    .map((entry) => JSON.stringify(entry.type === "emit_inbound" ? entry.frame : null))
    .join("");
  assert.include(
    processTwoFrames,
    FIRST_FINAL,
    "the respawned process must hydrate the first turn's transcript",
  );
  const answers = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.include(answers.at(-1), SECOND_FINAL);
}
