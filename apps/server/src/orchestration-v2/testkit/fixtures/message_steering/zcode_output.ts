import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertInStreamMessageSteeringOutput } from "./codex_output.ts";

/** zcode steers in-stream like codex: one run, one provider turn, no restart. */
export function assertZcodeMessageSteeringOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertInStreamMessageSteeringOutput(result, transcript);
}
