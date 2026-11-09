import { describe } from "@effect/vitest";

import { ZcodeOrchestratorReplayHarness } from "../Adapters/ZcodeAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Zcode replay fixtures", () => {
  itRunsReplayFixtures("zcode", (input) =>
    runReplayFixture({ ...input, harness: ZcodeOrchestratorReplayHarness }),
  );
});
