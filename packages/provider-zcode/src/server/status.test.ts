import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { ZcodeSettings } from "../settings.ts";
import {
  ZCODE_RUNTIME_MODE_BY_T3_MODE,
  checkZcodeProviderStatus,
  parsePersonalProviderConfig,
} from "./status.ts";
import { parseZcodeLoginPresence, resolveZcodeInstanceStatePaths } from "./instanceState.ts";
import { parseZcodeModelDiscoveryCache } from "./modelDiscoveryCache.ts";

const decodeSettings = Schema.decodeSync(ZcodeSettings);
const encoder = new TextEncoder();

function processHandle(input: { readonly stdout?: string; readonly exitCode?: number }) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_002),
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

const zcodeSpawner = (stdout: string, exitCode = 0) =>
  ChildProcessSpawner.make(() => Effect.succeed(processHandle({ stdout, exitCode })));

const PERSONAL_CONFIG = JSON.stringify({
  schemaVersion: 1,
  config: {
    providerConfigRules: {
      providerRules: [
        {
          providerId: "ollama-local",
          providerName: "Ollama (local)",
          config: { personalModelIds: ["gpt-oss:20b", "qwen3.5:9b"] },
        },
      ],
    },
    defaultModelSelection: { providerId: "ollama-local", modelId: "gpt-oss:20b" },
  },
});

const PLAN_CONFIG = JSON.stringify({
  schemaVersion: 1,
  config: {
    providerConfigRules: { providerRules: [] },
    modelConfigRules: {
      providerModelRules: [
        { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3" },
        { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3-Flash" },
        { providerId: "openrouter", modelId: "not-an-account-model" },
      ],
    },
    defaultModelSelection: { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3" },
  },
});

describe("parsePersonalProviderConfig", () => {
  it("collects models and the default selection", () => {
    const discovered = parsePersonalProviderConfig(PERSONAL_CONFIG);
    assert.strictEqual(discovered.providerCount, 1);
    assert.deepStrictEqual(discovered.modelSlugs, [
      "ollama-local/gpt-oss:20b",
      "ollama-local/qwen3.5:9b",
    ]);
    assert.strictEqual(discovered.defaultModelSlug, "ollama-local/gpt-oss:20b");
  });

  it("collects plan account models and ignores non-account model rules", () => {
    const discovered = parsePersonalProviderConfig(PLAN_CONFIG);
    assert.strictEqual(discovered.providerCount, 0);
    assert.strictEqual(discovered.accountProviderCount, 1);
    assert.deepStrictEqual(discovered.modelSlugs, [
      "account:zai-individual-coding-plan/GLM-5.3",
      "account:zai-individual-coding-plan/GLM-5.3-Flash",
    ]);
    assert.strictEqual(discovered.defaultModelSlug, "account:zai-individual-coding-plan/GLM-5.3");
  });

  it("returns empty for malformed JSON", () => {
    assert.strictEqual(parsePersonalProviderConfig("not json").providerCount, 0);
    assert.strictEqual(parsePersonalProviderConfig("not json").accountProviderCount, 0);
  });
});

describe("parseZcodeLoginPresence", () => {
  it("recognizes the login keys the CLI writes", () => {
    assert.isTrue(
      parseZcodeLoginPresence(JSON.stringify({ "oauth:zai:access_token": "encrypted" })),
    );
    assert.isTrue(parseZcodeLoginPresence(JSON.stringify({ zcodejwttoken: "encrypted" })));
    assert.isTrue(
      parseZcodeLoginPresence(
        JSON.stringify({ "oauth:bigmodel:refresh_token": "encrypted", unrelated: 5 }),
      ),
    );
  });

  it("treats blank values, API-key-only stores, and corrupt files as no login", () => {
    assert.isFalse(parseZcodeLoginPresence(JSON.stringify({ "oauth:zai:access_token": "" })));
    assert.isFalse(parseZcodeLoginPresence(JSON.stringify({ "sk-api-key": "plain" })));
    assert.isFalse(parseZcodeLoginPresence("{}"));
    assert.isFalse(parseZcodeLoginPresence("not json"));
    assert.isFalse(parseZcodeLoginPresence(JSON.stringify(["oauth:zai:access_token"])));
  });
});

const MIXED_CONFIG = JSON.stringify({
  schemaVersion: 1,
  config: {
    providerConfigRules: {
      providerRules: [
        {
          providerId: "ollama-local",
          providerName: "Ollama (local)",
          config: { personalModelIds: ["gpt-oss:20b"] },
        },
      ],
    },
    modelConfigRules: {
      providerModelRules: [
        { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3" },
        { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3-Flash" },
      ],
    },
    defaultModelSelection: { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3" },
  },
});

const DISCOVERY_CACHE = JSON.stringify({
  schemaVersion: 1,
  models: {
    "account:zai-individual-coding-plan/GLM-5.3": {
      levels: ["low", "high", "max"],
      defaultLevel: "max",
      contextWindow: 200_000,
    },
    "ollama-local/gpt-oss:20b": { levels: ["medium", "high"], defaultLevel: "medium" },
  },
});

const GLM_EFFORT_CAPABILITIES = {
  optionDescriptors: [
    {
      id: "effort",
      label: "Effort",
      type: "select",
      options: [
        { id: "low", label: "low" },
        { id: "high", label: "high" },
        { id: "max", label: "max", isDefault: true },
      ],
      currentValue: "max",
    },
  ],
} as const;

describe("parseZcodeModelDiscoveryCache", () => {
  it("parses discovered levels, defaults, and context windows", () => {
    const cache = parseZcodeModelDiscoveryCache(DISCOVERY_CACHE);
    assert.deepStrictEqual(cache["account:zai-individual-coding-plan/GLM-5.3"], {
      levels: ["low", "high", "max"],
      defaultLevel: "max",
      contextWindow: 200_000,
    });
  });

  it("returns empty for corrupt, foreign, or malformed files", () => {
    assert.deepStrictEqual(parseZcodeModelDiscoveryCache("not json"), {});
    assert.deepStrictEqual(
      parseZcodeModelDiscoveryCache(JSON.stringify({ schemaVersion: 99, models: {} })),
      {},
    );
    assert.deepStrictEqual(
      parseZcodeModelDiscoveryCache(
        JSON.stringify({
          schemaVersion: 1,
          models: { "not-a-slug": { defaultLevel: "max" }, "a/b": { nonsense: true } },
        }),
      ),
      {},
    );
  });

  it("trims level strings and drops blank ones, like corrupt-but-parseable files", () => {
    // Choice ids/labels and currentValue are TrimmedNonEmptyString in the
    // contracts; a hand-edited cache must decode like a clean discovery.
    const cache = parseZcodeModelDiscoveryCache(
      JSON.stringify({
        schemaVersion: 1,
        models: {
          "account:p/reasoning": { levels: ["", "  high  ", 5], defaultLevel: "   " },
          "account:p/blank": { levels: ["", "  "], defaultLevel: "" },
        },
      }),
    );
    assert.deepStrictEqual(cache["account:p/reasoning"], { levels: ["high"] });
    assert.strictEqual(cache["account:p/blank"], undefined);
  });
});

describe("ZCODE_RUNTIME_MODE_BY_T3_MODE", () => {
  it("maps only natively enforced zcode modes", () => {
    assert.strictEqual(ZCODE_RUNTIME_MODE_BY_T3_MODE["approval-required"], "build");
    assert.strictEqual(ZCODE_RUNTIME_MODE_BY_T3_MODE["auto-accept-edits"], "edit");
    assert.strictEqual(ZCODE_RUNTIME_MODE_BY_T3_MODE["full-access"], "yolo");
    assert.strictEqual(ZCODE_RUNTIME_MODE_BY_T3_MODE["auto"], undefined);
  });
});

describe("checkZcodeProviderStatus", () => {
  it.effect("reports ready with models from a configured provider", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkZcodeProviderStatus(
        decodeSettings({ enabled: true }),
        { ZCODE_PERSONAL_PROVIDER_CONFIG_FILE: "/tmp/definitely-missing-zcode.json" },
        "/nonexistent-home",
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          zcodeSpawner("zcode 0.17.1\n"),
        ),
        Effect.provide(NodeServices.layer),
      );
      // The binary runs, but the pointed config file is missing: warning posture.
      assert.strictEqual(snapshot.installed, true);
      assert.strictEqual(snapshot.status, "warning");
      assert.strictEqual(snapshot.version, "0.17.1");
    }),
  );

  it.effect("reports unconfigured when the CLI runs but no provider config exists", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkZcodeProviderStatus(
        decodeSettings({ enabled: true }),
        {},
        "/nonexistent-home",
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, zcodeSpawner("", 1)),
        Effect.provide(NodeServices.layer),
      );
      // The binary spawned (exit code ignored): installed, but unconfigured.
      assert.strictEqual(snapshot.installed, true);
      assert.strictEqual(snapshot.status, "warning");
      assert.strictEqual(snapshot.auth.status, "unauthenticated");
    }),
  );

  it.effect("reports authenticated and lists plan models after `zcode login`", () =>
    Effect.gen(function* () {
      const { home } = yield* makeStatusFixture({ config: PLAN_CONFIG, loggedIn: true });
      const snapshot = yield* checkZcodeProviderStatus(
        decodeSettings({ enabled: true }),
        { HOME: home },
        home,
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          zcodeSpawner("zcode 0.17.1\n"),
        ),
      );
      assert.strictEqual(snapshot.status, "ready");
      assert.strictEqual(snapshot.auth.status, "authenticated");
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
        ],
      );
      assert.strictEqual(snapshot.models[0]?.isDefault, true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reports unauthenticated when the account config has no credential-store login", () =>
    Effect.gen(function* () {
      const { home } = yield* makeStatusFixture({ config: PLAN_CONFIG, loggedIn: false });
      const snapshot = yield* checkZcodeProviderStatus(
        decodeSettings({ enabled: true }),
        { HOME: home },
        home,
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          zcodeSpawner("zcode 0.17.1\n"),
        ),
      );
      // Config rules alone never read as signed in: an expired login surfaces
      // as unauthenticated, while the config-derived models stay listed.
      assert.strictEqual(snapshot.auth.status, "unauthenticated");
      assert.strictEqual(snapshot.status, "warning");
      assert.match(snapshot.message ?? "", /zcode login/);
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps API-key setups unauthenticated-looking even with a credential store", () =>
    Effect.gen(function* () {
      const { home } = yield* makeStatusFixture({ config: PERSONAL_CONFIG, loggedIn: true });
      const snapshot = yield* checkZcodeProviderStatus(
        decodeSettings({ enabled: true }),
        { HOME: home },
        home,
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          zcodeSpawner("zcode 0.17.1\n"),
        ),
      );
      // No account rules: a credential file never reads as a plan login.
      assert.strictEqual(snapshot.status, "ready");
      assert.strictEqual(snapshot.auth.status, "unknown");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

// A fake HOME fixture: the personal provider config and the CLI credential
// store in zcode's own layout under home, plus an optional discovery cache in
// the scoped cache path the probe resolves.
const makeStatusFixture = Effect.fnUntraced(function* (input: {
  readonly config: string;
  readonly cacheJson?: string | undefined;
  readonly loggedIn?: boolean | undefined;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "zcode-status-discovery-",
  });
  const home = path.join(dir, "home");
  const zcodeDir = path.join(home, ".zcode", "v2");
  yield* fileSystem.makeDirectory(zcodeDir, { recursive: true });
  yield* fileSystem.writeFileString(path.join(zcodeDir, "provider_config.json"), input.config);
  if (input.loggedIn !== false) {
    yield* fileSystem.writeFileString(
      path.join(zcodeDir, "credentials.json"),
      JSON.stringify({ "oauth:zai:access_token": "encrypted-value" }),
    );
  }
  const cacheDir = path.join(dir, "caches");
  if (input.cacheJson !== undefined) {
    const instanceState = yield* resolveZcodeInstanceStatePaths({ HOME: home }, home);
    yield* fileSystem.makeDirectory(
      path.join(cacheDir, "zcode", instanceState.discoveryCacheScope),
      {
        recursive: true,
      },
    );
    yield* fileSystem.writeFileString(
      path.join(cacheDir, "zcode", instanceState.discoveryCacheScope, "model-discovery.json"),
      input.cacheJson,
    );
  }
  return { home, cacheDir };
});

const statusFromFixture = (input: {
  readonly home: string;
  readonly cacheDir?: string | undefined;
}) =>
  checkZcodeProviderStatus(
    decodeSettings({ enabled: true }),
    { HOME: input.home },
    input.home,
    input.cacheDir === undefined ? {} : { discoveryCacheDir: input.cacheDir },
  ).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, zcodeSpawner("zcode 0.17.1\n")),
  );

describe("checkZcodeProviderStatus with a runtime discovery cache", () => {
  it.effect("keeps the config-derived account listing before any discovery", () =>
    Effect.gen(function* () {
      const { home, cacheDir } = yield* makeStatusFixture({ config: PLAN_CONFIG });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      // Fresh install: no model ever announced itself, so both advertised
      // account models stay listed (the picker is never empty).
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
        ],
      );
      assert.strictEqual(snapshot.models[0]?.capabilities, null);
      assert.strictEqual(snapshot.models[1]?.capabilities, null);
      assert.strictEqual(snapshot.models[0]?.isDefault, true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps undiscovered siblings listed and enriches the announced one", () =>
    Effect.gen(function* () {
      const { home, cacheDir } = yield* makeStatusFixture({
        config: MIXED_CONFIG,
        cacheJson: DISCOVERY_CACHE,
      });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      // GLM-5.3 ran a turn and announced its levels; GLM-5.3-Flash never did
      // and stays listed without capabilities — discovery never removes a
      // config-declared sibling.
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "ollama-local/gpt-oss:20b",
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
        ],
      );
      assert.deepStrictEqual(snapshot.models[0]?.capabilities, {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [
              { id: "medium", label: "medium", isDefault: true },
              { id: "high", label: "high" },
            ],
            currentValue: "medium",
          },
        ],
      } as const);
      const glm = snapshot.models[1];
      assert.strictEqual(glm?.isDefault, true);
      assert.deepStrictEqual(glm?.capabilities, GLM_EFFORT_CAPABILITIES);
      assert.strictEqual(snapshot.models[2]?.capabilities, null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("lists a switched plan's config models despite the old plan's cache entries", () =>
    Effect.gen(function* () {
      // A re-login moved the account from the individual plan to a max plan:
      // the config advertises the new providerId's models, and the old plan's
      // cache entries neither suppress them nor appear as phantoms.
      const switchedConfig = JSON.stringify({
        schemaVersion: 1,
        config: {
          providerConfigRules: { providerRules: [] },
          modelConfigRules: {
            providerModelRules: [
              { providerId: "account:zai-max-plan", modelId: "GLM-5.3" },
              { providerId: "account:zai-max-plan", modelId: "GLM-5.3-Flash" },
            ],
          },
          defaultModelSelection: { providerId: "account:zai-max-plan", modelId: "GLM-5.3" },
        },
      });
      const oldPlanCache = JSON.stringify({
        schemaVersion: 1,
        models: {
          "account:zai-individual-coding-plan/GLM-5.3": { levels: ["low"], defaultLevel: "low" },
        },
      });
      const { home, cacheDir } = yield* makeStatusFixture({
        config: switchedConfig,
        cacheJson: oldPlanCache,
      });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        ["account:zai-max-plan/GLM-5.3", "account:zai-max-plan/GLM-5.3-Flash"],
      );
      assert.strictEqual(snapshot.models[0]?.capabilities, null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("lists every config model once one sibling announces its effort levels", () =>
    Effect.gen(function* () {
      // Both plans advertised; only the individual plan's GLM-5.3 ran a turn.
      // All three config models stay listed: effort info appears once a model
      // announced it, and the unannounced siblings never disappear.
      const bothPlansConfig = JSON.stringify({
        schemaVersion: 1,
        config: {
          providerConfigRules: { providerRules: [] },
          modelConfigRules: {
            providerModelRules: [
              { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3" },
              { providerId: "account:zai-individual-coding-plan", modelId: "GLM-5.3-Flash" },
              { providerId: "account:zai-max-plan", modelId: "GLM-5.3" },
            ],
          },
          defaultModelSelection: { providerId: "account:zai-max-plan", modelId: "GLM-5.3" },
        },
      });
      const partialCache = JSON.stringify({
        schemaVersion: 1,
        models: {
          "account:zai-individual-coding-plan/GLM-5.3": { levels: ["low"], defaultLevel: "low" },
        },
      });
      const { home, cacheDir } = yield* makeStatusFixture({
        config: bothPlansConfig,
        cacheJson: partialCache,
      });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
          "account:zai-max-plan/GLM-5.3",
        ],
      );
      assert.deepStrictEqual(snapshot.models[0]?.capabilities, {
        optionDescriptors: [
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [{ id: "low", label: "low", isDefault: true }],
            currentValue: "low",
          },
        ],
      } as const);
      assert.strictEqual(snapshot.models[1]?.capabilities, null);
      assert.strictEqual(snapshot.models[2]?.capabilities, null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("never lists cache entries the config does not declare", () =>
    Effect.gen(function* () {
      // A stale cache holds a model the current config no longer names:
      // the list source is the config, so it cannot reappear as a phantom.
      const phantomCache = JSON.stringify({
        schemaVersion: 1,
        models: {
          "ollama-local/removed-model": { levels: ["high"], defaultLevel: "high" },
        },
      });
      const { home, cacheDir } = yield* makeStatusFixture({
        config: PLAN_CONFIG,
        cacheJson: phantomCache,
      });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        [
          "account:zai-individual-coding-plan/GLM-5.3",
          "account:zai-individual-coding-plan/GLM-5.3-Flash",
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("falls back to the config-derived listing when the cache is corrupt", () =>
    Effect.gen(function* () {
      const { home, cacheDir } = yield* makeStatusFixture({
        config: PLAN_CONFIG,
        cacheJson: "{ not json",
      });
      const snapshot = yield* statusFromFixture({ home, cacheDir });
      assert.strictEqual(snapshot.models.length, 2);
      assert.strictEqual(snapshot.models[0]?.capabilities, null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
