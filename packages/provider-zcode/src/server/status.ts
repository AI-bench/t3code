// Status probe: version via `zcode --version`; models from the personal
// provider config, sign-in posture from the CLI credential store. Never opens
// an app-server session, so background status checks cannot start MCP servers,
// hooks, or a login flow.
import {
  ProviderDriverKind,
  type ModelCapabilities,
  type RuntimeMode,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as ChildProcess from "effect/process/ChildProcess";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
  type ServerProviderDraft,
} from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import type { ZcodeSettings } from "../settings.ts";
import {
  parseZcodeLoginPresence,
  resolveZcodeInstanceStatePaths,
  type ZcodeInstanceStatePaths,
} from "./instanceState.ts";
import type { ZcodeDiscoveredModelInfo, ZcodeModelDiscoveryCache } from "./modelDiscoveryCache.ts";
import { readZcodeModelDiscoveryCache } from "./modelDiscoveryCache.ts";

export const ZCODE_PROVIDER = ProviderDriverKind.make("zcode");

/** The UI's Effort control id for zcode models (a known reasoning-effort id). */
export const ZCODE_EFFORT_OPTION_ID = "effort";

const ZCODE_SUPPORTED_RUNTIME_MODES: ReadonlyArray<RuntimeMode> = [
  "approval-required",
  "auto-accept-edits",
  "full-access",
];

/**
 * T3 runtime mode → zcode sendText mode. Verified against the CLI's permission
 * service: build asks for every side-effecting tool (the only prompting mode),
 * edit auto-allows file edits, yolo bypasses prompts, and guarded is a yolo
 * base policy that only single-use-confirms dangerous commands — not approval-required.
 */
export const ZCODE_RUNTIME_MODE_BY_T3_MODE: Record<string, string> = {
  "approval-required": "build",
  "auto-accept-edits": "edit",
  "full-access": "yolo",
};

const VERSION_PROBE_TIMEOUT_MS = 10_000;

interface DiscoveredZcodeConfig {
  readonly modelSlugs: ReadonlyArray<string>;
  readonly defaultModelSlug: string | null;
  readonly providerCount: number;
  /** Distinct `account:` providers from model rules (plan logins via `zcode login`). */
  readonly accountProviderCount: number;
  readonly configPath: string | null;
}

const EMPTY_DISCOVERED: DiscoveredZcodeConfig = {
  modelSlugs: [],
  defaultModelSlug: null,
  providerCount: 0,
  accountProviderCount: 0,
  configPath: null,
};

interface PersonalProviderConfigFile {
  readonly schemaVersion?: number;
  readonly config?: {
    readonly providerConfigRules?: {
      readonly providerRules?: ReadonlyArray<{
        readonly providerId?: string;
        readonly config?: {
          readonly personalModelIds?: ReadonlyArray<string>;
        };
      }>;
    };
    readonly modelConfigRules?: {
      readonly providerModelRules?: ReadonlyArray<{
        readonly providerId?: string;
        readonly modelId?: string;
      }>;
    };
    readonly defaultModelSelection?: {
      readonly providerId?: string;
      readonly modelId?: string;
    };
  };
}

/** Pure parser, unit-testable; tolerates absent or malformed fields. */
export function parsePersonalProviderConfig(raw: string): DiscoveredZcodeConfig {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return EMPTY_DISCOVERED;
  }
  const file = parsed as PersonalProviderConfigFile;
  const rules = file.config?.providerConfigRules?.providerRules ?? [];
  const modelSlugs: string[] = [];
  let providerCount = 0;
  for (const rule of rules) {
    const providerId = rule.providerId?.trim();
    if (!providerId) continue;
    providerCount += 1;
    for (const modelId of rule.config?.personalModelIds ?? []) {
      if (typeof modelId === "string" && modelId.trim()) {
        modelSlugs.push(`${providerId}/${modelId}`);
      }
    }
  }
  // Plan logins (`zcode login`) surface as `account:` providers on model rules
  // instead of providerConfigRules entries; their models count as configured.
  const accountProviders = new Set<string>();
  for (const rule of file.config?.modelConfigRules?.providerModelRules ?? []) {
    const providerId = rule.providerId?.trim();
    const modelId = rule.modelId?.trim();
    if (!providerId?.startsWith("account:") || !modelId) continue;
    accountProviders.add(providerId);
    modelSlugs.push(`${providerId}/${modelId}`);
  }
  const selection = file.config?.defaultModelSelection;
  const defaultModelSlug =
    selection?.providerId && selection.modelId
      ? `${selection.providerId}/${selection.modelId}`
      : null;
  return {
    modelSlugs: [...new Set(modelSlugs)],
    defaultModelSlug,
    providerCount,
    accountProviderCount: accountProviders.size,
    configPath: null,
  };
}

function zcodeModelCapabilities(
  info: ZcodeDiscoveredModelInfo | undefined,
): ModelCapabilities | null {
  if (info?.levels === undefined || info.levels.length === 0) return null;
  return {
    optionDescriptors: [
      {
        id: ZCODE_EFFORT_OPTION_ID,
        label: "Effort",
        type: "select",
        options: info.levels.map((level) => ({
          id: level,
          label: level,
          ...(level === info.defaultLevel ? { isDefault: true } : {}),
        })),
        ...(info.defaultLevel !== undefined ? { currentValue: info.defaultLevel } : {}),
      },
    ],
  };
}

function zcodeModelsFromConfig(
  discovered: DiscoveredZcodeConfig,
  modelDiscovery: ZcodeModelDiscoveryCache,
): ReadonlyArray<ServerProviderModel> {
  // The config declares the list: every declared model stays listed and
  // selectable, discovered or not — a sibling that has not run its first turn
  // must never vanish. Discovery only enriches entries with effort levels,
  // and the cache never adds models the config does not name.
  return discovered.modelSlugs.map((slug) => ({
    slug,
    name: slug.split("/").pop() ?? slug,
    isCustom: false,
    ...(slug === discovered.defaultModelSlug ? { isDefault: true } : {}),
    capabilities: zcodeModelCapabilities(modelDiscovery[slug]),
  }));
}

const ZCODE_PRESENTATION = {
  displayName: "ZCode",
  showInteractionModeToggle: false,
  reportsContextWindow: true,
  supportedRuntimeModes: ZCODE_SUPPORTED_RUNTIME_MODES,
  supportsConversationRollback: false,
} as const;

export const buildInitialZcodeProviderSnapshot = Effect.fn(
  "ZcodeStatus.buildInitialZcodeProviderSnapshot",
)(function* (input: { readonly enabled: boolean }): Effect.fn.Return<ServerProviderDraft> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  return buildServerProvider({
    driver: ZCODE_PROVIDER,
    presentation: ZCODE_PRESENTATION,
    enabled: input.enabled,
    checkedAt,
    models: [],
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unauthenticated" },
    },
  });
});

const discoverZcodeConfig = Effect.fn("ZcodeStatus.discoverZcodeConfig")(function* (
  instanceState: ZcodeInstanceStatePaths,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const candidate = instanceState.personalConfigPath;
  if (candidate === null) return EMPTY_DISCOVERED;
  const discovered = yield* fileSystem.readFileString(candidate).pipe(
    Effect.map((raw) => ({ ...parsePersonalProviderConfig(raw), configPath: candidate })),
    Effect.option,
  );
  return Option.isSome(discovered) ? discovered.value : EMPTY_DISCOVERED;
});

/** Login posture: a non-empty entry under a login key in the CLI credential store. */
const readZcodeLoginPresence = Effect.fn("ZcodeStatus.readLoginPresence")(function* (
  credentialsPath: string | null,
) {
  if (credentialsPath === null) return false;
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.readFileString(credentialsPath).pipe(
    Effect.map(parseZcodeLoginPresence),
    Effect.orElseSucceed(() => false),
  );
});

type VersionProbeOutcome =
  | { readonly kind: "ok"; readonly stdout: string }
  | { readonly kind: "missing" }
  | { readonly kind: "timeout" }
  | { readonly kind: "failed" };

export const checkZcodeProviderStatus = Effect.fn("ZcodeStatus.check")(function* (
  config: ZcodeSettings,
  env: NodeJS.ProcessEnv,
  home: string,
  options: { readonly discoveryCacheDir?: string | undefined } = {},
) {
  const binaryPath = config.binaryPath?.trim() || "zcode";
  const command = yield* resolveSpawnCommand(binaryPath, ["--version"], { env });
  const versionProbe: VersionProbeOutcome = yield* spawnAndCollect(
    binaryPath,
    ChildProcess.make(command.command, [...command.args], {
      env,
      extendEnv: false,
      shell: command.shell,
      stdin: "ignore",
    }),
  ).pipe(
    Effect.timeout(VERSION_PROBE_TIMEOUT_MS),
    Effect.map((result): VersionProbeOutcome => ({ kind: "ok", stdout: result.stdout })),
    Effect.catchTags({
      TimeoutError: () => Effect.succeed({ kind: "timeout" } as const),
    }),
    Effect.catch((cause): Effect.Effect<VersionProbeOutcome> =>
      Effect.succeed(isCommandMissingCause(cause) ? { kind: "missing" } : { kind: "failed" }),
    ),
  );

  const instanceState = yield* resolveZcodeInstanceStatePaths(env, home);
  const discovered = yield* discoverZcodeConfig(instanceState);
  const loggedIn = yield* readZcodeLoginPresence(instanceState.credentialsPath);
  const modelDiscovery = yield* readZcodeModelDiscoveryCache(
    options.discoveryCacheDir ?? null,
    instanceState.discoveryCacheScope,
  );

  const modelsFromConfig = zcodeModelsFromConfig(discovered, modelDiscovery);
  const models = providerModelsFromSettings(modelsFromConfig, config.customModels, null);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);

  if (versionProbe.kind !== "ok") {
    const message =
      versionProbe.kind === "missing"
        ? "Install the zcode CLI and make sure `zcode --version` runs."
        : versionProbe.kind === "timeout"
          ? "`zcode --version` timed out."
          : "Failed to run `zcode --version`.";
    const probe: ProviderProbeResult = {
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unauthenticated" },
      message,
    };
    return buildServerProvider({
      driver: ZCODE_PROVIDER,
      presentation: ZCODE_PRESENTATION,
      enabled: config.enabled,
      checkedAt,
      models,
      probe,
    });
  }

  const version = parseGenericCliVersion(versionProbe.stdout);
  const planLogin = discovered.accountProviderCount > 0;
  const configured = discovered.providerCount > 0 || planLogin || config.customModels.length > 0;
  // Config rules alone never read as signed in: the credential store decides.
  const auth: ProviderProbeResult["auth"] = planLogin
    ? loggedIn
      ? { status: "authenticated" }
      : { status: "unauthenticated" }
    : configured
      ? { status: "unknown" }
      : { status: "unauthenticated" };
  const probe: ProviderProbeResult = configured
    ? {
        installed: true,
        version,
        status: models.length > 0 && (!planLogin || loggedIn) ? "ready" : "warning",
        auth,
        ...(models.length === 0
          ? {
              message:
                "No models found. Add a provider with personalModelIds to the zcode provider config.",
            }
          : planLogin && !loggedIn
            ? {
                message:
                  "Run `zcode login` on the server machine: the account models are configured, but its credential store holds no active login.",
              }
            : {}),
      }
    : {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unauthenticated" },
        message:
          "Configure zcode first: run `zcode login` or add an API-key provider to ~/.zcode/v2/provider_config.json.",
      };
  return buildServerProvider({
    driver: ZCODE_PROVIDER,
    presentation: ZCODE_PRESENTATION,
    enabled: config.enabled,
    checkedAt,
    models,
    probe,
  });
});
