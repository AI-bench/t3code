// zcode resolves its per-user state (credential store, personal provider
// config) from ZCODE_DATA_BASE_DIR, else HOME, mirroring the CLI's own
// resolution — so the status probe and the discovery cache address the same
// account's files that the spawned app-server reads.
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Path from "effect/Path";

const ZCODE_PERSONAL_PROVIDER_CONFIG_ENV = "ZCODE_PERSONAL_PROVIDER_CONFIG_FILE";
const ZCODE_DATA_BASE_DIR_ENV = "ZCODE_DATA_BASE_DIR";

export interface ZcodeInstanceStatePaths {
  readonly personalConfigPath: string | null;
  readonly credentialsPath: string | null;
  /** Names the discovery-cache file; instances on different accounts never share entries. */
  readonly discoveryCacheScope: string;
}

const sha256Hex = (value: string): Effect.Effect<string> =>
  Effect.promise(async () =>
    Hex.encode(
      new Uint8Array(
        await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)),
      ),
    ),
  );

export const resolveZcodeInstanceStatePaths = Effect.fn("ZcodeInstanceState.resolvePaths")(
  function* (env: NodeJS.ProcessEnv, hostHome: string) {
    const path = yield* Path.Path;
    const baseDir = env[ZCODE_DATA_BASE_DIR_ENV]?.trim() || env["HOME"]?.trim() || hostHome.trim();
    const override = env[ZCODE_PERSONAL_PROVIDER_CONFIG_ENV]?.trim() || null;
    const stateRoot = baseDir ? path.join(baseDir, ".zcode", "v2") : null;
    const digest = yield* sha256Hex(`${baseDir}\n${override ?? ""}`);
    return {
      personalConfigPath:
        override ?? (stateRoot === null ? null : path.join(stateRoot, "provider_config.json")),
      credentialsPath: stateRoot === null ? null : path.join(stateRoot, "credentials.json"),
      // Hashed, never the raw values: instance env may carry sensitive paths.
      discoveryCacheScope: digest.slice(0, 16),
    };
  },
);

// The store's values are CLI-encrypted; presence of a login key — never its
// value — decides the sign-in posture.
const LOGIN_CREDENTIAL_KEY = "zcodejwttoken";
const LOGIN_CREDENTIAL_PREFIX = "oauth:";

/** Pure parser, unit-testable: does this credential-store envelope hold an account login? */
export function parseZcodeLoginPresence(raw: string): boolean {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return false;
  }
  if (typeof parsed !== "object" || parsed === null) return false;
  return Object.entries(parsed as Record<string, unknown>).some(
    ([key, value]) =>
      (key === LOGIN_CREDENTIAL_KEY || key.startsWith(LOGIN_CREDENTIAL_PREFIX)) &&
      typeof value === "string" &&
      value.length > 0,
  );
}
