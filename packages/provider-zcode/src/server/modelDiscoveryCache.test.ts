// Discovery cache behavior: per-instance scoping, concurrent writes merging
// without losing entries (the atomic rename keeps readers safe), and skipping
// rewrites when an announcement changes nothing.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import { resolveZcodeInstanceStatePaths } from "./instanceState.ts";
import {
  readZcodeModelDiscoveryCache,
  writeZcodeModelDiscoveryCache,
} from "./modelDiscoveryCache.ts";

const GLM = "account:p/GLM-5.3";
const GLM_ANNOUNCEMENT = { [GLM]: { levels: ["low", "high"], defaultLevel: "high" } };

describe("ZcodeModelDiscovery cache", () => {
  it.effect("isolates entries per instance scope", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cacheDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-zcode-cache-scope-",
      });
      yield* writeZcodeModelDiscoveryCache(cacheDir, GLM_ANNOUNCEMENT, "instance-a");
      // The other instance's scope reads empty, and its write never touches A.
      assert.deepStrictEqual(yield* readZcodeModelDiscoveryCache(cacheDir, "instance-b"), {});
      yield* writeZcodeModelDiscoveryCache(
        cacheDir,
        { "account:p/Other": { contextWindow: 4096 } },
        "instance-b",
      );
      assert.deepStrictEqual(
        yield* readZcodeModelDiscoveryCache(cacheDir, "instance-a"),
        GLM_ANNOUNCEMENT,
      );
      assert.deepStrictEqual(yield* readZcodeModelDiscoveryCache(cacheDir, "instance-b"), {
        "account:p/Other": { contextWindow: 4096 },
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("merges concurrent announcements without losing entries", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cacheDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-zcode-cache-race-",
      });
      const scope = "shared-account";
      yield* Effect.all(
        [
          writeZcodeModelDiscoveryCache(cacheDir, GLM_ANNOUNCEMENT, scope),
          writeZcodeModelDiscoveryCache(
            cacheDir,
            { "account:p/GLM-5.3-Flash": { levels: ["low"] } },
            scope,
          ),
          writeZcodeModelDiscoveryCache(
            cacheDir,
            { "ollama-local/qwen": { contextWindow: 8192 } },
            scope,
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.deepStrictEqual(yield* readZcodeModelDiscoveryCache(cacheDir, scope), {
        ...GLM_ANNOUNCEMENT,
        "account:p/GLM-5.3-Flash": { levels: ["low"] },
        "ollama-local/qwen": { contextWindow: 8192 },
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("skips the rewrite when an announcement changes nothing", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cacheDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-zcode-cache-skip-",
      });
      const scope = "shared-account";
      yield* writeZcodeModelDiscoveryCache(cacheDir, GLM_ANNOUNCEMENT, scope);
      let renames = 0;
      const countingFs = FileSystem.make({
        ...fileSystem,
        rename: (from: string, to: string) =>
          Effect.tap(fileSystem.rename(from, to), () =>
            Effect.sync(() => {
              renames += 1;
            }),
          ),
      });
      yield* writeZcodeModelDiscoveryCache(cacheDir, GLM_ANNOUNCEMENT, scope).pipe(
        Effect.provideService(FileSystem.FileSystem, countingFs),
      );
      assert.strictEqual(renames, 0);
      assert.deepStrictEqual(
        yield* readZcodeModelDiscoveryCache(cacheDir, scope),
        GLM_ANNOUNCEMENT,
      );
      yield* writeZcodeModelDiscoveryCache(
        cacheDir,
        { [GLM]: { levels: ["low", "high", "max"], defaultLevel: "max" } },
        scope,
      ).pipe(Effect.provideService(FileSystem.FileSystem, countingFs));
      assert.strictEqual(renames, 1);
      assert.deepStrictEqual(yield* readZcodeModelDiscoveryCache(cacheDir, scope), {
        [GLM]: { levels: ["low", "high", "max"], defaultLevel: "max" },
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("resolveZcodeInstanceStatePaths", () => {
  it.effect("keys the scope on ZCODE_DATA_BASE_DIR over HOME, plus the config override", () =>
    Effect.gen(function* () {
      const base = yield* resolveZcodeInstanceStatePaths({ HOME: "/home/user" }, "/host/home");
      const dataDir = yield* resolveZcodeInstanceStatePaths(
        { HOME: "/home/user", ZCODE_DATA_BASE_DIR: "/data/zcode-state" },
        "/host/home",
      );
      const overridden = yield* resolveZcodeInstanceStatePaths(
        { HOME: "/home/user", ZCODE_PERSONAL_PROVIDER_CONFIG_FILE: "/other/config.json" },
        "/host/home",
      );
      assert.notStrictEqual(base.discoveryCacheScope, dataDir.discoveryCacheScope);
      assert.notStrictEqual(base.discoveryCacheScope, overridden.discoveryCacheScope);
      assert.strictEqual(dataDir.credentialsPath, "/data/zcode-state/.zcode/v2/credentials.json");
      assert.strictEqual(base.credentialsPath, "/home/user/.zcode/v2/credentials.json");
      assert.strictEqual(base.personalConfigPath, "/home/user/.zcode/v2/provider_config.json");
      assert.strictEqual(overridden.personalConfigPath, "/other/config.json");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("falls back to the host home and yields null paths without one", () =>
    Effect.gen(function* () {
      const fromHost = yield* resolveZcodeInstanceStatePaths({}, "/host/home");
      assert.strictEqual(fromHost.personalConfigPath, "/host/home/.zcode/v2/provider_config.json");
      const homeless = yield* resolveZcodeInstanceStatePaths({}, "");
      assert.strictEqual(homeless.personalConfigPath, null);
      assert.strictEqual(homeless.credentialsPath, null);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
