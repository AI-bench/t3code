import { describe, expect, it } from "@effect/vitest";

import { zcodeAppServerArgs, zcodeLaunchEnvironment } from "./adapter.ts";

describe("zcodeLaunchEnvironment", () => {
  it("defaults ZCODE_STANDALONE_ACCOUNT=1 so spawned app-servers own their account login", () => {
    const env = zcodeLaunchEnvironment({ PATH: "/usr/bin" });
    expect(env.ZCODE_STANDALONE_ACCOUNT).toBe("1");
    expect(env.PATH).toBe("/usr/bin");
  });

  it("keeps an explicit ZCODE_STANDALONE_ACCOUNT override (e.g. 0 for fixture recording)", () => {
    const env = zcodeLaunchEnvironment({ ZCODE_STANDALONE_ACCOUNT: "0" });
    expect(env.ZCODE_STANDALONE_ACCOUNT).toBe("0");
  });

  it("still strips ambient desktop markers before applying the default", () => {
    const env = zcodeLaunchEnvironment({
      ZCODE_OFFICIAL_ENDPOINT: "unix:///desktop",
      ZCODE_BUILTIN_PROVIDER_CONFIG_FILE: "/desktop-managed.json",
    });
    expect(env.ZCODE_OFFICIAL_ENDPOINT).toBeUndefined();
    expect(env.ZCODE_BUILTIN_PROVIDER_CONFIG_FILE).toBeUndefined();
    expect(env.ZCODE_STANDALONE_ACCOUNT).toBe("1");
  });
});

describe("zcodeAppServerArgs", () => {
  it("always leads with app-server and splits plain arguments on whitespace", () => {
    expect(zcodeAppServerArgs("")).toEqual(["app-server"]);
    expect(zcodeAppServerArgs("--trace --log-level debug")).toEqual([
      "app-server",
      "--trace",
      "--log-level",
      "debug",
    ]);
  });

  it("keeps quoted whitespace as one argument", () => {
    expect(zcodeAppServerArgs('--provider-config "/tmp/my config.json"')).toEqual([
      "app-server",
      "--provider-config",
      "/tmp/my config.json",
    ]);
    expect(zcodeAppServerArgs("--label 'two words'")).toEqual([
      "app-server",
      "--label",
      "two words",
    ]);
  });

  it("honors backslash escapes alongside quoted arguments", () => {
    expect(zcodeAppServerArgs('a\\ b "c"')).toEqual(["app-server", "a b", "c"]);
  });
});
