import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { ZcodeSettings } from "./settings.ts";

export const zcodeClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("zcode"),
  label: "ZCode",
  settingsSchema: ZcodeSettings,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [{ d: "M5 4h14v3.2H10.6L19 16.8V20H5v-3.2h8.4L5 7.2Z" }],
  },
});
