# ZCode

T3 Code can drive [ZCode](https://zcode.z.ai) through its CLI's `app-server` mode. ZCode uses the provider configuration on the machine that runs the T3 Code server.

## Set up ZCode

1. Install the zcode CLI on that machine and check it runs:

   ```bash
   zcode --version
   ```

2. Configure an API-key provider ZCode can use without its desktop app, in `~/.zcode/v2/provider_config.json`. List the models ZCode may use under `personalModelIds`, pick one as the default, and enable each model with a `modelConfigRules` rule. The `group` field and the enabled-rule are required: without `group` ZCode silently drops the provider, and without `modelConfigRules` it rejects the whole file:

   ```json
   {
     "schemaVersion": 1,
     "config": {
       "providerConfigRules": {
         "providerRules": [
           {
             "providerId": "my-provider",
             "providerName": "My provider",
             "config": {
               "group": "standard-personal",
               "access": { "type": "api-key", "apiKey": "sk-…" },
               "api": {
                 "type": "openai-chat-completions",
                 "baseUrl": "https://api.example.com/v1"
               },
               "personalModelIds": ["model-a"]
             }
           }
         ]
       },
       "modelConfigRules": {
         "providerModelRules": [
           {
             "providerId": "my-provider",
             "modelId": "model-a",
             "config": { "enabled": true }
           }
         ],
         "manualProviderModelRules": []
       },
       "defaultModelSelection": { "providerId": "my-provider", "modelId": "model-a" }
     }
   }
   ```

3. Open **Settings → Providers**, enable ZCode, and refresh the provider.

If `zcode` is not on the server's `PATH`, set **Binary path** to the executable. **Launch arguments** adds extra CLI arguments after `app-server`; arguments are split on whitespace, and double or single quotes keep spaces inside one argument.

## Account login

T3 Code spawns `zcode app-server` with `ZCODE_STANDALONE_ACCOUNT=1`, so on a zcode CLI build that supports standalone accounts the app-server reuses the machine's z.ai coding-plan login on its own — no desktop app needed as the host:

1. Run `zcode login` once on the server machine. On headless machines, `zcode login --no-browser` prints a URL and waits for the login to complete.
2. Refresh the provider in **Settings → Providers**. The plan's models appear next to your API-key models, and turns run on the plan's credentials.

A ZCode process that started before you logged in may not offer the plan's models. If a thread's first message to a plan model is rejected that way, T3 Code retries it once in a fresh ZCode process; if it is still rejected, start a new thread. Logging into the ZCode desktop app is not enough: that login belongs to the desktop host and is not written to the CLI's shared credential store.

The model picker shows every model your plan's config declares right away, and each one is selectable for its first turn. After a model runs its first turn, zcode reports what it serves for that model, and T3 Code fills in its details such as effort levels. Models your plan stops advertising (for example after switching plans) leave the picker when the config says so; cache entries from a previous login never resurrect them.

Older zcode builds ignore `ZCODE_STANDALONE_ACCOUNT` and behave as before — configure the API-key provider above. To opt out on a build that supports it, set `ZCODE_STANDALONE_ACCOUNT=0` in the T3 Code server's environment.

## Models

The model picker lists the models ZCode's provider config declares — your API-key providers' `personalModelIds` and the models of your account login when it is active — plus any custom models you add in provider settings.

Reasoning models report their effort levels, and which level is the default, after their first turn. Refresh the provider in **Settings → Providers** once that has run: the model picker's **Effort** column then lists the levels with the model's own default preselected. T3 Code runs the model at the level you pick, or at the model's default if you pick none. Until a model has reported its levels, its first turn runs at zcode's own default; if zcode refuses that turn for lack of a level, T3 Code resends it once with the model's default.

## Permission modes

ZCode enforces T3 Code's permission modes natively.

| T3 Code mode      | ZCode mode |
| ----------------- | ---------- |
| Approval required | build      |
| Auto-accept edits | edit       |
| Full access       | yolo       |

A thread set to a mode ZCode does not support keeps its current mode. Approvals and questions arrive in the thread's request panel; options such as allowing once, for the session, or always map back to ZCode's own choices.

## Not supported yet

- Conversation rollback.
- Usage limits from ZCode's account providers.

See [provider setup](./install.md#providers) and [Permission modes](./permission-modes.md).
