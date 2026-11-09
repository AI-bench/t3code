// ZCode native adapter: drives `zcode app-server` over ZCode Protocol v4
// (conversation topics + command inbox) and projects its authoritative
// ConversationSnapshot into orchestration events.
import type {
  MessageId,
  ModelSelection,
  OrchestrationV2ConversationMessage,
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderFailure,
  OrchestrationV2ProviderRef,
  OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2Subagent,
  OrchestrationV2TurnItem,
  OrchestrationV2ProviderCapabilities,
  ProviderInstanceId,
  ThreadId,
  ThreadTokenUsageSnapshot,
  TurnItemId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { ZcodeSettings } from "../settings.ts";
import { ZCODE_EFFORT_OPTION_ID, ZCODE_PROVIDER, ZCODE_RUNTIME_MODE_BY_T3_MODE } from "./status.ts";
import { resolveZcodeInstanceStatePaths } from "./instanceState.ts";
import {
  writeZcodeModelDiscoveryCache,
  type ZcodeDiscoveredModelInfo,
  type ZcodeModelDiscoveryCache,
} from "./modelDiscoveryCache.ts";
import {
  conversationTopic,
  parseZcodeModelSlug,
  recordArray,
  recordField,
  recordNumber,
  recordObject,
  recordString,
  ZCODE_METHODS,
  ZCODE_NOTIFICATIONS,
  ZCODE_PARK_REVERSE,
  ZCODE_REVERSE_METHODS,
  type ZcodeCommandAck,
  type ZcodeModelSelection,
  type ZcodeRecord,
  type ZcodeWireFrame,
} from "./protocol.ts";
import { makeZcodeRpcConnection, ZcodeRpcError, type ZcodeRpcConnection } from "./rpc.ts";
import { WireFrameReassembler } from "./wire.ts";
import {
  applyConversationDelta,
  readSnapshotState,
  type ZcodeConversationState,
} from "./snapshotReducer.ts";

const ZCODE_DRIVER_KIND = ZCODE_PROVIDER;
const DEFAULT_ZCODE_SETTINGS = Schema.decodeSync(ZcodeSettings)({});

/** Sentinel meaning "let zcode's own default model selection apply". Never sent. */
const ZCODE_INHERIT_MODEL_SLUG = "default";

const ZCODE_REQUEST_TIMEOUT_MS = 30_000;
const ZCODE_CREATE_SESSION_TIMEOUT_MS = 60_000;
const ZCODE_INTERRUPT_SETTLE_TIMEOUT_MS = 15_000;
// zcode announces capabilities right behind a level-less first turn's failure;
// these only bound the waits if the announcement never lands.
const ZCODE_REASONING_LEVEL_WAIT_TIMEOUT = Duration.millis(500);
const ZCODE_STEER_CAPABILITY_WAIT_TIMEOUT = Duration.seconds(2);

const isZcodeRpcError = Schema.is(ZcodeRpcError);

const ZCODE_TRANSPORT_LOST_MESSAGE = "ZCode process exited unexpectedly.";

// Ambient desktop markers a standalone app-server must not inherit: the
// official-endpoint socket and runtime identity belong to the desktop host.
const AMBIENT_DESKTOP_ENV_KEYS = [
  "ZCODE_OFFICIAL_ENDPOINT",
  "ZCODE_OFFICIAL_ENDPOINT_TOKEN",
  "ZCODE_APP_VERSION",
  "ZCODE_PROCESS_LABEL",
  "ZCODE_RUNTIME_ENV",
  "ZCODE_ENV",
  "ZCODE_BUILTIN_PROVIDER_CONFIG_FILE",
  "ZCODE_DESKTOP_CONTEXT_PROMPT_ENABLED",
  "ZCODE_BFS_BINARY",
  "ZCODE_RG_BINARY",
  "ZCODE_UGREP_BINARY",
] as const;

export function zcodeLaunchEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const cleaned: Record<string, string | undefined> = { ...env };
  for (const key of AMBIENT_DESKTOP_ENV_KEYS) {
    delete cleaned[key];
  }
  // We are not a desktop host, so the spawned app-server must own its
  // credentials (CLI `zcode login`). An ambient ZCODE_STANDALONE_ACCOUNT
  // wins — set 0 for host-managed semantics (e.g. fixture recording).
  if (cleaned.ZCODE_STANDALONE_ACCOUNT === undefined) {
    cleaned.ZCODE_STANDALONE_ACCOUNT = "1";
  }
  return cleaned;
}

export function zcodeAppServerArgs(launchArgs: string): ReadonlyArray<string> {
  return ["app-server", ...tokenizeCliArgs(launchArgs)];
}

const ZcodeProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: true,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: true,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface ZcodeAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: ZcodeSettings;
  readonly environment: NodeJS.ProcessEnv;
}

interface ActiveZcodeTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  commandId: string;
  nativeTurnLinked: boolean;
  nextItemOrdinal: number;
  readonly itemOrdinals: Map<string, number>;
  interrupted: boolean;
  failure: OrchestrationV2ProviderFailure | null;
  readonly settle: Deferred.Deferred<void, ProviderAdapter.ProviderAdapterV2Error>;
  /** Text exactly as sent to zcode (attachment-annotated), for model-level resend. */
  sentText: string;
  /** The sent modelSelection omitted reasoningLevel while levels were unknown. */
  sentWithoutReasoningLevel: boolean;
  modelLevelRetried: boolean;
  /** First contextUsage frame already emitted; later per-frame reports are suppressed. */
  usageEmitted: boolean;
  /** Completed on first native link and at settlement; wakes capability waiters. */
  readonly nativeLinked: Deferred.Deferred<void>;
}

interface ZcodeInteraction {
  readonly runtimeRequest: OrchestrationV2RuntimeRequest;
  /** zcode optionId by T3 decision, for provider-owned option ids. */
  readonly optionByDecision: Map<string, string>;
  /** Parked reverse-channel protocol ids; zcode settles its race on any one answer. */
  readonly reverseRequestIds: Set<string | number>;
  /** Set once a snapshot carried this interaction; only then may absence mean resolved. */
  snapshotSeen: boolean;
}

interface ZcodeThreadState {
  /** Re-assigned when a respawn cannot re-hydrate a never-used draft session. */
  nativeSessionId: string;
  /** Re-assigned when the app-server process is respawned and re-subscribed. */
  subscriptionId: string | null;
  providerThread: OrchestrationV2ProviderThread;
  snapshot: ZcodeConversationState | null;
  /** Last applied frame's toSeq; frames account (fromSeq, toSeq], so a delta must start here. */
  frameSeq: number | null;
  /** A resync is outstanding; cleared by the next snapshot frame or a failed request. */
  resyncInFlight: boolean;
  activeTurn: ActiveZcodeTurn | null;
  readonly interactions: Map<string, ZcodeInteraction>;
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: ZCODE_PROVIDER, nativeId, strength };
}

function isZcodeWireFrame(value: unknown): value is ZcodeWireFrame & { deliveryKind?: string } {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  if (record.kind === "complete") {
    return (
      typeof record.topic === "string" &&
      typeof record.subscriptionId === "string" &&
      typeof record.frame === "object" &&
      record.frame !== null
    );
  }
  return (
    record.kind === "fragment" &&
    typeof record.topic === "string" &&
    typeof record.subscriptionId === "string" &&
    typeof record.logicalFrameId === "string" &&
    typeof record.fragmentIndex === "number" &&
    typeof record.fragmentCount === "number" &&
    typeof record.logicalBytes === "number" &&
    typeof record.dataBase64 === "string"
  );
}

/** zcode permission option → T3 approval decision (option ids pass through). */
function t3DecisionByZcodeOption(
  option: ZcodeRecord,
): { decision: string; optionId: string } | null {
  const optionId = recordString(option, "optionId");
  if (optionId === undefined) return null;
  const kind = recordString(option, "kind");
  const decision =
    kind === "allowOnce" || optionId === "allowOnce"
      ? "accept"
      : kind === "allowAlways" || optionId === "allowAlways"
        ? "acceptAlways"
        : optionId === "allowSession" || kind === "allow_session"
          ? "acceptForSession"
          : kind === "deny" || optionId === "deny"
            ? "decline"
            : optionId === "fullAccess"
              ? "acceptAlways"
              : null;
  if (decision === null) return null;
  return { decision, optionId };
}

function toolKindForZcodeTool(
  toolName: string | undefined,
): "command" | "file" | "search" | "other" {
  const name = (toolName ?? "").toLowerCase();
  if (/(bash|shell|terminal|exec|command)/.test(name)) return "command";
  if (/(write|edit|patch|notebook|apply)/.test(name)) return "file";
  if (/(search|grep|glob|find|list|read)/.test(name)) return "search";
  return "other";
}

// Module-scope JSON helpers: bounded stringify for display fields only.
function boundedJson(value: unknown, max: number): string | undefined {
  try {
    const text = JSON.stringify(value);
    if (typeof text !== "string") return undefined;
    return text.length > max ? `${text.slice(0, max)}…` : text;
  } catch {
    return undefined;
  }
}

function parseFileNameFromToolInput(inputText: string): string | null {
  const match = /(?:file_path|path|fileName|file)["']?\s*[:=]\s*["']([^"']+)["']/i.exec(inputText);
  return match?.[1] ?? null;
}

// sendCommand folds a rejected ACK's reasonCode into its detail right after
// the status word; account-model `modelUnavailable` there means the process
// was born without the account registry, not that the model is missing.
const SEND_TEXT_MODEL_UNAVAILABLE_PATTERN = /^v4\/command sendText was \w+ \(modelUnavailable\)/;

function isAccountModelUnavailable(
  error: ProviderAdapter.ProviderAdapterV2Error,
  modelSlug: string,
): boolean {
  if (!modelSlug.startsWith("account:")) return false;
  return (
    error._tag === "ProviderAdapterProtocolError" &&
    SEND_TEXT_MODEL_UNAVAILABLE_PATTERN.test(error.detail)
  );
}

const EMPTY_THREAD_SNAPSHOT: ZcodeConversationState = {
  sessionId: "",
  logEpoch: "",
  seq: 0,
  revision: 0,
  control: undefined,
  availability: undefined,
  inputRouting: undefined,
  meta: undefined,
  config: undefined,
  usage: undefined,
  queue: undefined,
  pendingInteractions: [],
  backgroundWorks: [],
  subagents: undefined,
  goal: null,
  plan: null,
  runtimeInitialization: undefined,
  rows: { window: [], totalCount: 0, firstRowId: null },
};

const makeZcodeAdapterV2 = Effect.fn("makeZcodeAdapterV2")(function* (
  options: ZcodeAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  // Captured here so the session runtime never demands services beyond its
  // scope; the discovery cache write reuses these exact services.
  const discoveryFileSystem = yield* FileSystem.FileSystem;
  const discoveryPath = yield* Path.Path;
  const discoveryInstanceState = yield* resolveZcodeInstanceStatePaths(
    options.environment,
    yield* HostProcess.HomeDirectory,
  );

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: ZCODE_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: ZCODE_PROVIDER,
    getCapabilities: Effect.fn("ZcodeAdapter.getCapabilities")(function () {
      return Effect.succeed(ZcodeProviderCapabilitiesV2);
    }),
    planSelectionTransition: Effect.fn("ZcodeAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("ZcodeAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      // The adapter driver merged the instance environment at create time.
      const env = zcodeLaunchEnvironment(options.environment);

      let handleReverseRequest:
        | ((
            method: string,
            params: unknown,
            protocolRequestId: string | number,
          ) => Effect.Effect<unknown, ZcodeRpcError>)
        | null = null;

      // The app-server process is replaceable at runtime: one born while the
      // credential store was mid-login-write never registers account models
      // and stays modelless forever, so only a fresh process cures it (see
      // respawnZcodeProcess).
      const spawnZcodeConnection = Effect.fnUntraced(function* (): Effect.fn.Return<
        ZcodeRpcConnection,
        ProviderAdapter.ProviderAdapterV2Error
      > {
        return yield* makeZcodeRpcConnection({
          command: options.settings.binaryPath?.trim() || "zcode",
          args: zcodeAppServerArgs(options.settings.launchArgs ?? ""),
          cwd,
          env,
          reverseRequests: {
            onRequest: (method, params, protocolRequestId) =>
              handleReverseRequest === null
                ? Effect.undefined
                : handleReverseRequest(method, params, protocolRequestId),
          },
        }).pipe(
          // Route every incarnation's teardown through the session scope:
          // respawns spawn later from startTurn, where no ambient session
          // scope flows.
          Effect.provideService(Scope.Scope, scope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: ZCODE_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      });
      let connection = yield* spawnZcodeConnection();
      // Held across a respawn (terminate → spawn → re-subscribe) so steer
      // sends wait for the fresh process instead of failing against the
      // terminated transport.
      const respawnPermit = yield* Semaphore.make(1);
      // Connections replaced or torn down on purpose; their transport dying is
      // expected, so the notification pump must not report it as a crash.
      const retiredConnections = new WeakSet<ZcodeRpcConnection>();
      // Suspended: respawn swaps `connection`, and close tears down the live one.
      let closed = false;
      yield* Scope.addFinalizer(
        scope,
        Effect.suspend(() => {
          closed = true;
          retiredConnections.add(connection);
          return connection.terminate;
        }).pipe(Effect.ignore),
      );

      const now0 = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: ZCODE_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: ZcodeProviderCapabilitiesV2,
        createdAt: now0,
        updatedAt: now0,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error
      >();
      // Serialize snapshot application so terminal cannot overtake request settlement.
      const statePermit = yield* Semaphore.make(1);
      let threadState: ZcodeThreadState | null = null;
      // First snapshot of a conversation topic that is not this session's own
      // thread (a fork source); applyFrame would otherwise drop it.
      const pendingSnapshots = new Map<string, Deferred.Deferred<ZcodeConversationState>>();
      // Model discovery from state.updated: slug → context window + reasoning levels.
      const modelInfo = new Map<
        string,
        { contextWindow?: number; levels?: ReadonlyArray<string>; defaultLevel?: string }
      >();
      // Parked until the named slug's capability announcement lands.
      const modelCapabilityWaiters = new Map<string, Array<Deferred.Deferred<void>>>();
      const awaitModelCapability = (slug: string): Effect.Effect<void> =>
        Effect.suspend(() => {
          if (modelInfo.has(slug)) return Effect.void;
          return Effect.gen(function* () {
            const capability = yield* Deferred.make<void>();
            const waiters = modelCapabilityWaiters.get(slug);
            if (waiters === undefined) {
              modelCapabilityWaiters.set(slug, [capability]);
            } else {
              waiters.push(capability);
            }
            yield* Deferred.await(capability).pipe(
              // Drop the entry when the wait is lost (timeout race) or scoped out.
              Effect.onExit(() =>
                Effect.sync(() => {
                  const current = modelCapabilityWaiters.get(slug);
                  if (current === undefined) return;
                  const next = current.filter((candidate) => candidate !== capability);
                  if (next.length === 0) modelCapabilityWaiters.delete(slug);
                  else modelCapabilityWaiters.set(slug, next);
                }),
              ),
            );
          });
        });

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) {
        const updatedAt = yield* DateTime.now;
        sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
        yield* emit({
          type: "provider_session.updated",
          driver: ZCODE_PROVIDER,
          providerSession: sessionEntity,
        });
      });

      // Streaming turns re-report contextUsage on every frame; suppress those
      // intermediates after the turn's first usage frame — settlement leaves
      // the turn, so the settling frame emits the last report. Idle-period
      // frames stay per-frame: snapshot reconciliation syncs on them.
      const updateProviderThread = Effect.fnUntraced(function* (
        state: ZcodeThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        const patchKeys = Object.keys(patch) as Array<keyof OrchestrationV2ProviderThread>;
        const usageOnly = patchKeys.every((key) => key === "contextUsage");
        const turn = state.activeTurn;
        if (turn !== null && usageOnly) {
          if (turn.usageEmitted) return;
          turn.usageEmitted = true;
        }
        const updatedAt = yield* DateTime.now;
        state.providerThread = { ...state.providerThread, ...patch, updatedAt };
        yield* emit({
          type: "provider_thread.updated",
          driver: ZCODE_PROVIDER,
          providerThread: state.providerThread,
          ...(activeWorksCount(state) > 0 ? { pendingBackgroundWork: true } : {}),
        });
      });

      function activeWorksCount(state: ZcodeThreadState): number {
        return recordArray(state.snapshot?.control ?? {}, "activeWorks").length;
      }

      const sendCommand = Effect.fnUntraced(function* (
        type: string,
        payload: ZcodeRecord,
        sessionId: string | null,
        extra?: {
          readonly baseRevision?: number;
          readonly baseLogEpoch?: string;
          /** Pre-allocated id so the caller can bind turn state before the write. */
          readonly commandId?: string;
        },
      ): Effect.fn.Return<ZcodeCommandAck, ProviderAdapter.ProviderAdapterV2Error> {
        const commandId = extra?.commandId !== undefined ? extra.commandId : yield* randomUuidV4;
        const issuedAt = yield* DateTime.now;
        const envelope: ZcodeRecord = {
          commandId,
          clientId: `t3-${options.instanceId}`,
          sessionId,
          type,
          payload,
          issuedAt: DateTime.toEpochMillis(issuedAt),
          ...(extra?.baseRevision !== undefined ? { baseRevision: extra.baseRevision } : {}),
          ...(extra?.baseLogEpoch !== undefined ? { baseLogEpoch: extra.baseLogEpoch } : {}),
        };
        const result = yield* connection
          .request(ZCODE_METHODS.command, envelope, ZCODE_REQUEST_TIMEOUT_MS)
          .pipe(
            Effect.mapError((cause) =>
              protocolError(
                `v4/command ${type} failed`,
                isZcodeRpcError(cause) ? cause.detail : cause,
              ),
            ),
          );
        const ack = result as ZcodeCommandAck;
        if (ack === undefined || typeof ack !== "object" || typeof ack.status !== "string") {
          return yield* protocolError(`v4/command ${type} returned a malformed ACK`);
        }
        if (ack.status !== "accepted" && ack.status !== "duplicate" && ack.status !== "noop") {
          return yield* protocolError(
            `v4/command ${type} was ${ack.status}${ack.reasonCode ? ` (${ack.reasonCode})` : ""}${
              ack.message ? `: ${ack.message}` : ""
            }`,
          );
        }
        return ack;
      });

      const subscribeTopic = Effect.fnUntraced(function* (
        sessionId: string,
      ): Effect.fn.Return<
        { readonly subscriptionId: string | null },
        ProviderAdapter.ProviderAdapterV2Error
      > {
        const result = yield* connection
          .request(
            ZCODE_METHODS.conversationSubscribe,
            {
              topic: conversationTopic(sessionId),
              connectionId: `t3-${options.instanceId}`,
              clientMode: "desktop-continuous",
            },
            ZCODE_REQUEST_TIMEOUT_MS,
          )
          .pipe(
            Effect.mapError((cause) =>
              protocolError(
                "v4/conversation/subscribe failed",
                isZcodeRpcError(cause) ? cause.detail : cause,
              ),
            ),
          );
        const subscriptionId = recordString(recordObject(result, "ack") ?? {}, "subscriptionId");
        return { subscriptionId: subscriptionId ?? null };
      });

      // ── Interactions (approvals & questions) ───────────────────────────────

      const optionDecisionsFor = (payload: ZcodeRecord | undefined) => {
        const optionByDecision = new Map<string, string>();
        if (payload === undefined) return optionByDecision;
        for (const option of recordArray(payload, "options")) {
          const mapped = t3DecisionByZcodeOption(option as ZcodeRecord);
          if (mapped !== null) optionByDecision.set(mapped.decision, mapped.optionId);
        }
        return optionByDecision;
      };

      const currentRuntimeRequest = Effect.fnUntraced(function* (
        interaction: ZcodeRecord,
        state: ZcodeThreadState,
      ) {
        const interactionId = recordString(interaction, "interactionId");
        if (interactionId === undefined) return null;
        const kind = recordString(interaction, "kind");
        const createdAt = yield* DateTime.now;
        const turn = state.activeTurn;
        const isUserInput = kind === "userInput";
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: ZCODE_PROVIDER,
          ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
          nativeRequestId: interactionId,
        });
        return {
          id: requestId,
          nodeId: idAllocator.derive.approvalNode({ requestId }),
          providerTurnId: turn?.providerTurn.id ?? null,
          nativeRequestRef: providerRef(interactionId),
          kind: isUserInput ? "user_input" : "permission",
          status: "pending",
          responseCapability: {
            type: "live",
            providerSessionId: input.providerSessionId,
          },
          createdAt,
          resolvedAt: null,
        } satisfies OrchestrationV2RuntimeRequest;
      });

      /** Legacy reverse-channel answer (zcodePermission/UserInputResponse schema). */
      const reverseChannelAnswer = (
        isUserInput: boolean,
        response: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput | null,
      ): ZcodeRecord => {
        if (isUserInput) {
          if (response === null) return { action: "cancel" };
          return {
            action:
              response.decision === "decline" || response.decision === "cancel"
                ? "decline"
                : "accept",
            ...(response.answers !== undefined ? { content: { answers: response.answers } } : {}),
          };
        }
        if (response === null) return { decision: "deny" };
        return {
          decision:
            response.decision === undefined ||
            response.decision === "decline" ||
            response.decision === "cancel"
              ? "deny"
              : "allow",
        };
      };

      /** Settle every parked reverse-channel incarnation; zcode drops duplicates. */
      const answerParkedReverse = (interaction: ZcodeInteraction, answer: ZcodeRecord) =>
        Effect.forEach(
          [...interaction.reverseRequestIds],
          (requestId) => connection.respondReverse(requestId, answer).pipe(Effect.ignore),
          { discard: true },
        ).pipe(
          Effect.andThen(Effect.sync(() => interaction.reverseRequestIds.clear())),
          Effect.asVoid,
        );

      const projectInteractions = Effect.fnUntraced(function* (state: ZcodeThreadState) {
        const snapshot = state.snapshot;
        if (snapshot === null) return;
        const seen = new Set<string>();
        for (const interaction of snapshot.pendingInteractions) {
          const interactionId = recordString(interaction, "interactionId");
          if (interactionId === undefined) continue;
          seen.add(interactionId);
          const existing = state.interactions.get(interactionId);
          if (existing !== undefined) {
            existing.snapshotSeen = true;
            continue;
          }
          const request = yield* currentRuntimeRequest(interaction, state);
          if (request === null) continue;
          state.interactions.set(interactionId, {
            runtimeRequest: request,
            optionByDecision: optionDecisionsFor(
              recordField(interaction, "payload") as ZcodeRecord | undefined,
            ),
            reverseRequestIds: new Set(),
            snapshotSeen: true,
          });
          yield* emit({
            type: "runtime_request.updated",
            driver: ZCODE_PROVIDER,
            ...(state.providerThread.appThreadId
              ? { threadId: state.providerThread.appThreadId }
              : {}),
            runtimeRequest: request,
          });
        }
        for (const [interactionId, interaction] of state.interactions) {
          if (seen.has(interactionId) || !interaction.snapshotSeen) continue;
          state.interactions.delete(interactionId);
          // Settled on zcode's side without T3; close parked incarnations anyway
          // so the transport's parked map cannot leak (late replies are dropped).
          yield* answerParkedReverse(
            interaction,
            reverseChannelAnswer(interaction.runtimeRequest.kind === "user_input", null),
          ).pipe(Effect.ignore);
          const resolvedAt = yield* DateTime.now;
          yield* emit({
            type: "runtime_request.updated",
            driver: ZCODE_PROVIDER,
            ...(state.providerThread.appThreadId
              ? { threadId: state.providerThread.appThreadId }
              : {}),
            runtimeRequest: {
              ...interaction.runtimeRequest,
              status: "resolved",
              resolvedAt,
            },
          });
        }
      });

      // Reverse interaction requests park until T3 answers via resolveInteraction;
      // zcode re-announces them ~1/s under fresh protocol ids while they wait.
      handleReverseRequest = (
        method: string,
        params: unknown,
        protocolRequestId: string | number,
      ): Effect.Effect<unknown, ZcodeRpcError> =>
        Effect.suspend((): Effect.Effect<unknown, ZcodeRpcError> => {
          if (
            method !== ZCODE_REVERSE_METHODS.requestPermission &&
            method !== ZCODE_REVERSE_METHODS.requestUserInput
          ) {
            return Effect.undefined;
          }
          const state = threadState;
          if (state === null || closed) {
            // No thread yet: decline so the CLI is not left hanging.
            return Effect.succeed(
              method === ZCODE_REVERSE_METHODS.requestPermission
                ? { decision: "deny", reason: "No active T3 thread" }
                : { action: "decline", reason: "No active T3 thread" },
            );
          }
          const requestId = recordString(params, "requestId");
          const fallbackId = recordString(params, "toolCallId");
          // Matches the v4 projection's interactionId derivation, so a
          // snapshot-derived entry and a reverse-derived entry never double up.
          const interactionId = requestId ?? `perm-${fallbackId ?? "unknown"}`;
          const payload = params as ZcodeRecord;
          const interaction: ZcodeRecord = {
            interactionId,
            kind: method === ZCODE_REVERSE_METHODS.requestUserInput ? "userInput" : "permission",
            anchorRowId: null,
            payload,
          };
          return statePermit
            .withPermits(1)(
              Effect.gen(function* () {
                const existing = state.interactions.get(interactionId);
                if (existing !== undefined) {
                  existing.reverseRequestIds.add(protocolRequestId);
                  return ZCODE_PARK_REVERSE;
                }
                const request = yield* currentRuntimeRequest(interaction, state);
                if (request === null) return ZCODE_PARK_REVERSE;
                state.interactions.set(interactionId, {
                  runtimeRequest: request,
                  optionByDecision: optionDecisionsFor(payload),
                  reverseRequestIds: new Set([protocolRequestId]),
                  snapshotSeen: false,
                });
                yield* emit({
                  type: "runtime_request.updated",
                  driver: ZCODE_PROVIDER,
                  ...(state.providerThread.appThreadId
                    ? { threadId: state.providerThread.appThreadId }
                    : {}),
                  runtimeRequest: request,
                });
                return ZCODE_PARK_REVERSE;
              }),
            )
            .pipe(Effect.orElseSucceed(() => ZCODE_PARK_REVERSE));
        });

      // ── Turn settlement ────────────────────────────────────────────────────

      function snapshotUsage(state: ZcodeThreadState, updatedAtIso: string) {
        const usage = state.snapshot?.usage;
        if (usage === undefined) return undefined;
        const cumulative = recordObject(usage, "cumulative");
        const contextWindow = recordObject(usage, "contextWindow");
        const inputTokens = recordNumber(cumulative ?? {}, "inputTokens");
        const outputTokens = recordNumber(cumulative ?? {}, "outputTokens");
        if (inputTokens === undefined && outputTokens === undefined) return undefined;
        const cacheRead = recordNumber(cumulative ?? {}, "cacheReadTokens") ?? 0;
        const cacheWrite = recordNumber(cumulative ?? {}, "cacheWriteTokens") ?? 0;
        const maxTokens = recordNumber(contextWindow ?? {}, "maxTokens");
        return {
          usedTokens: (inputTokens ?? 0) + (outputTokens ?? 0) + cacheRead + cacheWrite,
          inputTokens: inputTokens ?? 0,
          outputTokens: outputTokens ?? 0,
          ...(maxTokens !== undefined ? { maxTokens } : {}),
          updatedAt: updatedAtIso,
        };
      }

      function snapshotLastError(state: ZcodeThreadState): {
        code: string | null;
        message: string;
      } | null {
        const control = state.snapshot?.control;
        const lastError = recordObject(control ?? {}, "lastError");
        if (lastError === undefined) return null;
        return {
          code: recordString(lastError, "code") ?? null,
          message: recordString(lastError, "message") ?? "The zcode turn failed.",
        };
      }

      function usageContextUsage(state: ZcodeThreadState): ThreadTokenUsageSnapshot | null {
        const usage = state.snapshot?.usage;
        const contextWindow = recordObject(usage ?? {}, "contextWindow");
        const used = recordNumber(contextWindow ?? {}, "usedTokens");
        const max = recordNumber(contextWindow ?? {}, "maxTokens");
        if (used === undefined || max === undefined || max <= 0) return null;
        return {
          usedTokens: used,
          maxTokens: max,
          inputTokens: used,
          outputTokens: 0,
        };
      }

      const settleTurn = Effect.fnUntraced(function* (
        state: ZcodeThreadState,
        rowState: string,
        failureOverride?: OrchestrationV2ProviderFailure,
      ) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        yield* Deferred.succeed(turn.nativeLinked, void 0).pipe(Effect.asVoid);
        const completedAt = yield* DateTime.now;
        const usage = snapshotUsage(state, DateTime.formatIso(completedAt));
        const failed = rowState === "failed";
        const providerTurn: OrchestrationV2ProviderTurn = {
          ...turn.providerTurn,
          status: failed
            ? "failed"
            : rowState === "completedInterrupted"
              ? "interrupted"
              : "completed",
          completedAt,
          ...(usage !== undefined ? { tokenUsage: usage } : {}),
        };
        yield* emit({
          type: "provider_turn.updated",
          driver: ZCODE_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn,
        });
        if (failed) {
          const lastError = snapshotLastError(state);
          yield* emit({
            type: "turn.terminal",
            driver: ZCODE_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: turn.nextItemOrdinal,
            status: "failed",
            failure:
              failureOverride ??
              makeProviderFailure({
                message: lastError?.message ?? "The zcode turn failed.",
                code: lastError?.code ?? null,
              }),
            threadDisposition: "reusable",
          } satisfies ProviderAdapter.ProviderAdapterV2Event);
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: ZCODE_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            status: rowState === "completedInterrupted" ? "interrupted" : "completed",
            failure: null,
            threadDisposition: "reusable",
          } satisfies ProviderAdapter.ProviderAdapterV2Event);
        }
        yield* updateProviderThread(state, { status: "idle" });
        yield* updateProviderSession("ready", null);
        yield* Deferred.succeed(turn.settle, void 0).pipe(Effect.asVoid);
      });

      // The capability announcement releases the wait; mutations stay under
      // statePermit and the fiber is session-scoped so close interrupts it.
      const resendTurnWithReasoningLevel = Effect.fnUntraced(function* (
        state: ZcodeThreadState,
        turn: ActiveZcodeTurn,
      ) {
        const slug = turn.turnInput.modelSelection.model;
        yield* awaitModelCapability(slug).pipe(
          Effect.timeoutOrElse({
            duration: ZCODE_REASONING_LEVEL_WAIT_TIMEOUT,
            orElse: () => Effect.void,
          }),
        );
        const selection = resolveZcodeModelSelection(turn.turnInput.modelSelection);
        const retryMode = zcodeModeForRuntimePolicy(turn.turnInput.runtimePolicy.runtimeMode);
        let retryCommandId: string | null = null;
        yield* statePermit.withPermits(1)(
          Effect.gen(function* () {
            if (state.activeTurn !== turn) return;
            if (turn.interrupted || selection?.options?.reasoningLevel === undefined) {
              yield* settleTurn(state, turn.interrupted ? "completedInterrupted" : "failed");
              return;
            }
            // Bind before the write: replay pumps the new turn's header frame
            // the moment the command lands, before this fiber could read the ack.
            retryCommandId = yield* randomUuidV4;
            turn.commandId = retryCommandId;
          }),
        );
        if (retryCommandId === null) return;
        const retryAck = yield* sendCommand(
          "sendText",
          {
            text: turn.sentText,
            requestedDelivery: "startNow",
            modelSelection: selection,
            ...(retryMode !== undefined ? { mode: retryMode } : {}),
          },
          state.nativeSessionId,
          { commandId: retryCommandId },
        ).pipe(Effect.result);
        yield* statePermit.withPermits(1)(
          Effect.gen(function* () {
            if (state.activeTurn !== turn) return;
            if (Result.isFailure(retryAck)) {
              // The resend itself failed; surface the original turn failure.
              yield* settleTurn(state, "failed");
            }
          }),
        );
      });

      // ── Row projection ─────────────────────────────────────────────────────

      const turnItemIdFor = (entityId: string): TurnItemId =>
        idAllocator.derive.turnItemFromProviderItem({
          driver: ZCODE_PROVIDER,
          nativeItemId: entityId,
        });

      const messageIdFor = (entityId: string): MessageId =>
        idAllocator.derive.messageFromProviderItem({
          driver: ZCODE_PROVIDER,
          nativeItemId: entityId,
        }) as MessageId;

      // Child nodes are adapter-owned (Codex/Claude emit them too): each
      // conversational row becomes an execution node parented at the turn's
      // root, and the message/turn-item events reference that node.
      const emitItemNode = Effect.fnUntraced(function* (
        state: ZcodeThreadState,
        turn: ActiveZcodeTurn,
        entityId: string,
        kind: "assistant_message" | "reasoning" | "tool_call",
        status: "running" | "completed",
        now: DateTime.Utc,
      ) {
        const nodeId = idAllocator.derive.nodeFromProviderItem({
          driver: ZCODE_PROVIDER,
          nativeItemId: entityId,
        });
        yield* emit({
          type: "node.updated",
          driver: ZCODE_PROVIDER,
          node: {
            id: nodeId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(entityId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: turn.providerTurn.startedAt,
            completedAt: status === "completed" ? now : null,
          } satisfies OrchestrationV2ExecutionNode,
        });
        return nodeId;
      });

      const projectRow = Effect.fnUntraced(function* (state: ZcodeThreadState, row: ZcodeRecord) {
        const kind = recordString(row, "kind");
        const entityId =
          recordString(row, "entityId") ?? String(recordNumber(row, "rowId") ?? "row");
        const turn = state.activeTurn;
        const now = yield* DateTime.now;
        switch (kind) {
          case "turnHeader": {
            const rowState = recordString(row, "state");
            const sourceCommandId = recordString(row, "sourceCommandId");
            if (turn !== null && !turn.nativeTurnLinked && sourceCommandId === turn.commandId) {
              turn.nativeTurnLinked = true;
              yield* Deferred.succeed(turn.nativeLinked, void 0).pipe(Effect.asVoid);
              turn.providerTurn = {
                ...turn.providerTurn,
                nativeTurnRef: providerRef(entityId),
                status: "running",
              };
              yield* emit({
                type: "provider_turn.updated",
                driver: ZCODE_PROVIDER,
                threadId: turn.turnInput.threadId,
                providerTurn: turn.providerTurn,
              });
            }
            if (
              turn !== null &&
              turn.nativeTurnLinked &&
              sourceCommandId === turn.commandId &&
              (rowState === "completedSuccess" ||
                rowState === "completedInterrupted" ||
                rowState === "failed")
            ) {
              // zcode announces model capabilities only after the first
              // sendText, so a reasoning model's first turn can fail demanding
              // a level we could not know. Fork a resend with the level; the
              // capability announcement lands right behind this failure.
              if (
                rowState === "failed" &&
                !turn.modelLevelRetried &&
                turn.sentWithoutReasoningLevel
              ) {
                turn.modelLevelRetried = true;
                turn.nativeTurnLinked = false;
                // Session-scoped: session close interrupts a pending resend.
                yield* resendTurnWithReasoningLevel(state, turn).pipe(Effect.forkIn(scope));
                return;
              }
              yield* settleTurn(state, rowState);
            }
            return;
          }
          case "userInput": {
            // The user message is emitted from turnInput at startTurn; skip the echo.
            return;
          }
          case "assistantText": {
            if (turn === null) return;
            const text = recordString(row, "text") ?? "";
            const rowState = recordString(row, "state");
            const streaming = rowState === "streaming";
            const messageId = messageIdFor(entityId);
            const itemNodeId = yield* emitItemNode(
              state,
              turn,
              entityId,
              "assistant_message",
              streaming ? "running" : "completed",
              now,
            );
            yield* emit({
              type: "message.updated",
              driver: ZCODE_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: itemNodeId,
                role: "assistant",
                text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: turn.startedAt,
                updatedAt: now,
              } satisfies OrchestrationV2ConversationMessage,
            });
            const ordinal = turn.itemOrdinals.get(entityId) ?? turn.nextItemOrdinal++;
            turn.itemOrdinals.set(entityId, ordinal);
            yield* emit({
              type: "turn_item.updated",
              driver: ZCODE_PROVIDER,
              turnItem: {
                id: turnItemIdFor(entityId),
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: itemNodeId,
                providerThreadId: state.providerThread.id,
                providerTurnId: turn.providerTurn.id,
                nativeItemRef: providerRef(entityId),
                parentItemId: null,
                ordinal,
                status: streaming ? "running" : "completed",
                title: null,
                startedAt: turn.providerTurn.startedAt,
                completedAt: streaming ? null : now,
                updatedAt: now,
                type: "assistant_message",
                messageId,
                text,
                streaming,
              } satisfies OrchestrationV2TurnItem,
            });
            return;
          }
          case "reasoning": {
            if (turn === null) return;
            const text = recordString(row, "text") ?? "";
            const streaming = recordString(row, "state") === "streaming";
            const reasoningNodeId = yield* emitItemNode(
              state,
              turn,
              entityId,
              "reasoning",
              streaming ? "running" : "completed",
              now,
            );
            const ordinal = turn.itemOrdinals.get(entityId) ?? turn.nextItemOrdinal++;
            turn.itemOrdinals.set(entityId, ordinal);
            yield* emit({
              type: "turn_item.updated",
              driver: ZCODE_PROVIDER,
              turnItem: {
                id: turnItemIdFor(entityId),
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: reasoningNodeId,
                providerThreadId: state.providerThread.id,
                providerTurnId: turn.providerTurn.id,
                nativeItemRef: providerRef(entityId),
                parentItemId: null,
                ordinal,
                status: streaming ? "running" : "completed",
                title: null,
                startedAt: turn.providerTurn.startedAt,
                completedAt: streaming ? null : now,
                updatedAt: now,
                type: "reasoning",
                text,
                streaming,
              } satisfies OrchestrationV2TurnItem,
            });
            return;
          }
          case "toolCall": {
            if (turn === null) return;
            const status = recordString(row, "status") ?? "running";
            const toolName = recordString(row, "toolName") ?? recordString(row, "name") ?? "tool";
            const ordinal = turn.itemOrdinals.get(entityId) ?? turn.nextItemOrdinal++;
            turn.itemOrdinals.set(entityId, ordinal);
            const input = recordField(row, "input");
            const inputText =
              typeof input === "string"
                ? input
                : input === undefined || input === null
                  ? ""
                  : (boundedJson(input, 2000) ?? "");
            const output = recordField(row, "output");
            const outputText =
              typeof output === "string"
                ? output.slice(0, 4000)
                : output === undefined || output === null
                  ? undefined
                  : boundedJson(recordField(output, "text") ?? output, 4000);
            const itemStatus =
              status === "success"
                ? ("completed" as const)
                : status === "error"
                  ? ("failed" as const)
                  : status === "cancelled"
                    ? ("cancelled" as const)
                    : ("running" as const);
            const bucket = toolKindForZcodeTool(toolName);
            const toolNodeId = yield* emitItemNode(
              state,
              turn,
              entityId,
              "tool_call",
              itemStatus === "running" ? "running" : "completed",
              now,
            );
            const base = {
              id: turnItemIdFor(entityId),
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              nodeId: toolNodeId,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurn.id,
              nativeItemRef: providerRef(entityId),
              parentItemId: null,
              ordinal,
              status: itemStatus,
              title: null,
              startedAt: turn.providerTurn.startedAt,
              completedAt: itemStatus === "running" ? null : now,
              updatedAt: now,
            };
            const item: OrchestrationV2TurnItem =
              bucket === "command"
                ? {
                    ...base,
                    type: "command_execution",
                    input: inputText,
                    ...(outputText !== undefined ? { output: outputText } : {}),
                  }
                : bucket === "file"
                  ? {
                      ...base,
                      type: "file_change",
                      fileName: parseFileNameFromToolInput(inputText) ?? toolName,
                    }
                  : bucket === "search"
                    ? {
                        ...base,
                        type: "file_search",
                        ...(inputText ? { pattern: inputText } : {}),
                      }
                    : {
                        ...base,
                        type: "notification",
                        source: { kind: "system" as const },
                        outcome:
                          itemStatus === "completed"
                            ? ("completed" as const)
                            : itemStatus === "failed"
                              ? ("failed" as const)
                              : ("updated" as const),
                        summary: toolName,
                        ...(outputText !== undefined ? { detail: outputText } : {}),
                      };
            yield* emit({
              type: "turn_item.updated",
              driver: ZCODE_PROVIDER,
              turnItem: item,
            });
            return;
          }
          case "subagent": {
            if (turn === null) return;
            const status = recordString(row, "status") ?? "running";
            const childSessionId = recordString(row, "childSessionId");
            const summary = recordString(row, "summaryText");
            yield* emit({
              type: "subagent.updated",
              driver: ZCODE_PROVIDER,
              subagent: {
                id: idAllocator.derive.nodeFromProviderItem({
                  driver: ZCODE_PROVIDER,
                  nativeItemId: `subagent:${entityId}`,
                }),
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                parentNodeId: turn.turnInput.rootNodeId,
                origin: "provider_native",
                createdBy: "agent",
                driver: ZCODE_PROVIDER,
                providerInstanceId: options.instanceId,
                providerThreadId: state.providerThread.id,
                childThreadId: null,
                nativeTaskRef: childSessionId === undefined ? null : providerRef(childSessionId),
                prompt: summary ?? "",
                title: recordString(row, "subagentType") ?? "Subagent",
                model: null,
                status:
                  status === "running" ? "running" : status === "success" ? "completed" : "failed",
                ...(summary !== undefined && status === "running" ? { progress: summary } : {}),
                result: null,
                startedAt: turn.providerTurn.startedAt,
                completedAt: status === "running" ? null : now,
                updatedAt: now,
              } satisfies OrchestrationV2Subagent,
            });
            return;
          }
          default:
            return;
        }
      });

      // ── Frame pump ─────────────────────────────────────────────────────────

      // A gap or an unreadable frame leaves our copy of the conversation stale;
      // ask for a fresh snapshot on the same subscription. Forked so the pump
      // never waits on the request; one outstanding resync per thread.
      const requestResync = Effect.fnUntraced(function* (state: ZcodeThreadState, reason: string) {
        if (state.resyncInFlight) return;
        state.resyncInFlight = true;
        yield* Effect.logWarning("zcode conversation resync", { reason });
        const topic = conversationTopic(state.nativeSessionId);
        const request: Effect.Effect<unknown, ProviderAdapter.ProviderAdapterV2Error> =
          state.subscriptionId === null
            ? subscribeTopic(state.nativeSessionId).pipe(
                Effect.tap(({ subscriptionId }) =>
                  Effect.sync(() => {
                    state.subscriptionId = subscriptionId;
                  }),
                ),
              )
            : connection
                .request(
                  ZCODE_METHODS.conversationResync,
                  {
                    topic,
                    connectionId: `t3-${options.instanceId}`,
                    subscriptionId: state.subscriptionId,
                    base: null,
                    forceSnapshot: true,
                  },
                  ZCODE_REQUEST_TIMEOUT_MS,
                )
                .pipe(Effect.mapError(() => protocolError("v4/conversation/resync failed")));
        yield* request.pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              state.resyncInFlight = false;
              yield* Effect.logWarning("zcode conversation resync failed", {
                error: error._tag,
              });
            }),
          ),
          Effect.forkIn(scope),
        );
      });

      // A snapshot can carry the active turn's rows, including its terminal
      // header (e.g. after a resync); project only that turn's rows, header last.
      const projectActiveTurnFromSnapshot = Effect.fnUntraced(function* (state: ZcodeThreadState) {
        const turn = state.activeTurn;
        const rows = state.snapshot?.rows.window ?? [];
        if (turn === null || rows.length === 0) return;
        const header = rows.findLast(
          (row) =>
            recordString(row, "kind") === "turnHeader" &&
            recordString(row, "sourceCommandId") === turn.commandId,
        );
        const nativeTurnId = header === undefined ? undefined : recordString(header, "turnId");
        if (header === undefined || nativeTurnId === undefined) return;
        for (const row of rows) {
          if (row === header || recordString(row, "turnId") !== nativeTurnId) continue;
          if (recordString(row, "kind") === "turnHeader") continue;
          yield* projectRow(state, row);
        }
        yield* projectRow(state, header);
      });

      const applyFrame = Effect.fnUntraced(function* (topicFrame: {
        readonly topic: string;
        readonly fromSeq?: number;
        readonly toSeq?: number;
        readonly payload: unknown;
      }) {
        const payload = topicFrame.payload as ZcodeRecord;
        const snapshotWaiter = pendingSnapshots.get(topicFrame.topic);
        if (snapshotWaiter !== undefined && recordString(payload, "kind") === "snapshot") {
          const waitedSnapshot = recordField(payload, "snapshot") as ZcodeRecord | undefined;
          if (waitedSnapshot !== undefined) {
            pendingSnapshots.delete(topicFrame.topic);
            yield* Deferred.succeed(snapshotWaiter, readSnapshotState(waitedSnapshot));
          }
        }
        const state = threadState;
        if (state === null || topicFrame.topic !== conversationTopic(state.nativeSessionId)) {
          return;
        }
        const toSeq = typeof topicFrame.toSeq === "number" ? topicFrame.toSeq : undefined;
        if (recordString(payload, "kind") === "snapshot") {
          const snapshot = recordField(payload, "snapshot") as ZcodeRecord | undefined;
          state.snapshot = snapshot === undefined ? null : readSnapshotState(snapshot);
          state.frameSeq = toSeq ?? null;
          state.resyncInFlight = false;
          yield* projectActiveTurnFromSnapshot(state);
        } else if (recordString(payload, "kind") === "deltas") {
          const fromSeq = typeof topicFrame.fromSeq === "number" ? topicFrame.fromSeq : undefined;
          if (state.frameSeq !== null && fromSeq !== undefined && toSeq !== undefined) {
            // Already applied (a replay overlapping what we have).
            if (toSeq <= state.frameSeq) return;
            if (fromSeq !== state.frameSeq) {
              yield* requestResync(state, "sequence gap");
              return;
            }
          }
          if (toSeq !== undefined) state.frameSeq = toSeq;
          let current = state.snapshot ?? {
            ...EMPTY_THREAD_SNAPSHOT,
            sessionId: state.nativeSessionId,
          };
          for (const delta of recordArray(payload, "deltas") as ZcodeRecord[]) {
            const op = recordString(delta, "op");
            if (op === undefined) continue;
            current = applyConversationDelta(current, { ...delta, op });
            if (op === "row.appended" || op === "row.upserted") {
              const row = recordField(delta, "row") as ZcodeRecord | undefined;
              if (row !== undefined) {
                yield* projectRow(state, row);
              }
            } else if (op === "row.delta") {
              const rowId = recordNumber(delta, "rowId");
              const streamingRow = current.rows.window.findLast(
                (candidate) => recordNumber(candidate, "rowId") === rowId,
              );
              if (streamingRow !== undefined) {
                yield* projectRow(state, streamingRow);
              }
            }
          }
          state.snapshot = current;
        }
        yield* projectInteractions(state).pipe(Effect.ignore);
        yield* updateProviderThread(state, {
          contextUsage: usageContextUsage(state),
        }).pipe(Effect.ignore);
      });

      const rememberModelsFromStateUpdated = (
        params: unknown,
        capabilityReleases: Array<Deferred.Deferred<void>>,
      ): ZcodeModelDiscoveryCache => {
        const announced: Record<string, ZcodeDiscoveredModelInfo> = {};
        const patch = recordObject(params, "patch") ?? (params as ZcodeRecord);
        const model = recordObject(patch, "model");
        if (model === undefined) return announced;
        for (const option of recordArray(model, "available")) {
          const ref = recordObject(option, "ref");
          const providerId = recordString(ref ?? {}, "providerId");
          const modelId = recordString(ref ?? {}, "modelId");
          if (providerId === undefined || modelId === undefined) continue;
          const slug = `${providerId}/${modelId}`;
          const reasoning = recordObject(option, "reasoning");
          const levelValues = recordArray(reasoning ?? {}, "levels")
            .map((level) => recordString(level as ZcodeRecord, "value"))
            .filter((value): value is string => value !== undefined);
          const contextWindow = recordNumber(option, "contextWindow");
          const defaultLevel = recordString(reasoning ?? {}, "defaultLevel");
          const info: ZcodeDiscoveredModelInfo = {
            ...(contextWindow !== undefined ? { contextWindow } : {}),
            ...(levelValues.length > 0 ? { levels: levelValues } : {}),
            ...(defaultLevel !== undefined ? { defaultLevel } : {}),
          };
          modelInfo.set(slug, info);
          announced[slug] = info;
          const waiters = modelCapabilityWaiters.get(slug);
          if (waiters !== undefined) {
            modelCapabilityWaiters.delete(slug);
            capabilityReleases.push(...waiters);
          }
        }
        return announced;
      };

      // state.updated can burst, so announcements coalesce into one cache
      // flush per burst, forked off the frame pump into the session scope —
      // file I/O never delays frame handling or the turn that taught us the
      // models.
      const pendingDiscovery: Record<string, ZcodeDiscoveredModelInfo> = {};
      let discoveryFlushQueued = false;
      const scheduleDiscoveryCacheWrite = Effect.fn("ZcodeAdapter.scheduleDiscoveryCacheWrite")(
        function* (announced: ZcodeModelDiscoveryCache) {
          Object.assign(pendingDiscovery, announced);
          if (discoveryFlushQueued) return;
          discoveryFlushQueued = true;
          yield* Effect.gen(function* () {
            discoveryFlushQueued = false;
            const batch: ZcodeModelDiscoveryCache = { ...pendingDiscovery };
            for (const slug of Object.keys(pendingDiscovery)) delete pendingDiscovery[slug];
            yield* writeZcodeModelDiscoveryCache(
              host.paths.providerStatusCacheDir,
              batch,
              discoveryInstanceState.discoveryCacheScope,
            ).pipe(
              Effect.provideService(FileSystem.FileSystem, discoveryFileSystem),
              Effect.provideService(Path.Path, discoveryPath),
              Effect.catchCause((cause) =>
                Effect.logWarning("zcode model discovery cache write failed", {
                  cause: String(cause).slice(0, 200),
                }),
              ),
            );
          }).pipe(Effect.forkIn(scope));
        },
      );

      // The notifications queue only ends when the transport does. Unless that
      // was a respawn or session close, the process died under us: fail the
      // turn and the session like Pi does instead of leaving them running.
      const handleTransportLoss = Effect.fnUntraced(function* (
        conn: ZcodeRpcConnection,
        cause: Cause.Cause<unknown>,
      ): Effect.fn.Return<void> {
        if (closed || retiredConnections.has(conn) || conn !== connection) return;
        if (Cause.hasInterruptsOnly(cause)) return;
        yield* Effect.logWarning("zcode app-server transport lost", {
          cause: String(cause).slice(0, 300),
        });
        yield* statePermit.withPermits(1)(
          Effect.gen(function* () {
            const state = threadState;
            const turn = state?.activeTurn ?? null;
            if (state !== null && turn !== null) {
              yield* turn.interrupted
                ? settleTurn(state, "completedInterrupted")
                : settleTurn(
                    state,
                    "failed",
                    makeProviderFailure({
                      cause,
                      message: ZCODE_TRANSPORT_LOST_MESSAGE,
                      class: "transport_error",
                    }),
                  );
            }
            yield* updateProviderSession("error", ZCODE_TRANSPORT_LOST_MESSAGE);
          }),
        );
        yield* Queue.fail(
          events,
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver: ZCODE_PROVIDER,
            providerSessionId: input.providerSessionId,
            cause,
          }),
        );
      });

      // One pump per connection incarnation; a respawned process gets its
      // own reassembler so fragments staged by a dead process cannot leak
      // into the fresh one's frames.
      const forkNotificationPump = Effect.fnUntraced(function* (conn: ZcodeRpcConnection) {
        const reassembler = new WireFrameReassembler();
        yield* Stream.fromQueue(conn.notifications).pipe(
          Stream.mapEffect((notification) =>
            Effect.gen(function* () {
              if (notification.method === ZCODE_NOTIFICATIONS.conversationFrame) {
                if (!isZcodeWireFrame(notification.params)) {
                  yield* Effect.logWarning("zcode frame rejected", {
                    reasonCode: "proto.frameAssemblyInvalidPayload",
                  });
                  return;
                }
                const outcome = reassembler.push(notification.params);
                if (outcome.kind === "frame") {
                  yield* applyFrame(outcome.frame).pipe(statePermit.withPermits(1));
                } else if (outcome.kind === "rejected") {
                  yield* Effect.logWarning("zcode frame rejected", {
                    reasonCode: outcome.reasonCode,
                  });
                  const rejectedTopic = notification.params.topic;
                  yield* statePermit.withPermits(1)(
                    Effect.suspend(() => {
                      const state = threadState;
                      return state !== null &&
                        rejectedTopic === conversationTopic(state.nativeSessionId)
                        ? requestResync(state, outcome.reasonCode)
                        : Effect.void;
                    }),
                  );
                }
                return;
              }
              if (notification.method === ZCODE_NOTIFICATIONS.stateUpdated) {
                const capabilityReleases: Array<Deferred.Deferred<void>> = [];
                const announced = rememberModelsFromStateUpdated(
                  notification.params,
                  capabilityReleases,
                );
                yield* Effect.forEach(
                  capabilityReleases,
                  (deferred) => Deferred.succeed(deferred, void 0),
                  {
                    discard: true,
                  },
                );
                if (Object.keys(announced).length > 0) {
                  // Persist the discovery so the status probe can enrich the
                  // config-derived list before any session exists.
                  yield* scheduleDiscoveryCacheWrite(announced);
                }
                return;
              }
              // session/lifecycle/changed and telemetry are informational only.
            }).pipe(
              // A handler bug must not end the pump; only transport loss does.
              Effect.catchDefect((defect) =>
                Effect.logWarning("zcode notification handling failed", {
                  defect: String(defect).slice(0, 300),
                }),
              ),
            ),
          ),
          Stream.runDrain,
          Effect.exit,
          Effect.flatMap((exit) =>
            handleTransportLoss(conn, exit._tag === "Failure" ? exit.cause : Cause.empty),
          ),
          Effect.forkIn(scope),
        );
      });

      yield* forkNotificationPump(connection);

      // A born-broken process never registers account models; replacing it is
      // the only cure. Completed conversations re-hydrate on re-subscribe; a
      // never-used draft exists only in the dead process, so its refusal
      // re-binds the thread to a fresh draft instead of wedging it.
      const respawnZcodeProcess = Effect.fnUntraced(function* (
        reason: string,
      ): Effect.fn.Return<void, ProviderAdapter.ProviderAdapterV2Error> {
        yield* Effect.logWarning("zcode app-server respawn", {
          reason,
          providerSessionId: input.providerSessionId,
        });
        const state = threadState;
        yield* Effect.gen(function* () {
          retiredConnections.add(connection);
          yield* connection.terminate;
          if (state !== null) {
            state.snapshot = null;
            state.frameSeq = null;
            state.resyncInFlight = false;
          }
          connection = yield* spawnZcodeConnection();
          yield* forkNotificationPump(connection);
          if (state === null) return;
          const resubscribe = yield* subscribeTopic(state.nativeSessionId).pipe(Effect.result);
          if (Result.isSuccess(resubscribe)) {
            state.subscriptionId = resubscribe.success.subscriptionId;
            return;
          }
          // Only a never-used draft may be replaced: a conversation that
          // already ran owes its history, so that failure surfaces to the
          // caller instead of silently discarding turns.
          if (state.providerThread.firstRunOrdinal !== null) {
            return yield* resubscribe.failure;
          }
          const nativeSessionId = yield* createDraftSession(cwd);
          const { subscriptionId } = yield* subscribeTopic(nativeSessionId);
          state.nativeSessionId = nativeSessionId;
          state.subscriptionId = subscriptionId;
          const updatedAt = yield* DateTime.now;
          state.providerThread = {
            ...state.providerThread,
            nativeThreadRef: providerRef(nativeSessionId),
            updatedAt,
          };
          yield* emit({
            type: "provider_thread.updated",
            driver: ZCODE_PROVIDER,
            providerThread: state.providerThread,
          });
        }).pipe(Semaphore.withPermits(respawnPermit, 1));
      });

      // ── Model / mode application ───────────────────────────────────────────

      const resolveZcodeModelSelection = (
        selection: ModelSelection,
      ): ZcodeModelSelection | null => {
        if (selection.model === ZCODE_INHERIT_MODEL_SLUG) return null;
        const parsed = parseZcodeModelSlug(selection.model);
        if (parsed === null) return null;
        const info = modelInfo.get(selection.model);
        // The Effort picker's choice wins over the discovered default; before
        // discovery, fall back to the default, then the first announced level.
        const reasoningLevel =
          getModelSelectionStringOptionValue(selection, ZCODE_EFFORT_OPTION_ID) ??
          info?.defaultLevel ??
          (info?.levels !== undefined && info.levels.length > 0 ? info.levels[0] : undefined);
        return {
          providerId: parsed.providerId,
          modelId: parsed.modelId,
          ...(reasoningLevel !== undefined ? { options: { reasoningLevel } } : {}),
        };
      };

      const applySessionModel = Effect.fnUntraced(function* (
        state: ZcodeThreadState,
        selection: ModelSelection,
      ) {
        if (selection.model === sessionEntity.model) return;
        const zcodeSelection = resolveZcodeModelSelection(selection);
        if (zcodeSelection === null) return;
        const snapshot = state.snapshot;
        // A rejected switch fails the caller (turn start, thread registration)
        // instead of leaving the session claiming a model zcode never applied.
        yield* sendCommand(
          "switchModelConfig",
          {
            provider: zcodeSelection.providerId,
            model: zcodeSelection.modelId,
            ...(zcodeSelection.options?.reasoningLevel !== undefined
              ? { thought: zcodeSelection.options.reasoningLevel }
              : {}),
          },
          state.nativeSessionId,
          {
            ...(snapshot?.revision !== undefined ? { baseRevision: snapshot.revision } : {}),
            ...(snapshot?.logEpoch ? { baseLogEpoch: snapshot.logEpoch } : {}),
          },
        );
        const updatedAt = yield* DateTime.now;
        sessionEntity = { ...sessionEntity, model: selection.model, updatedAt };
        yield* emit({
          type: "provider_session.updated",
          driver: ZCODE_PROVIDER,
          providerSession: sessionEntity,
        });
      });

      function zcodeModeForRuntimePolicy(runtimeMode: string | undefined): string | undefined {
        if (runtimeMode === undefined) return undefined;
        return ZCODE_RUNTIME_MODE_BY_T3_MODE[runtimeMode];
      }

      // ── Thread registration ────────────────────────────────────────────────

      const createDraftSession = Effect.fnUntraced(function* (
        workspaceId: string,
      ): Effect.fn.Return<string, ProviderAdapter.ProviderAdapterV2Error> {
        const ack = yield* sendCommand("createSession", { workspaceId }, null).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(ZCODE_CREATE_SESSION_TIMEOUT_MS),
            orElse: () => protocolError("v4/command createSession timed out"),
          }),
        );
        const result = recordObject(ack, "result") ?? {};
        const sessionId = recordString(result, "sessionId");
        if (sessionId === undefined) {
          return yield* protocolError("createSession ACK missing sessionId");
        }
        return sessionId;
      });

      // Reads a persisted session's snapshot through a short-lived subscription.
      // The waiter is registered before the request: the frame follows the ack.
      const loadConversationSnapshot = Effect.fnUntraced(function* (
        sessionId: string,
      ): Effect.fn.Return<ZcodeConversationState, ProviderAdapter.ProviderAdapterV2Error> {
        const topic = conversationTopic(sessionId);
        const inflight = pendingSnapshots.get(topic);
        const waiter = inflight ?? (yield* Deferred.make<ZcodeConversationState>());
        if (inflight === undefined) pendingSnapshots.set(topic, waiter);
        // One deadline bounds the subscribe and the frame, so a hung subscribe
        // reports the same timeout as a missing snapshot.
        return yield* Effect.gen(function* () {
          if (inflight !== undefined) return yield* Deferred.await(waiter);
          const { subscriptionId } = yield* subscribeTopic(sessionId).pipe(
            Effect.onError(() => Effect.sync(() => pendingSnapshots.delete(topic))),
          );
          const snapshot = yield* Deferred.await(waiter);
          if (subscriptionId !== null) {
            yield* connection
              .request(
                ZCODE_METHODS.conversationUnsubscribe,
                { topic, subscriptionId, connectionId: `t3-${options.instanceId}` },
                ZCODE_REQUEST_TIMEOUT_MS,
              )
              .pipe(Effect.ignore, Effect.forkIn(scope));
          }
          return snapshot;
        }).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(ZCODE_REQUEST_TIMEOUT_MS),
            orElse: () => {
              if (pendingSnapshots.get(topic) === waiter) pendingSnapshots.delete(topic);
              return protocolError(`zcode conversation ${sessionId} snapshot timed out`);
            },
          }),
        );
      });

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ): Effect.fn.Return<OrchestrationV2ProviderThread, ProviderAdapter.ProviderAdapterV2Error> {
        if (threadState !== null) {
          // One provider thread per zcode process (like Pi); reuse when the
          // orchestrator re-ensures the same native thread.
          const nativeId = threadState.providerThread.nativeThreadRef?.nativeId ?? null;
          const requested = threadInput.existingProviderThread?.nativeThreadRef?.nativeId ?? null;
          if (nativeId === null || requested === null || nativeId === requested) {
            return threadState.providerThread;
          }
          return yield* protocolError("This zcode session already hosts a different native thread");
        }
        const existingNativeId =
          threadInput.existingProviderThread?.nativeThreadRef?.nativeId ?? null;
        const now = yield* DateTime.now;
        const nativeSessionId = existingNativeId ?? (yield* createDraftSession(cwd));
        const { subscriptionId } = yield* subscribeTopic(nativeSessionId);
        const providerThread: OrchestrationV2ProviderThread = {
          id: idAllocator.derive.providerThread({
            driver: ZCODE_PROVIDER,
            nativeThreadId: nativeSessionId,
          }),
          driver: ZCODE_PROVIDER,
          providerInstanceId: options.instanceId,
          providerSessionId: input.providerSessionId,
          appThreadId: threadInput.threadId,
          ownerNodeId: null,
          nativeThreadRef: providerRef(nativeSessionId),
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        };
        threadState = {
          nativeSessionId,
          subscriptionId,
          providerThread,
          snapshot: null,
          frameSeq: null,
          resyncInFlight: false,
          activeTurn: null,
          interactions: new Map(),
        };
        yield* applySessionModel(threadState, threadInput.modelSelection);
        yield* emit({
          type: "provider_thread.updated",
          driver: ZCODE_PROVIDER,
          providerThread,
        });
        return providerThread;
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: ZCODE_DRIVER_KIND,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        hasPendingBackgroundWork: Effect.sync(
          () =>
            threadState !== null &&
            (activeWorksCount(threadState) > 0 ||
              recordArray(threadState.snapshot ?? {}, "backgroundWorks").length > 0),
        ),
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          return modelInfo.get(selection.model)?.contextWindow;
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: ZCODE_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: ZCODE_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: Effect.fn("ZcodeAdapter.startTurn")(function* (turnInput) {
          const state = threadState;
          if (state === null) {
            return yield* protocolError("zcode session has no registered thread");
          }
          if (state.activeTurn !== null) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: ZCODE_PROVIDER,
              detail: `zcode provider thread ${turnInput.providerThread.id} already has an active turn`,
              reason: "previous_turn_active",
            });
          }
          yield* applySessionModel(state, turnInput.modelSelection);
          const text = yield* providerMessageTextWithAttachmentPaths({
            text: turnInput.message.text,
            attachments: turnInput.message.attachments,
            resolveAttachmentPath: host.resolveAttachmentPath,
            resolveExtractedTextPath: host.resolveExtractedTextPath,
          });
          const startedAt = yield* DateTime.now;
          const syntheticTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: idAllocator.derive.providerTurn({
              driver: ZCODE_PROVIDER,
              nativeTurnId: syntheticTurnId,
            }),
            providerThreadId: turnInput.providerThread.id,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: providerRef(syntheticTurnId, "weak"),
            ordinal: turnInput.providerTurnOrdinal,
            status: "running",
            startedAt,
            completedAt: null,
          };
          const settle = yield* Deferred.make<void, ProviderAdapter.ProviderAdapterV2Error>();
          const nativeLinked = yield* Deferred.make<void>();
          const activeTurn: ActiveZcodeTurn = {
            turnInput,
            providerTurn,
            startedAt,
            commandId: "",
            nativeTurnLinked: false,
            nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
            itemOrdinals: new Map(),
            interrupted: false,
            failure: null,
            settle,
            sentText: text,
            sentWithoutReasoningLevel: false,
            modelLevelRetried: false,
            usageEmitted: false,
            nativeLinked,
          };
          const provisionalCommandId = yield* randomUuidV4;
          activeTurn.commandId = provisionalCommandId;
          state.providerThread = turnInput.providerThread;
          state.activeTurn = activeTurn;
          const zcodeMode = zcodeModeForRuntimePolicy(turnInput.runtimePolicy.runtimeMode);
          let modelSelection = resolveZcodeModelSelection(turnInput.modelSelection);
          activeTurn.sentWithoutReasoningLevel =
            modelSelection !== null && modelSelection.options?.reasoningLevel === undefined;
          const sendTurnText = Effect.fnUntraced(function* () {
            yield* sendCommand(
              "sendText",
              {
                text,
                requestedDelivery: "startNow",
                ...(modelSelection !== null ? { modelSelection } : {}),
                ...(zcodeMode !== undefined ? { mode: zcodeMode } : {}),
              },
              state.nativeSessionId,
              // The turn is bound to this id up front so header frames link even
              // when they arrive before this fiber reads the ack.
              { commandId: activeTurn.commandId },
            );
          });
          // An account-model sendText rejected modelUnavailable means the
          // app-server process was born without the account registry. Replace
          // the process once and resend on the fresh one; a second
          // modelUnavailable fails the turn like any other send error.
          const sendTurnTextWithRespawn = Effect.fnUntraced(function* (): Effect.fn.Return<
            void,
            ProviderAdapter.ProviderAdapterV2Error
          > {
            const first = yield* sendTurnText().pipe(Effect.result);
            if (Result.isSuccess(first)) return;
            if (!isAccountModelUnavailable(first.failure, turnInput.modelSelection.model)) {
              return yield* first.failure;
            }
            yield* respawnZcodeProcess("account model unavailable on sendText");
            // The fresh process re-announces model capabilities; re-resolve so
            // the retry can carry a reasoning level.
            modelSelection = resolveZcodeModelSelection(turnInput.modelSelection);
            activeTurn.sentWithoutReasoningLevel =
              modelSelection !== null && modelSelection.options?.reasoningLevel === undefined;
            const retryCommandId = yield* randomUuidV4;
            // Bind before the write: the retried turn's header frames link to
            // the new command id even when they arrive before the ack is read.
            activeTurn.commandId = retryCommandId;
            yield* sendTurnText();
          });
          yield* sendTurnTextWithRespawn().pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                if (state.activeTurn === activeTurn) state.activeTurn = null;
              }),
            ),
          );
          // The orchestrator owns user message items (it knows whether a
          // message started its turn or was queued); zcode echoes the text as
          // a userInput row we skip in projectRow.
          yield* emit({
            type: "provider_turn.updated",
            driver: ZCODE_PROVIDER,
            threadId: turnInput.threadId,
            providerTurn,
          });
          yield* updateProviderThread(state, {
            status: "active",
            firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateProviderSession("running", null);
        }),
        steerTurn: Effect.fn("ZcodeAdapter.steerTurn")(function* (steerInput) {
          const state = threadState;
          if (state === null || state.activeTurn === null) {
            return yield* protocolError("zcode steering requires an active turn");
          }
          const text = yield* providerMessageTextWithAttachmentPaths({
            text: steerInput.message.text,
            attachments: steerInput.message.attachments,
            resolveAttachmentPath: host.resolveAttachmentPath,
            resolveExtractedTextPath: host.resolveExtractedTextPath,
          });
          // During a fresh session's first-turn bootstrap the queued guide
          // would run level-less and fail; hold it until the model capability
          // announcement lands and the turn linked natively (or until the turn
          // ends or the wait times out). Deferreds keep this TestClock-safe.
          const steerTarget = state.activeTurn;
          yield* Effect.all([
            awaitModelCapability(steerTarget.turnInput.modelSelection.model),
            Deferred.await(steerTarget.nativeLinked),
          ]).pipe(
            Effect.timeoutOrElse({
              duration: ZCODE_STEER_CAPABILITY_WAIT_TIMEOUT,
              orElse: () => Effect.void,
            }),
          );
          yield* statePermit.withPermits(1)(
            Effect.gen(function* () {
              if (state.activeTurn !== steerTarget) {
                return yield* protocolError("zcode steering target turn ended");
              }
            }),
          );
          // The send takes the respawn permit and re-reads the session id
          // inside: a steer that lands while the app-server is being replaced
          // waits for the fresh process (and any re-bound draft session)
          // instead of dying against the terminated transport.
          yield* Effect.suspend(() =>
            sendCommand("sendText", { text, requestedDelivery: "guide" }, state.nativeSessionId),
          ).pipe(Semaphore.withPermits(respawnPermit, 1));
        }),
        interruptTurn: Effect.fn("ZcodeAdapter.interruptTurn")(function* (interruptInput) {
          void interruptInput;
          const state = threadState;
          if (state === null) {
            return yield* protocolError("zcode session has no registered thread");
          }
          const turn = state.activeTurn;
          if (turn === null) return;
          turn.interrupted = true;
          const control = state.snapshot?.control;
          let foregroundExecutionId: string | undefined;
          for (const work of recordArray(control ?? {}, "activeWorks")) {
            const id = recordString(work as ZcodeRecord, "foregroundExecutionId");
            if (id !== undefined) {
              foregroundExecutionId = id;
              break;
            }
          }
          yield* sendCommand(
            "stop",
            foregroundExecutionId !== undefined
              ? { expectedForegroundExecutionId: foregroundExecutionId }
              : {},
            state.nativeSessionId,
          ).pipe(
            Effect.catch(() =>
              // v4 stop is a no-op if the target changed; session/stop always aborts.
              connection
                .request(
                  ZCODE_METHODS.sessionStop,
                  { sessionId: state.nativeSessionId },
                  ZCODE_REQUEST_TIMEOUT_MS,
                )
                .pipe(
                  Effect.mapError(() => protocolError("zcode interrupt failed")),
                  Effect.asVoid,
                ),
            ),
          );
          // Wait for the turnHeader terminal row; force-abort on timeout.
          yield* Deferred.await(turn.settle).pipe(
            Effect.timeout(ZCODE_INTERRUPT_SETTLE_TIMEOUT_MS),
            Effect.catch(() =>
              Effect.gen(function* () {
                if (state.activeTurn === turn) {
                  yield* settleTurn(
                    state,
                    turn.interrupted ? "completedInterrupted" : "completedSuccess",
                  );
                }
              }),
            ),
            Effect.asVoid,
          );
        }),
        compactThread: Effect.fn("ZcodeAdapter.compactThread")(() =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("zcode session has no registered thread");
            }
            yield* sendCommand("compact", {}, state.nativeSessionId);
          }),
        ),
        respondToRuntimeRequest: Effect.fn("ZcodeAdapter.respondToRuntimeRequest")(
          function* (responseInput) {
            const state = threadState;
            if (state === null) {
              return yield* new ProviderAdapter.ProviderAdapterSessionStoppedError({
                driver: ZCODE_PROVIDER,
              });
            }
            const interaction = [...state.interactions.values()].find(
              (candidate) => candidate.runtimeRequest.id === responseInput.requestId,
            );
            if (interaction === undefined) {
              return yield* new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                driver: ZCODE_PROVIDER,
                requestId: responseInput.requestId,
              });
            }
            const interactionId = interaction.runtimeRequest.nativeRequestRef?.nativeId;
            if (interactionId === undefined || interactionId === null) {
              return yield* protocolError("zcode runtime request has no interaction id");
            }
            const isUserInput = interaction.runtimeRequest.kind === "user_input";
            const answer: ZcodeRecord = isUserInput
              ? {
                  action:
                    responseInput.decision === "decline" || responseInput.decision === "cancel"
                      ? "decline"
                      : "accept",
                  ...(responseInput.answers !== undefined
                    ? { content: { answers: responseInput.answers } }
                    : {}),
                }
              : (() => {
                  const decision = responseInput.decision ?? "decline";
                  const optionId =
                    interaction.optionByDecision.get(decision) ??
                    (decision === "decline" || decision === "cancel" ? "deny" : "allowOnce");
                  return { optionId };
                })();
            // Send the v4 answer first: it wins zcode's race against the parked
            // reverse channel and carries full-access/session semantics. Only a
            // settled interaction may report resolved — a failed send keeps the
            // entry pending and surfaces the error to the caller.
            yield* sendCommand(
              "resolveInteraction",
              { interactionId, answer },
              state.nativeSessionId,
            ).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: ZCODE_PROVIDER,
                    requestId: responseInput.requestId,
                    cause,
                  }),
              ),
            );
            // The v4 answer already left; the tail only touches interaction
            // state and outbound queues, so it can take statePermit without
            // deadlocking against the frame pump or the reverse handler.
            yield* statePermit.withPermits(1)(
              Effect.gen(function* () {
                yield* answerParkedReverse(
                  interaction,
                  reverseChannelAnswer(isUserInput, responseInput),
                ).pipe(Effect.ignore);
                const resolvedAt = yield* DateTime.now;
                const resolved: OrchestrationV2RuntimeRequest = {
                  ...interaction.runtimeRequest,
                  status: "resolved",
                  resolvedAt,
                  ...(responseInput.decision !== undefined
                    ? { decision: responseInput.decision }
                    : {}),
                  ...(responseInput.answers !== undefined
                    ? { answers: responseInput.answers }
                    : {}),
                };
                state.interactions.delete(interactionId);
                yield* emit({
                  type: "runtime_request.updated",
                  driver: ZCODE_PROVIDER,
                  ...(state.providerThread.appThreadId
                    ? { threadId: state.providerThread.appThreadId }
                    : {}),
                  runtimeRequest: resolved,
                });
              }),
            );
          },
        ),
        readThreadSnapshot: Effect.fn("ZcodeAdapter.readThreadSnapshot")(function* (snapshotInput) {
          const state = threadState;
          if (state === null) {
            return yield* protocolError("zcode session has no registered thread");
          }
          const snapshot = state.snapshot;
          const messages: OrchestrationV2ConversationMessage[] = [];
          const now = yield* DateTime.now;
          const fallbackThread = (state.providerThread.appThreadId ?? input.threadId) as ThreadId;
          for (const [index, row] of (snapshot?.rows.window ?? []).entries()) {
            const kind = recordString(row, "kind");
            if (kind !== "userInput" && kind !== "assistantText") continue;
            const entityId =
              recordString(row, "entityId") ?? String(recordNumber(row, "rowId") ?? index);
            const createdAtMillis = recordNumber(row, "createdAt");
            const createdAt =
              createdAtMillis === undefined ? now : DateTime.makeUnsafe(createdAtMillis);
            messages.push({
              id: messageIdFor(entityId),
              threadId: fallbackThread,
              runId: null,
              nodeId: null,
              role: kind === "userInput" ? "user" : "assistant",
              text: recordString(row, "text") ?? "",
              attachments: [],
              streaming: false,
              createdBy: kind === "userInput" ? "user" : "agent",
              creationSource: "provider",
              createdAt,
              updatedAt: createdAt,
            } satisfies OrchestrationV2ConversationMessage);
          }
          return {
            providerThread: snapshotInput.providerThread,
            providerTurns: state.activeTurn === null ? [] : [state.activeTurn.providerTurn],
            messages,
            runtimeRequests: [...state.interactions.values()].map(
              (interaction) => interaction.runtimeRequest,
            ),
          };
        }),
        rollbackThread: (rollbackInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: ZCODE_PROVIDER,
              providerThreadId: rollbackInput.providerThread.id,
              cause: protocolError(
                "zcode conversation rollback is not supported; use file rewind checkpoints",
              ),
            }),
          ),
        forkThread: Effect.fn("ZcodeAdapter.forkThread")(function* (forkInput) {
          // The orchestrator forks on a fresh session for the target thread, so the
          // source conversation comes from the input, not from threadState.
          const sourceSessionId = forkInput.sourceProviderThread.nativeThreadRef?.nativeId;
          if (sourceSessionId === undefined || sourceSessionId === null) {
            return yield* protocolError("zcode fork source thread has no native session reference");
          }
          if (threadState?.activeTurn != null) {
            return yield* protocolError("zcode session has an active turn; cannot fork");
          }
          const hosted = threadState;
          const snapshot =
            hosted !== null &&
            hosted.nativeSessionId === sourceSessionId &&
            hosted.snapshot !== null
              ? hosted.snapshot
              : yield* loadConversationSnapshot(sourceSessionId);
          // T3 turn ids never equal zcode turnHeader entityIds; resolve the
          // requested turn's native id through the link the turnHeader row
          // established, then target that row (latest only when unspecified).
          const turnHeaders = snapshot.rows.window.filter(
            (row) => recordString(row, "kind") === "turnHeader",
          );
          let targetRow: ZcodeRecord | undefined;
          if (forkInput.providerTurnId !== undefined) {
            const requested = forkInput.sourceProviderTurns?.find(
              (turn) =>
                turn.id === forkInput.providerTurnId &&
                turn.providerThreadId === forkInput.sourceProviderThread.id,
            );
            if (requested === undefined) {
              return yield* protocolError(
                `zcode fork target turn ${forkInput.providerTurnId} was not found among the source provider turns`,
              );
            }
            const ref = requested.nativeTurnRef;
            const nativeTurnId =
              ref !== undefined && ref !== null && ref.strength === "strong"
                ? ref.nativeId
                : undefined;
            if (nativeTurnId === undefined || nativeTurnId === null) {
              return yield* protocolError(
                `zcode fork target turn ${forkInput.providerTurnId} has no native turn reference`,
              );
            }
            targetRow = turnHeaders.findLast(
              (row) => recordString(row, "entityId") === nativeTurnId,
            );
            if (targetRow === undefined) {
              return yield* protocolError(
                `zcode fork target turn ${forkInput.providerTurnId} is not in the loaded conversation snapshot`,
              );
            }
          } else {
            targetRow = turnHeaders[turnHeaders.length - 1];
          }
          if (targetRow === undefined) {
            return yield* protocolError("zcode fork found no turn to fork from");
          }
          const rowId = recordNumber(targetRow, "rowId");
          const entityId = recordString(targetRow, "entityId");
          if (rowId === undefined || entityId === undefined) {
            return yield* protocolError("zcode fork target row is malformed");
          }
          const ack = yield* sendCommand(
            "forkAssistant",
            { target: { rowId, entityId } },
            sourceSessionId,
            {
              baseRevision: snapshot.revision,
              ...(snapshot.logEpoch ? { baseLogEpoch: snapshot.logEpoch } : {}),
            },
          );
          const result = recordObject(ack, "result") ?? {};
          const forkedSessionId = recordString(result, "sessionId");
          if (forkedSessionId === undefined) {
            return yield* protocolError("zcode forkAssistant ACK missing sessionId");
          }
          const { subscriptionId } = yield* subscribeTopic(forkedSessionId);
          const now = yield* DateTime.now;
          const providerThread: OrchestrationV2ProviderThread = {
            id: idAllocator.derive.providerThread({
              driver: ZCODE_PROVIDER,
              nativeThreadId: forkedSessionId,
            }),
            driver: ZCODE_PROVIDER,
            providerInstanceId: options.instanceId,
            providerSessionId: input.providerSessionId,
            appThreadId: forkInput.targetThreadId,
            ownerNodeId: forkInput.ownerNodeId ?? null,
            nativeThreadRef: providerRef(forkedSessionId),
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: {
              providerThreadId: forkInput.sourceProviderThread.id,
              ...(forkInput.providerTurnId !== undefined
                ? { providerTurnId: forkInput.providerTurnId }
                : {}),
            },
            createdAt: now,
            updatedAt: now,
          };
          // A fresh session adopts the fork; one already hosting a thread keeps it.
          if (threadState === null) {
            threadState = {
              nativeSessionId: forkedSessionId,
              subscriptionId,
              providerThread,
              snapshot: null,
              frameSeq: null,
              resyncInFlight: false,
              activeTurn: null,
              interactions: new Map(),
            };
          }
          yield* emit({
            type: "provider_thread.updated",
            driver: ZCODE_PROVIDER,
            providerThread,
          });
          return providerThread;
        }),
        stopSubagent: Effect.fn("ZcodeAdapter.stopSubagent")(function* (stopInput) {
          const state = threadState;
          if (state === null) return;
          yield* sendCommand(
            "cancelBackgroundWork",
            { workId: stopInput.nativeTaskId },
            state.nativeSessionId,
          ).pipe(Effect.ignore);
        }),
      };

      return runtime;
    }),
  });
});

export type ZcodeAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | IdAllocator.IdAllocatorV2
  | ProviderHost.ProviderHost
  | FileSystem.FileSystem
  | Path.Path;

export const ZcodeAdapterV2Driver: ProviderAdapterDriver<ZcodeSettings, ZcodeAdapterV2DriverEnv> = {
  driverKind: ZCODE_PROVIDER,
  configSchema: ZcodeSettings,
  defaultConfig: (): ZcodeSettings => DEFAULT_ZCODE_SETTINGS,
  create: Effect.fn("ZcodeAdapterV2Driver.create")(
    function* (createInput: ProviderAdapterDriverCreateInput<ZcodeSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      return yield* makeZcodeAdapterV2({
        instanceId: createInput.instanceId,
        settings: { ...createInput.config, enabled: createInput.enabled },
        environment: yield* mergeProviderInstanceEnvironment(
          createInput.environment,
          hostEnvironment,
        ),
      });
    },
    (effect, createInput) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: ZCODE_PROVIDER,
              instanceId: createInput.instanceId,
              detail: "Failed to create zcode adapter.",
              cause,
            }),
        ),
      ),
  ),
};
