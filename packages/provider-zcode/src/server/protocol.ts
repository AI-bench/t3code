// ZCode Protocol v4 subset consumed by the T3 adapter; tolerant accessors so
// additive CLI changes never drop a frame.

import * as Predicate from "effect/Predicate";

// ── Method names ─────────────────────────────────────────────────────────────

export const ZCODE_METHODS = {
  command: "v4/command",
  commandsQuery: "v4/commands/query",
  conversationSubscribe: "v4/conversation/subscribe",
  conversationResync: "v4/conversation/resync",
  conversationUnsubscribe: "v4/conversation/unsubscribe",
  sessionStop: "session/stop",
  sessionClose: "session/close",
  providerRefreshRegistry: "provider/refreshRegistry",
} as const;

export const ZCODE_NOTIFICATIONS = {
  conversationFrame: "v4/conversation/frame",
  stateUpdated: "state.updated",
  sessionLifecycleChanged: "session/lifecycle/changed",
} as const;

// Reverse requests the CLI sends to the controller.
export const ZCODE_REVERSE_METHODS = {
  requestRuntimePreferences: "session/requestRuntimePreferences",
  requestProviderRuntimeHeaders: "interaction/requestProviderRuntimeHeaders",
  requestPermission: "interaction/requestPermission",
  requestUserInput: "interaction/requestUserInput",
  requestOfficialMcpAuthHeaders: "interaction/requestOfficialMcpAuthHeaders",
  browserList: "interaction/browserList",
  browserExecute: "interaction/browserExecute",
} as const;

export const conversationTopic = (sessionId: string): string => `conversation/${sessionId}`;

// ── Tolerant record accessors ────────────────────────────────────────────────

export type ZcodeRecord = Record<string, unknown>;

export function recordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? (input as ZcodeRecord)[key] : undefined;
}

export function recordString(input: unknown, key: string): string | undefined {
  const value = recordField(input, key);
  return Predicate.isString(value) ? value : undefined;
}

export function recordNumber(input: unknown, key: string): number | undefined {
  const value = recordField(input, key);
  return Predicate.isNumber(value) && Number.isFinite(value) ? value : undefined;
}

export function recordArray(input: unknown, key: string): ReadonlyArray<unknown> {
  const value = recordField(input, key);
  return Array.isArray(value) ? value : [];
}

export function recordObject(input: unknown, key: string): ZcodeRecord | undefined {
  const value = recordField(input, key);
  return Predicate.isObject(value) ? (value as ZcodeRecord) : undefined;
}

// ── Commands ─────────────────────────────────────────────────────────────────

export type ZcodeModelSelection = {
  readonly providerId: string;
  readonly modelId: string;
  readonly options?: { readonly reasoningLevel?: string };
};

export function parseZcodeModelSlug(slug: string): ZcodeModelSelection | null {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return null;
  return { providerId: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

export type ZcodeSessionMode = "plan" | "build" | "edit" | "yolo" | "guarded";

export type ZcodeRequestedDelivery = "startNow" | "queue" | "guide";

export interface ZcodeCommandEnvelope {
  readonly commandId: string;
  readonly clientId: string;
  readonly sessionId: string | null;
  readonly type: string;
  readonly payload: unknown;
  readonly issuedAt: number;
  readonly baseRevision?: number;
  readonly baseLogEpoch?: string;
  readonly inputOrigin?: "desktop" | "mobile";
}

export interface ZcodeCommandAck {
  readonly commandId: string;
  readonly status: "accepted" | "rejected" | "stale" | "duplicate" | "noop" | "failed";
  readonly reasonCode?: string;
  readonly message?: string;
  readonly revisionAtDecision: number;
  readonly result?: ZcodeRecord;
}

// ── Conversation frames ──────────────────────────────────────────────────────

export type ZcodeWireFrame =
  | {
      readonly kind: "complete";
      readonly topic: string;
      readonly subscriptionId: string;
      /** inner logical frame */
      readonly frame: ZcodeTopicFrame;
    }
  | {
      readonly kind: "fragment";
      readonly topic: string;
      readonly subscriptionId: string;
      readonly logicalFrameId: string;
      readonly fragmentIndex: number;
      readonly fragmentCount: number;
      readonly logicalBytes: number;
      readonly checksum?: { readonly algorithm?: string; readonly value: number };
      readonly dataBase64: string;
    };

export interface ZcodeTopicFrame {
  readonly topic: string;
  readonly subscriptionId: string;
  readonly fromSeq: number;
  readonly toSeq: number;
  readonly sentAt?: number;
  readonly payload:
    | { readonly kind: "snapshot"; readonly snapshot: ZcodeRecord }
    | { readonly kind: "deltas"; readonly deltas: ReadonlyArray<ZcodeDelta> };
}

export type ZcodeDelta = ZcodeRecord & { readonly op: string };

// ── Rows (subset we project) ─────────────────────────────────────────────────

export type ZcodeRowKind =
  | "turnHeader"
  | "userInput"
  | "assistantText"
  | "reasoning"
  | "toolCall"
  | "subagent"
  | "artifact"
  | "hookInvocation"
  | "timelineMarker";

export type ZcodeTurnHeaderState =
  | "running"
  | "completedSuccess"
  | "completedInterrupted"
  | "failed";

export type ZcodeToolCallStatus =
  | "inputStreaming"
  | "pendingApproval"
  | "running"
  | "success"
  | "error"
  | "cancelled";

export type ZcodeSubagentStatus = "running" | "success" | "failed" | "cancelled";

export interface ZcodeRowBase {
  readonly rowId: number;
  readonly entityId: string;
  readonly turnId?: string;
  readonly productTurnId?: string;
  readonly kind: ZcodeRowKind;
  readonly createdAt?: number;
}

// ── Pending interactions (approvals / questions) ─────────────────────────────

export interface ZcodeInteractionOption {
  readonly optionId: string;
  readonly label?: string;
  readonly kind?: string;
}

export interface ZcodePendingInteraction {
  readonly interactionId: string;
  readonly kind: "permission" | "userInput" | "workspaceHookReview";
  readonly anchorRowId: number | null;
  readonly payload: ZcodeRecord;
}

// ── Runtime preferences reverse request ──────────────────────────────────────

export const ZCODE_RUNTIME_PREFERENCES_RESPONSE = {
  nativeSearchEnhancementsEnabled: false,
  toolSearchMode: "off",
  // false suppresses the subagents/readRuntimeConfig reverse request entirely.
  subagentRuntimeConfigEnabled: false,
  memoryEnabled: false,
  askUserQuestionAutoResolutionEnabled: false,
  modelContextBudgetStrategy: "preflight-v1",
} as const;

export const ZCODE_PROVIDER_HEADERS_DECLINE = { headersApplied: false } as const;

/**
 * A reverse-request handler returns this marker to park the request: the CLI
 * re-announces unanswered interaction requests; the adapter answers later via
 * `respondReverse` (zcode races the reverse channel against resolveInteraction).
 */
export const ZCODE_PARK_REVERSE = Symbol("zcode-park-reverse-request");
