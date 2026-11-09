// Maintains the authoritative v4 ConversationSnapshot from snapshot frames +
// contiguous deltas. Semantics ported from zcode's apply.ts: key-level
// replacement for state.updated, tail-searched row upserts, no-op for
// unloaded rowIds, append-only row text paths.
import {
  recordArray,
  recordField,
  recordNumber,
  recordString,
  type ZcodeDelta,
  type ZcodeRecord,
} from "./protocol.ts";

export interface ZcodeRowsWindow {
  readonly window: ReadonlyArray<ZcodeRecord>;
  readonly totalCount: number;
  readonly firstRowId: number | null;
}

export interface ZcodeConversationState {
  readonly sessionId: string;
  readonly logEpoch: string;
  readonly seq: number;
  readonly revision: number;
  readonly control: ZcodeRecord | undefined;
  readonly availability: ZcodeRecord | undefined;
  readonly inputRouting: ZcodeRecord | undefined;
  readonly meta: ZcodeRecord | undefined;
  readonly config: ZcodeRecord | undefined;
  readonly usage: ZcodeRecord | undefined;
  readonly queue: ZcodeRecord | undefined;
  readonly pendingInteractions: ReadonlyArray<ZcodeRecord>;
  readonly backgroundWorks: ReadonlyArray<ZcodeRecord>;
  readonly subagents: ZcodeRecord | undefined;
  readonly goal: ZcodeRecord | null | undefined;
  readonly plan: ZcodeRecord | null | undefined;
  readonly rows: ZcodeRowsWindow;
  readonly runtimeInitialization: ZcodeRecord | undefined;
}

export function readSnapshotState(snapshot: ZcodeRecord): ZcodeConversationState {
  return {
    sessionId: recordString(snapshot, "sessionId") ?? "",
    logEpoch: recordString(snapshot, "logEpoch") ?? "",
    seq: recordNumber(snapshot, "seq") ?? 0,
    revision: recordNumber(snapshot, "revision") ?? 0,
    control: recordField(snapshot, "control") as ZcodeRecord | undefined,
    availability: recordField(snapshot, "availability") as ZcodeRecord | undefined,
    inputRouting: recordField(snapshot, "inputRouting") as ZcodeRecord | undefined,
    meta: recordField(snapshot, "meta") as ZcodeRecord | undefined,
    config: recordField(snapshot, "config") as ZcodeRecord | undefined,
    usage: recordField(snapshot, "usage") as ZcodeRecord | undefined,
    queue: recordField(snapshot, "queue") as ZcodeRecord | undefined,
    pendingInteractions: recordArray(snapshot, "pendingInteractions") as ReadonlyArray<ZcodeRecord>,
    backgroundWorks: recordArray(snapshot, "backgroundWorks") as ReadonlyArray<ZcodeRecord>,
    subagents: recordField(snapshot, "subagents") as ZcodeRecord | undefined,
    goal: recordField(snapshot, "goal") as ZcodeRecord | null | undefined,
    plan: recordField(snapshot, "plan") as ZcodeRecord | null | undefined,
    runtimeInitialization: recordField(snapshot, "runtimeInitialization") as
      | ZcodeRecord
      | undefined,
    rows: readRowsWindow(recordField(snapshot, "rows")),
  };
}

function readRowsWindow(rows: unknown): ZcodeRowsWindow {
  const record = rows as ZcodeRecord | undefined;
  if (record === undefined) return { window: [], totalCount: 0, firstRowId: null };
  const firstRowId = recordNumber(record, "firstRowId");
  return {
    window: recordArray(record, "window") as ReadonlyArray<ZcodeRecord>,
    totalCount: recordNumber(record, "totalCount") ?? 0,
    firstRowId: firstRowId === undefined ? null : firstRowId,
  };
}

function appendToRowText(row: ZcodeRecord, key: string, append: unknown): ZcodeRecord {
  const current = row[key];
  return {
    ...row,
    [key]:
      (typeof current === "string" ? current : "") + (typeof append === "string" ? append : ""),
  };
}

function appendToNestedRowText(row: ZcodeRecord, parentKey: string, append: unknown): ZcodeRecord {
  const parent = row[parentKey];
  if (typeof parent !== "object" || parent === null) return row;
  const parentRecord = parent as ZcodeRecord;
  const current = parentRecord["text"];
  return {
    ...row,
    [parentKey]: {
      ...parentRecord,
      text:
        (typeof current === "string" ? current : "") + (typeof append === "string" ? append : ""),
    },
  };
}

function appendToRow(row: ZcodeRecord, path: unknown, append: unknown): ZcodeRecord {
  switch (path) {
    case "text":
    case "inputText":
    case "summaryText":
      return appendToRowText(row, path as string, append);
    case "output.text":
      return appendToNestedRowText(row, "output", append);
    default:
      return row;
  }
}

function findLastIndex(
  rows: ReadonlyArray<ZcodeRecord>,
  predicate: (row: ZcodeRecord) => boolean,
): number {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    if (predicate(rows[index]!)) return index;
  }
  return -1;
}

/** Apply one delta; pure, returns a new state (or the same state for no-ops). */
export function applyConversationDelta(
  state: ZcodeConversationState,
  delta: ZcodeDelta,
): ZcodeConversationState {
  switch (delta.op) {
    case "row.appended": {
      const row = recordField(delta, "row") as ZcodeRecord | undefined;
      const rowId = row === undefined ? undefined : recordNumber(row, "rowId");
      if (row === undefined || rowId === undefined) return state;
      return {
        ...state,
        rows: {
          window: [...state.rows.window, row],
          totalCount: state.rows.totalCount + 1,
          firstRowId: state.rows.firstRowId ?? rowId,
        },
      };
    }
    case "row.upserted": {
      const row = recordField(delta, "row") as ZcodeRecord | undefined;
      const rowId = row === undefined ? undefined : recordNumber(row, "rowId");
      if (row === undefined || rowId === undefined) return state;
      const index = findLastIndex(
        state.rows.window,
        (candidate) => recordNumber(candidate, "rowId") === rowId,
      );
      // Unloaded rowId = no-op (evicted rows only come back via rowsRange).
      if (index === -1) return state;
      const window = [...state.rows.window];
      window[index] = row;
      return { ...state, rows: { ...state.rows, window } };
    }
    case "row.removed": {
      const fromRowId = recordNumber(delta, "fromRowId");
      if (fromRowId === undefined) return state;
      const window = state.rows.window.filter(
        (row) => (recordNumber(row, "rowId") ?? 0) < fromRowId,
      );
      const removed = state.rows.window.length - window.length;
      const removesEntireActiveBranch =
        state.rows.firstRowId !== null && fromRowId <= state.rows.firstRowId;
      return {
        ...state,
        rows: {
          window,
          totalCount: removesEntireActiveBranch ? 0 : Math.max(0, state.rows.totalCount - removed),
          firstRowId: removesEntireActiveBranch ? null : state.rows.firstRowId,
        },
      };
    }
    case "row.delta": {
      const rowId = recordNumber(delta, "rowId");
      if (rowId === undefined) return state;
      const index = findLastIndex(state.rows.window, (row) => recordNumber(row, "rowId") === rowId);
      if (index === -1) return state;
      const window = [...state.rows.window];
      window[index] = appendToRow(
        window[index]!,
        recordField(delta, "path"),
        recordField(delta, "append"),
      );
      return { ...state, rows: { ...state.rows, window } };
    }
    case "state.updated": {
      // Key-level replacement: present keys overwrite, never deep-merge.
      const patch = recordField(delta, "patch") as ZcodeRecord | undefined;
      if (patch === undefined) return state;
      const merged: Record<string, unknown> = {
        sessionId: state.sessionId,
        logEpoch: state.logEpoch,
        seq: state.seq,
        revision: state.revision,
        control: state.control,
        availability: state.availability,
        inputRouting: state.inputRouting,
        meta: state.meta,
        config: state.config,
        usage: state.usage,
        queue: state.queue,
        pendingInteractions: state.pendingInteractions,
        backgroundWorks: state.backgroundWorks,
        subagents: state.subagents,
        goal: state.goal,
        plan: state.plan,
        runtimeInitialization: state.runtimeInitialization,
        rows: state.rows,
      };
      for (const [key, value] of Object.entries(patch)) {
        merged[key] = value;
      }
      return readSnapshotState(merged as ZcodeRecord);
    }
    default:
      // workflowRun.*/pluginUi.* and future ops do not change the core snapshot.
      return state;
  }
}
