import { describe, expect, it } from "@effect/vitest";

import { applyConversationDelta, readSnapshotState } from "./snapshotReducer.ts";

const snapshot = {
  protocolVersion: 1,
  sessionId: "sess_1",
  logEpoch: "epoch-1",
  seq: 4,
  revision: 2,
  control: { phase: "draft" },
  rows: {
    window: [
      { rowId: 1, kind: "turnHeader", entityId: "t1", state: "running" },
      { rowId: 2, kind: "userInput", entityId: "u1", text: "hi" },
    ],
    totalCount: 2,
    firstRowId: 1,
  },
};

describe("readSnapshotState", () => {
  it("reads core fields from a snapshot frame payload", () => {
    const state = readSnapshotState(snapshot as never);
    expect(state.sessionId).toBe("sess_1");
    expect(state.seq).toBe(4);
    expect(state.rows.window).toHaveLength(2);
    expect(state.rows.firstRowId).toBe(1);
  });

  it("tolerates missing optional sections", () => {
    const state = readSnapshotState({ sessionId: "s", logEpoch: "e" });
    expect(state.rows.window).toEqual([]);
    expect(state.pendingInteractions).toEqual([]);
  });
});

describe("applyConversationDelta", () => {
  it("appends rows and tracks firstRowId and totalCount", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "row.appended",
      row: { rowId: 3, kind: "assistantText", entityId: "a1", text: "" },
    } as never);
    expect(state.rows.window).toHaveLength(3);
    expect(state.rows.totalCount).toBe(3);
    expect(state.rows.firstRowId).toBe(1);
  });

  it("upserts the tail row by rowId", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "row.upserted",
      row: { rowId: 1, kind: "turnHeader", entityId: "t1", state: "completedSuccess" },
    } as never);
    expect(state.rows.window[0]).toMatchObject({ state: "completedSuccess" });
    expect(state.rows.window).toHaveLength(2);
  });

  it("no-ops an upsert for an unloaded rowId", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "row.upserted",
      row: { rowId: 99, kind: "turnHeader", entityId: "t99" },
    } as never);
    expect(state.rows.window).toHaveLength(2);
  });

  it("appends text via row.delta", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "row.appended",
      row: { rowId: 3, kind: "assistantText", entityId: "a1", text: "P" },
    } as never);
    state = applyConversationDelta(state, {
      op: "row.delta",
      rowId: 3,
      path: "text",
      append: "ONG",
    } as never);
    expect(state.rows.window[2]).toMatchObject({ text: "PONG" });
  });

  it("replaces state keys without deep merge", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "state.updated",
      patch: { control: { phase: "running" }, revision: 3 },
    } as never);
    expect(state.control).toEqual({ phase: "running" });
    expect(state.revision).toBe(3);
  });

  it("row.removed truncates from fromRowId and resets anchors when removing everything", () => {
    let state = readSnapshotState(snapshot as never);
    state = applyConversationDelta(state, {
      op: "row.removed",
      fromRowId: 2,
    } as never);
    expect(state.rows.window).toHaveLength(1);
    expect(state.rows.totalCount).toBe(1);
    state = applyConversationDelta(state, {
      op: "row.removed",
      fromRowId: 1,
    } as never);
    expect(state.rows.window).toHaveLength(0);
    expect(state.rows.totalCount).toBe(0);
    expect(state.rows.firstRowId).toBeNull();
  });

  it("ignores future delta ops", () => {
    const state = readSnapshotState(snapshot as never);
    const next = applyConversationDelta(state, { op: "workflowRun.updated" } as never);
    expect(next).toBe(state);
  });
});
