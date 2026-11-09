import { describe, expect, it } from "@effect/vitest";

import { crc32WireBytes, WireFrameReassembler } from "./wire.ts";
import type { ZcodeTopicFrame, ZcodeWireFrame } from "./protocol.ts";

const logicalFrame = (topic: string, subscriptionId: string, toSeq: number): ZcodeTopicFrame => ({
  topic,
  subscriptionId,
  fromSeq: 0,
  toSeq,
  payload: {
    kind: "deltas",
    deltas: [{ op: "state.updated", patch: { revision: toSeq } }],
  },
});

function fragmentFrame(
  whole: ZcodeTopicFrame,
  index: number,
  count: number,
): ZcodeWireFrame & { kind: "fragment" } {
  const bytes = new TextEncoder().encode(JSON.stringify(whole));
  const chunkSize = Math.ceil(bytes.length / count);
  const slice = bytes.slice(index * chunkSize, (index + 1) * chunkSize);
  return {
    kind: "fragment",
    topic: whole.topic,
    subscriptionId: whole.subscriptionId,
    logicalFrameId: `${whole.subscriptionId}-lf-1`,
    fragmentIndex: index,
    fragmentCount: count,
    logicalBytes: bytes.length,
    checksum: { algorithm: "crc32", value: crc32WireBytes(bytes) },
    dataBase64: Buffer.from(slice).toString("base64"),
  };
}

describe("crc32WireBytes", () => {
  it("matches known CRC-32 check values", () => {
    expect(crc32WireBytes(new TextEncoder().encode("123456789"))).toBe(0xcbf43926);
  });
});

describe("WireFrameReassembler", () => {
  it("passes complete frames through", () => {
    const frame = logicalFrame("conversation/sess_1", "sub-1", 1);
    const reassembler = new WireFrameReassembler();
    const outcome = reassembler.push({
      kind: "complete",
      topic: frame.topic,
      subscriptionId: frame.subscriptionId,
      frame,
    });
    expect(outcome.kind).toBe("frame");
    if (outcome.kind === "frame") {
      expect(outcome.frame.toSeq).toBe(1);
      expect(outcome.frame.topic).toBe("conversation/sess_1");
    }
  });

  it("reassembles fragments into one logical frame", () => {
    const frame = logicalFrame("conversation/sess_2", "sub-2", 7);
    const reassembler = new WireFrameReassembler();
    let outcome = reassembler.push(fragmentFrame(frame, 0, 3));
    expect(outcome.kind).toBe("pending");
    outcome = reassembler.push(fragmentFrame(frame, 2, 3));
    expect(outcome.kind).toBe("pending");
    outcome = reassembler.push(fragmentFrame(frame, 1, 3));
    expect(outcome.kind).toBe("frame");
    if (outcome.kind === "frame") {
      expect(outcome.frame.toSeq).toBe(7);
      expect(outcome.frame.payload.kind).toBe("deltas");
    }
    expect(reassembler.stagedCount).toBe(0);
  });

  it("rejects a checksum mismatch over the reassembled bytes", () => {
    const frame = logicalFrame("conversation/sess_3", "sub-3", 2);
    const bytes = new TextEncoder().encode(JSON.stringify(frame));
    // All fragments carry the same (wrong) whole-frame checksum, so metadata matches.
    const badChecksum = { algorithm: "crc32", value: crc32WireBytes(bytes) + 1 };
    const first = { ...fragmentFrame(frame, 0, 2), checksum: badChecksum };
    const second = { ...fragmentFrame(frame, 1, 2), checksum: badChecksum };
    const reassembler = new WireFrameReassembler();
    reassembler.push(first);
    const outcome = reassembler.push(second);
    expect(outcome).toMatchObject({
      kind: "rejected",
      reasonCode: "proto.frameAssemblyChecksumMismatch",
    });
  });

  it("rejects fragments whose metadata disagrees", () => {
    const frame = logicalFrame("conversation/sess_3", "sub-3", 2);
    const reassembler = new WireFrameReassembler();
    reassembler.push(fragmentFrame(frame, 0, 2));
    const outcome = reassembler.push({
      ...fragmentFrame(frame, 1, 2),
      subscriptionId: "sub-other",
    });
    expect(outcome).toMatchObject({
      kind: "rejected",
      reasonCode: "proto.frameAssemblyMetadataMismatch",
    });
  });

  it("rejects out-of-range and non-integer fragment metadata", () => {
    const frame = logicalFrame("conversation/sess_4", "sub-4", 3);
    const cases: ReadonlyArray<[Partial<ZcodeWireFrame & { kind: "fragment" }>, string]> = [
      [{ fragmentCount: 10_000 }, "proto.frameFragmentCountExceeded"],
      [{ fragmentIndex: 0.5 }, "proto.frameFragmentCountExceeded"],
      [{ fragmentIndex: Number.NaN }, "proto.frameFragmentCountExceeded"],
      [{ fragmentCount: 1.5 }, "proto.frameFragmentCountExceeded"],
      [{ fragmentCount: Number.POSITIVE_INFINITY }, "proto.frameFragmentCountExceeded"],
      [{ logicalBytes: 1e6 + 0.5 }, "proto.frameAssemblyMetadataMismatch"],
      [{ logicalBytes: Number.NaN }, "proto.frameAssemblyMetadataMismatch"],
    ];
    for (const [override, reasonCode] of cases) {
      const reassembler = new WireFrameReassembler();
      const outcome = reassembler.push({ ...fragmentFrame(frame, 0, 2), ...override });
      expect(outcome).toMatchObject({ kind: "rejected", reasonCode });
      expect(reassembler.stagedCount).toBe(0);
    }
  });

  it("rejects fragment payloads that are not valid wire base64", () => {
    const frame = logicalFrame("conversation/sess_5", "sub-5", 3);
    const reassembler = new WireFrameReassembler();
    const outcome = reassembler.push({
      ...fragmentFrame(frame, 0, 2),
      dataBase64: "not base64!!",
    });
    expect(outcome).toMatchObject({
      kind: "rejected",
      reasonCode: "proto.frameAssemblyInvalidBase64",
    });
    // Nothing staged: the invalid fragment leaves no partial state behind.
    expect(reassembler.stagedCount).toBe(0);
  });

  it("caps staged logical frames and evicts the oldest one", () => {
    const reassembler = new WireFrameReassembler();
    // fragmentCount=2 frames staged by pushing only fragment 0 stay pending.
    for (let index = 0; index < 33; index += 1) {
      const frame = logicalFrame("conversation/sess_6", "sub-6", 1);
      const outcome = reassembler.push({
        ...fragmentFrame(frame, 0, 2),
        logicalFrameId: `lf-stale-${index}`,
      });
      expect(outcome.kind).toBe("pending");
    }
    // The 33rd push evicted the oldest staged frame instead of growing further.
    expect(reassembler.stagedCount).toBe(32);
    // The second-oldest frame survived and completes from its staged fragment.
    const survivorFrame = logicalFrame("conversation/sess_6", "sub-6", 1);
    const outcome = reassembler.push({
      ...fragmentFrame(survivorFrame, 1, 2),
      logicalFrameId: "lf-stale-1",
    });
    expect(outcome.kind).toBe("frame");
    if (outcome.kind === "frame") expect(outcome.frame.toSeq).toBe(1);
    expect(reassembler.stagedCount).toBe(31);
  });

  it("clears staged state when a conflicting fragment arrives", () => {
    const frame = logicalFrame("conversation/sess_7", "sub-7", 2);
    const reassembler = new WireFrameReassembler();
    reassembler.push(fragmentFrame(frame, 0, 2));
    expect(reassembler.stagedCount).toBe(1);
    const outcome = reassembler.push({
      ...fragmentFrame(frame, 0, 2),
      dataBase64: Buffer.from("conflicting bytes").toString("base64"),
    });
    expect(outcome).toMatchObject({
      kind: "rejected",
      reasonCode: "proto.frameAssemblyFragmentConflict",
    });
    expect(reassembler.stagedCount).toBe(0);
  });
});
