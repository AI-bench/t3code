// v4 physical wire reassembly: fragments stage per logicalFrameId; only after
// bytes, checksum, UTF-8 and JSON all pass does a logical frame come out.
// Semantics ported from zcode's wire-reassembly (fail-closed, bounded memory).
import type { ZcodeRecord, ZcodeTopicFrame, ZcodeWireFrame } from "./protocol.ts";

const MAX_LOGICAL_FRAME_BYTES = 16 * 1024 * 1024;
const MAX_FRAGMENTS = 64;
// Protocol v4 §3.2 per-consumer staging caps: concurrent assemblies and total
// decoded staging bytes stay bounded when fragments never complete.
const MAX_STAGED_FRAMES = 32;
const MAX_STAGED_BYTES = 32 * 1024 * 1024;

export type ReassemblyOutcome =
  | { readonly kind: "frame"; readonly frame: ZcodeTopicFrame; readonly deliveryKind?: string }
  | { readonly kind: "pending" }
  | { readonly kind: "rejected"; readonly reasonCode: string };

function completeOutcome(
  frame: ZcodeTopicFrame,
  deliveryKind: string | undefined,
): ReassemblyOutcome {
  return deliveryKind === undefined
    ? { kind: "frame", frame }
    : { kind: "frame", frame, deliveryKind };
}

interface FragmentMetadata {
  readonly logicalFrameId: string;
  readonly topic: string;
  readonly subscriptionId: string;
  readonly fragmentCount: number;
  readonly logicalBytes: number;
  readonly checksumValue: number;
}

interface StagedLogical {
  readonly meta: FragmentMetadata;
  readonly fragments: Map<number, Uint8Array>;
  decodedByteTotal: number;
}

// Standard CRC-32 (IEEE 802.3), table-driven.
const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32WireBytes(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i += 1) {
    const byte = bytes[i] ?? 0;
    crc = (CRC32_TABLE[(crc ^ byte) & 0xff] ?? 0) ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// Node's base64 decoder never throws and silently skips non-alphabet bytes,
// so the fragment payload is validated against the CLI's strict wire alphabet
// before decoding (zcode wire-binary topicWireBase64Schema).
const WIRE_BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u;

function decodeBase64(input: string): Uint8Array | undefined {
  if (typeof input !== "string" || input.length < 4 || !WIRE_BASE64_PATTERN.test(input)) {
    return undefined;
  }
  return new Uint8Array(Buffer.from(input, "base64"));
}

function frameMatchesEnvelope(
  frame: unknown,
  wire: { topic: string; subscriptionId: string },
): boolean {
  if (typeof frame !== "object" || frame === null) return false;
  const record = frame as { topic?: unknown; subscriptionId?: unknown };
  return record.topic === wire.topic && record.subscriptionId === wire.subscriptionId;
}

export class WireFrameReassembler {
  private readonly staged = new Map<string, StagedLogical>();
  private stagedBytes = 0;

  private discard(logicalFrameId: string): void {
    const staged = this.staged.get(logicalFrameId);
    if (staged === undefined) return;
    this.stagedBytes -= staged.decodedByteTotal;
    this.staged.delete(logicalFrameId);
  }

  /** Feed one physical wire frame; complete frames surface atomically. */
  push(wire: ZcodeWireFrame & { deliveryKind?: string }): ReassemblyOutcome {
    if (wire.kind === "complete") {
      if (Object.keys(wire.frame).length === 0) {
        return { kind: "rejected", reasonCode: "proto.frameAssemblyInvalidPayload" };
      }
      return completeOutcome(wire.frame, wire.deliveryKind);
    }

    const fragment = wire;
    if (
      !Number.isInteger(fragment.fragmentCount) ||
      !Number.isInteger(fragment.fragmentIndex) ||
      fragment.fragmentCount > MAX_FRAGMENTS ||
      fragment.fragmentCount <= 0 ||
      fragment.fragmentIndex < 0 ||
      fragment.fragmentIndex >= fragment.fragmentCount
    ) {
      return { kind: "rejected", reasonCode: "proto.frameFragmentCountExceeded" };
    }
    const logicalBytes = fragment.logicalBytes;
    if (!Number.isInteger(logicalBytes)) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyMetadataMismatch" };
    }
    if (logicalBytes > MAX_LOGICAL_FRAME_BYTES) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyTooLarge" };
    }
    const checksumValue = fragment.checksum?.value;
    if (typeof checksumValue !== "number") {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyMetadataMismatch" };
    }
    if (fragment.fragmentCount > logicalBytes) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyMetadataMismatch" };
    }
    // Decode before touching staging state: an invalid fragment must leave
    // nothing behind.
    const decoded = decodeBase64(fragment.dataBase64);
    if (decoded === undefined) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyInvalidBase64" };
    }

    const meta: FragmentMetadata = {
      logicalFrameId: fragment.logicalFrameId,
      topic: wire.topic,
      subscriptionId: wire.subscriptionId,
      fragmentCount: fragment.fragmentCount,
      logicalBytes,
      checksumValue,
    };
    let staged = this.staged.get(fragment.logicalFrameId);
    if (staged === undefined) {
      // Cap concurrent assemblies (protocol v4 §3.2): the oldest staged frame
      // yields its slot when a peer floods never-completing logical frames.
      const oldest =
        this.staged.size >= MAX_STAGED_FRAMES ? this.staged.keys().next().value : undefined;
      if (oldest !== undefined) this.discard(oldest);
      staged = { meta, fragments: new Map(), decodedByteTotal: 0 };
      this.staged.set(fragment.logicalFrameId, staged);
    } else {
      const existing = staged.meta;
      if (
        existing.topic !== meta.topic ||
        existing.subscriptionId !== meta.subscriptionId ||
        existing.fragmentCount !== meta.fragmentCount ||
        existing.logicalBytes !== meta.logicalBytes ||
        existing.checksumValue !== meta.checksumValue
      ) {
        this.discard(fragment.logicalFrameId);
        return { kind: "rejected", reasonCode: "proto.frameAssemblyMetadataMismatch" };
      }
    }

    const previous = staged.fragments.get(fragment.fragmentIndex);
    if (previous !== undefined) {
      if (
        previous.byteLength !== decoded.byteLength ||
        previous.some((byte, index) => byte !== decoded[index])
      ) {
        // The peer's stream is inconsistent; without resetting, the staged
        // fragments would linger forever waiting for a frame that cannot pass
        // checksum honestly.
        this.discard(fragment.logicalFrameId);
        return { kind: "rejected", reasonCode: "proto.frameAssemblyFragmentConflict" };
      }
      if (staged.fragments.size < staged.meta.fragmentCount) return { kind: "pending" };
    } else {
      const nextDecodedByteTotal = staged.decodedByteTotal + decoded.byteLength;
      if (nextDecodedByteTotal > MAX_LOGICAL_FRAME_BYTES) {
        this.discard(fragment.logicalFrameId);
        return { kind: "rejected", reasonCode: "proto.frameAssemblyTooLarge" };
      }
      if (nextDecodedByteTotal > staged.meta.logicalBytes) {
        this.discard(fragment.logicalFrameId);
        return { kind: "rejected", reasonCode: "proto.frameAssemblyLengthMismatch" };
      }
      if (this.stagedBytes + decoded.byteLength > MAX_STAGED_BYTES) {
        this.discard(fragment.logicalFrameId);
        return { kind: "rejected", reasonCode: "proto.frameAssemblyTooLarge" };
      }
      staged.fragments.set(fragment.fragmentIndex, decoded);
      staged.decodedByteTotal = nextDecodedByteTotal;
      this.stagedBytes += decoded.byteLength;
    }

    if (staged.fragments.size < staged.meta.fragmentCount) return { kind: "pending" };

    this.discard(fragment.logicalFrameId);
    if (staged.decodedByteTotal !== staged.meta.logicalBytes) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyLengthMismatch" };
    }
    const logical = new Uint8Array(staged.decodedByteTotal);
    let offset = 0;
    for (let index = 0; index < staged.meta.fragmentCount; index += 1) {
      const part = staged.fragments.get(index);
      if (part === undefined) {
        return { kind: "rejected", reasonCode: "proto.frameAssemblyLengthMismatch" };
      }
      logical.set(part, offset);
      offset += part.byteLength;
    }
    if (crc32WireBytes(logical) !== staged.meta.checksumValue) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyChecksumMismatch" };
    }
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(logical);
    } catch {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyInvalidUtf8" };
    }
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyInvalidJson" };
    }
    if (!frameMatchesEnvelope(value, staged.meta)) {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyMetadataMismatch" };
    }
    const record = value as ZcodeRecord;
    const payload = record["payload"];
    const payloadKind =
      typeof payload === "object" && payload !== null
        ? (payload as ZcodeRecord)["kind"]
        : undefined;
    if (payloadKind !== "snapshot" && payloadKind !== "deltas") {
      return { kind: "rejected", reasonCode: "proto.frameAssemblyInvalidPayload" };
    }
    return completeOutcome(record as unknown as ZcodeTopicFrame, fragment.deliveryKind);
  }

  /** Number of logical frames stuck waiting for fragments (memory guard). */
  get stagedCount(): number {
    return this.staged.size;
  }
}
