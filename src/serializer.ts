import type { ClientFrame, ServerFrame } from './engine/connection';

/**
 * Wire-format adapter (1.16.0). The engine and client default to JSON, but
 * for high-throughput deployments — a customer pushing 1 MB initial
 * snapshots, a tenant fan-out hitting 100k frames/sec — a binary serializer
 * (msgpack, cbor, or a custom tagged layout) cuts both bandwidth and
 * parse-side CPU.
 *
 * Both ends of the connection MUST use the same serializer. The default
 * `jsonSerializer` keeps every existing client + server pair working
 * unchanged; opt in to a different one on BOTH `syncSocket` and the client
 * lib to gain the win.
 *
 * The serializer only handles the wire format. Frame-shape validation
 * stays in the engine (`parseFrame` in connection.ts) — it runs on the
 * decoded object, so the same validation works for JSON, msgpack, etc.
 */
export type FrameSerializer = {
	/** Server→client: encode an outgoing `ServerFrame` for transport. */
	encodeServer: (frame: ServerFrame) => string | ArrayBufferLike | Uint8Array;
	/** Client→server: encode an outgoing `ClientFrame` for transport. */
	encodeClient: (frame: ClientFrame) => string | ArrayBufferLike | Uint8Array;
	/**
	 * Deserialize a wire payload into a raw object. Return `null` for
	 * unparseable input — the engine's validation step turns that into
	 * a protocol error.
	 */
	decode: (raw: unknown) => unknown;
};

/**
 * Default JSON serializer — what `@absolutejs/sync` has always shipped.
 * Strings go through `JSON.parse`; already-parsed objects pass through
 * (some WS adapters auto-decode). `Uint8Array` / `ArrayBuffer` get
 * UTF-8 decoded first (binary WS frames carrying JSON text).
 */
export const jsonSerializer: FrameSerializer = {
	decode: (raw: unknown): unknown => {
		if (typeof raw === 'string') {
			try {
				return JSON.parse(raw);
			} catch {
				return null;
			}
		}
		if (raw instanceof Uint8Array) {
			try {
				return JSON.parse(new TextDecoder().decode(raw));
			} catch {
				return null;
			}
		}
		if (raw instanceof ArrayBuffer) {
			try {
				return JSON.parse(new TextDecoder().decode(new Uint8Array(raw)));
			} catch {
				return null;
			}
		}
		return raw;
	},
	encodeClient: (frame: ClientFrame): string => JSON.stringify(frame),
	encodeServer: (frame: ServerFrame): string => JSON.stringify(frame)
};
