import type { PresenceHandle, PresenceHub, PresenceMember } from './presence';
import type { Subscription, SyncEngine } from './syncEngine';
import type { FrameSerializer } from '../serializer';
import { jsonSerializer } from '../serializer';

/**
 * Wire protocol for the sync-engine WebSocket. One connection multiplexes many
 * collection subscriptions, each tagged with a client-chosen `id`.
 */

/** Client → server. */
export type ClientFrame =
	| {
			type: 'subscribe';
			id: string;
			collection: string;
			params?: unknown;
			/**
			 * Resume from a point already applied (catch-up instead of snapshot).
			 *
			 * Accepts either:
			 *  - `number` (pre-1.18 legacy) — the version of THIS engine instance.
			 *  - `string` (1.17.0+ cursor) — an opaque cross-instance resume
			 *    cursor returned by the server on prior snapshot/diff/frame.
			 *    The client round-trips it unmodified.
			 */
			since?: number | string;
	  }
	| { type: 'unsubscribe'; id: string }
	| { type: 'mutate'; mutationId: number; name: string; args?: unknown }
	| { type: 'presence-join'; room: string; memberId: string; state: unknown }
	| { type: 'presence-set'; room: string; state: unknown }
	| { type: 'presence-leave'; room: string };

/** One subscription's delta within a {@link ServerFrame} `frame`. */
export type FrameDiff<T = unknown> = {
	id: string;
	added: T[];
	removed: T[];
	changed: T[];
};

/**
 * Server → client. `version` is THIS engine's local change-feed watermark.
 * `cursor` (1.17.0+) is an opaque cross-instance resume cursor — round-trip
 * it on `subscribe.since` to resume across cluster shards.
 */
export type ServerFrame<T = unknown> =
	| {
			type: 'snapshot';
			id: string;
			rows: T[];
			version?: number;
			cursor?: string;
	  }
	| {
			type: 'diff';
			id: string;
			added: T[];
			removed: T[];
			changed: T[];
			version?: number;
			cursor?: string;
	  }
	| {
			// One atomic batch (e.g. a transactional mutation) that touched several
			// subscriptions — bundled into one message so the client applies them in
			// a single frame, never showing a torn cross-collection intermediate.
			type: 'frame';
			version?: number;
			cursor?: string;
			diffs: FrameDiff<T>[];
	  }
	| {
			// A presence room changed: members joined, updated state, or left.
			type: 'presence';
			room: string;
			joined: PresenceMember<T>[];
			updated: PresenceMember<T>[];
			left: string[];
	  }
	| { type: 'error'; id?: string; message: string }
	| { type: 'ack'; mutationId: number; result?: unknown }
	| { type: 'reject'; mutationId: number; message: string };

export type SyncConnectionOptions = {
	engine: SyncEngine;
	/** Resolved auth context for this connection; passed to every subscribe. */
	ctx: unknown;
	/**
	 * Send a frame to the client (the transport serializes it). May return
	 * a number — by convention `-1` signals transport-layer backpressure (the
	 * value Bun's `ws.send()` returns when the WS buffer is full). The
	 * connection tracks consecutive `-1` returns and surfaces them via
	 * `connection.stats().slowSendsRecent`. Legacy `void`-returning sends
	 * keep working unchanged.
	 *
	 * NOTE: 1.16.0 — `send` receives the typed `ServerFrame`. The connection
	 * does NOT pre-serialize; the WS adapter (`syncSocket`) wraps `send` to
	 * call `serializer.encodeServer(frame)` before `ws.send(...)`. This keeps
	 * the connection layer transport-agnostic.
	 */
	send: (frame: ServerFrame) => void | number;
	/** Optional presence hub; enables the `presence-*` frames (see createPresenceHub). */
	presence?: PresenceHub;
	/**
	 * Wire-format serializer (1.16.0). Defaults to `jsonSerializer` —
	 * the historical JSON-over-WS behavior. Both ends of the connection
	 * MUST use the same serializer; pair this option with the matching
	 * client-side `serializer` to opt into binary frames.
	 */
	serializer?: FrameSerializer;
};

/**
 * Connection-level operational counters surfaced via {@link SyncConnection.stats}.
 * Pair with the WS adapter's own backpressure signal for end-to-end slow-client
 * detection.
 */
export type SyncConnectionStats = {
	/** Active subscriptions on this connection. */
	subscriptionCount: number;
	/** Active presence-room memberships on this connection. */
	presenceRoomCount: number;
	/** Frames successfully sent (non-backpressure return) since the connection opened. */
	framesSent: number;
	/** Consecutive `-1` (backpressure) returns from `send` since the last successful send. */
	slowSendsRecent: number;
};

export type SyncConnection = {
	/** Handle one client frame (a parsed object or a raw JSON string). */
	handle: (raw: unknown) => Promise<void>;
	/** Tear down every subscription on this connection (call on socket close). */
	close: () => void;
	/**
	 * Point-in-time connection counters — subscription count, frames sent, and
	 * how many consecutive `send` calls came back with the transport's backpressure
	 * signal. Cheap; safe to call from a metering loop.
	 *
	 * Added in 1.14.0.
	 */
	stats: () => SyncConnectionStats;
};

const parseFrame = (
	raw: unknown,
	serializer: FrameSerializer
): ClientFrame | undefined => {
	// 1.16.0: hand off the wire decode to the serializer (default JSON).
	// The shape validation below is identical regardless of wire format —
	// the serializer just produces an object, we walk it for type safety.
	let value: unknown = raw;
	if (
		typeof value === 'string' ||
		value instanceof Uint8Array ||
		value instanceof ArrayBuffer
	) {
		value = serializer.decode(raw);
		if (value === null) return undefined;
	}
	if (typeof value !== 'object' || value === null) {
		return undefined;
	}
	const frame = value as {
		type?: unknown;
		id?: unknown;
		collection?: unknown;
		params?: unknown;
		since?: unknown;
		mutationId?: unknown;
		name?: unknown;
		args?: unknown;
		room?: unknown;
		memberId?: unknown;
		state?: unknown;
	};
	if (frame.type === 'subscribe') {
		// 1.17.0+: `since` accepts a number (legacy local-version) OR an
		// opaque cursor string. Drop anything else.
		const since =
			typeof frame.since === 'number' || typeof frame.since === 'string'
				? frame.since
				: undefined;
		return typeof frame.id === 'string' &&
			typeof frame.collection === 'string'
			? {
					type: 'subscribe',
					id: frame.id,
					collection: frame.collection,
					params: frame.params,
					since
				}
			: undefined;
	}
	if (frame.type === 'unsubscribe') {
		return typeof frame.id === 'string'
			? { type: 'unsubscribe', id: frame.id }
			: undefined;
	}
	if (frame.type === 'mutate') {
		return typeof frame.mutationId === 'number' &&
			typeof frame.name === 'string'
			? {
					type: 'mutate',
					mutationId: frame.mutationId,
					name: frame.name,
					args: frame.args
				}
			: undefined;
	}
	if (frame.type === 'presence-join') {
		return typeof frame.room === 'string' &&
			typeof frame.memberId === 'string'
			? {
					type: 'presence-join',
					room: frame.room,
					memberId: frame.memberId,
					state: frame.state
				}
			: undefined;
	}
	if (frame.type === 'presence-set') {
		return typeof frame.room === 'string'
			? { type: 'presence-set', room: frame.room, state: frame.state }
			: undefined;
	}
	if (frame.type === 'presence-leave') {
		return typeof frame.room === 'string'
			? { type: 'presence-leave', room: frame.room }
			: undefined;
	}
	return undefined;
};

/**
 * The per-connection protocol handler — transport-agnostic glue between a single
 * client socket and the {@link SyncEngine}. It owns that connection's
 * subscriptions: a `subscribe` frame authorizes + hydrates and replies with a
 * `snapshot`, then streams `diff` frames; `unsubscribe`/`close` release views.
 *
 * Pure (no WebSocket import) so it can be unit-tested with a fake `send`; the
 * Elysia `syncSocket` plugin is the thin adapter that feeds it socket events.
 */
export const createSyncConnection = ({
	engine,
	ctx,
	send: rawSend,
	presence,
	serializer = jsonSerializer
}: SyncConnectionOptions): SyncConnection => {
	const subscriptions = new Map<string, Subscription<unknown>>();
	// This connection's presence memberships (one per room), torn down on close.
	const presenceRooms = new Map<string, PresenceHandle<unknown>>();

	// 1.14.0: track transport-layer backpressure. `send` may return -1 (Bun's
	// WS backpressure signal) — accumulate consecutive `-1`s so the host can
	// detect a slow client; reset to 0 on any non-backpressure return.
	let framesSent = 0;
	let slowSendsRecent = 0;
	const send = (frame: ServerFrame) => {
		const ret = rawSend(frame);
		if (ret === -1) {
			slowSendsRecent += 1;
		} else {
			framesSent += 1;
			slowSendsRecent = 0;
		}
	};

	// Diffs from one atomic batch (a mutation, or a single applyChange) arrive via
	// onDiff synchronously and share a version. Buffer them and flush as one
	// message: a lone diff stays a plain `diff` (so single-collection clients are
	// unchanged); several become one `frame` the client applies atomically.
	let pending: FrameDiff[] = [];
	let pendingVersion: number | undefined;
	// 1.18.0: the cursor that came alongside the in-flight diff batch. We
	// forward it on the wire so clients can resume across cluster shards.
	let pendingCursor: string | undefined;
	let flushScheduled = false;

	const flush = () => {
		if (pending.length === 0) {
			return;
		}
		const diffs = pending;
		const version = pendingVersion;
		const cursor = pendingCursor;
		pending = [];
		pendingVersion = undefined;
		pendingCursor = undefined;
		if (diffs.length === 1) {
			const only = diffs[0]!;
			send({
				type: 'diff',
				id: only.id,
				added: only.added,
				removed: only.removed,
				changed: only.changed,
				version,
				cursor
			});
		} else {
			send({ type: 'frame', diffs, version, cursor });
		}
	};

	const scheduleFlush = () => {
		if (flushScheduled) {
			return;
		}
		flushScheduled = true;
		queueMicrotask(() => {
			flushScheduled = false;
			flush();
		});
	};

	const bufferDiff = (
		diff: FrameDiff,
		diffVersion: number,
		cursor?: string
	) => {
		// A new version means a new batch — flush the previous one first.
		if (pending.length > 0 && pendingVersion !== diffVersion) {
			flush();
		}
		pending.push(diff);
		pendingVersion = diffVersion;
		// Cursor for the in-flight batch — same cursor for every diff sharing
		// `diffVersion` (the engine emits them with one currentCursor() call).
		if (cursor !== undefined) pendingCursor = cursor;
		scheduleFlush();
	};

	const handle = async (raw: unknown) => {
		const frame = parseFrame(raw, serializer);
		if (frame === undefined) {
			send({ type: 'error', message: 'Malformed sync frame' });
			return;
		}

		if (frame.type === 'mutate') {
			try {
				const result = await engine.runMutation(
					frame.name,
					frame.args,
					ctx
				);
				// The mutation's diffs were buffered during runMutation; flush them
				// (as one frame) before the ack so the ack always arrives after.
				flush();
				send({ type: 'ack', mutationId: frame.mutationId, result });
			} catch (error) {
				send({
					type: 'reject',
					mutationId: frame.mutationId,
					message:
						error instanceof Error ? error.message : String(error)
				});
			}
			return;
		}

		if (frame.type === 'unsubscribe') {
			subscriptions.get(frame.id)?.unsubscribe();
			subscriptions.delete(frame.id);
			return;
		}

		if (frame.type === 'presence-join') {
			if (presence === undefined) {
				send({ type: 'error', message: 'Presence is not enabled' });
				return;
			}
			// A re-join replaces the prior membership for this room.
			presenceRooms.get(frame.room)?.leave();
			const handle = presence.join(
				frame.room,
				frame.memberId,
				frame.state,
				(diff) => {
					send({
						type: 'presence',
						room: frame.room,
						joined: diff.joined,
						updated: diff.updated,
						left: diff.left
					});
				}
			);
			presenceRooms.set(frame.room, handle);
			// Initial snapshot to the joiner (peers got a `joined` diff instead).
			send({
				type: 'presence',
				room: frame.room,
				joined: handle.members,
				updated: [],
				left: []
			});
			return;
		}

		if (frame.type === 'presence-set') {
			presenceRooms.get(frame.room)?.set(frame.state);
			return;
		}

		if (frame.type === 'presence-leave') {
			presenceRooms.get(frame.room)?.leave();
			presenceRooms.delete(frame.room);
			return;
		}

		if (subscriptions.has(frame.id)) {
			send({
				type: 'error',
				id: frame.id,
				message: `Subscription id "${frame.id}" already in use`
			});
			return;
		}

		try {
			const subscription = await engine.subscribe({
				collection: frame.collection,
				params: frame.params,
				ctx,
				since: frame.since,
				onDiff: (diff, diffVersion, cursor) => {
					bufferDiff(
						{
							id: frame.id,
							added: diff.added,
							removed: diff.removed,
							changed: diff.changed
						},
						diffVersion,
						cursor
					);
				}
			});
			subscriptions.set(frame.id, subscription);
			// No await between subscribe resolving and this send, so the initial
			// reply always precedes any diff for this subscription.
			if (subscription.catchup !== undefined) {
				// Resumed: a catch-up diff applied on top of the client's set.
				send({
					type: 'diff',
					id: frame.id,
					added: subscription.catchup.added,
					removed: subscription.catchup.removed,
					changed: subscription.catchup.changed,
					version: subscription.version,
					cursor: subscription.cursor
				});
			} else {
				send({
					type: 'snapshot',
					id: frame.id,
					rows: subscription.initial,
					version: subscription.version,
					cursor: subscription.cursor
				});
			}
		} catch (error) {
			send({
				type: 'error',
				id: frame.id,
				message: error instanceof Error ? error.message : String(error)
			});
		}
	};

	const close = () => {
		for (const subscription of subscriptions.values()) {
			subscription.unsubscribe();
		}
		subscriptions.clear();
		// Drop this connection's presence so peers see it leave (auto-cleanup).
		for (const handle of presenceRooms.values()) {
			handle.leave();
		}
		presenceRooms.clear();
	};

	const stats = (): SyncConnectionStats => ({
		framesSent,
		presenceRoomCount: presenceRooms.size,
		slowSendsRecent,
		subscriptionCount: subscriptions.size
	});

	return { close, handle, stats };
};
