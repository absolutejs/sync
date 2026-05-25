import type { PresenceHandle, PresenceHub, PresenceMember } from './presence';
import type { Subscription, SyncEngine } from './syncEngine';

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
			/** Resume from a version already applied (catch-up instead of snapshot). */
			since?: number;
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

/** Server → client. `version` is the change-feed watermark this frame brings. */
export type ServerFrame<T = unknown> =
	| { type: 'snapshot'; id: string; rows: T[]; version?: number }
	| {
			type: 'diff';
			id: string;
			added: T[];
			removed: T[];
			changed: T[];
			version?: number;
	  }
	| {
			// One atomic batch (e.g. a transactional mutation) that touched several
			// subscriptions — bundled into one message so the client applies them in
			// a single frame, never showing a torn cross-collection intermediate.
			type: 'frame';
			version?: number;
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
	/** Send a frame to the client (the transport serializes it). */
	send: (frame: ServerFrame) => void;
	/** Optional presence hub; enables the `presence-*` frames (see createPresenceHub). */
	presence?: PresenceHub;
};

export type SyncConnection = {
	/** Handle one client frame (a parsed object or a raw JSON string). */
	handle: (raw: unknown) => Promise<void>;
	/** Tear down every subscription on this connection (call on socket close). */
	close: () => void;
};

const parseFrame = (raw: unknown): ClientFrame | undefined => {
	let value: unknown = raw;
	if (typeof value === 'string') {
		try {
			value = JSON.parse(value);
		} catch {
			return undefined;
		}
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
		return typeof frame.id === 'string' &&
			typeof frame.collection === 'string'
			? {
					type: 'subscribe',
					id: frame.id,
					collection: frame.collection,
					params: frame.params,
					since:
						typeof frame.since === 'number'
							? frame.since
							: undefined
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
	send,
	presence
}: SyncConnectionOptions): SyncConnection => {
	const subscriptions = new Map<string, Subscription<unknown>>();
	// This connection's presence memberships (one per room), torn down on close.
	const presenceRooms = new Map<string, PresenceHandle<unknown>>();

	// Diffs from one atomic batch (a mutation, or a single applyChange) arrive via
	// onDiff synchronously and share a version. Buffer them and flush as one
	// message: a lone diff stays a plain `diff` (so single-collection clients are
	// unchanged); several become one `frame` the client applies atomically.
	let pending: FrameDiff[] = [];
	let pendingVersion: number | undefined;
	let flushScheduled = false;

	const flush = () => {
		if (pending.length === 0) {
			return;
		}
		const diffs = pending;
		const version = pendingVersion;
		pending = [];
		pendingVersion = undefined;
		if (diffs.length === 1) {
			const only = diffs[0]!;
			send({
				type: 'diff',
				id: only.id,
				added: only.added,
				removed: only.removed,
				changed: only.changed,
				version
			});
		} else {
			send({ type: 'frame', diffs, version });
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

	const bufferDiff = (diff: FrameDiff, diffVersion: number) => {
		// A new version means a new batch — flush the previous one first.
		if (pending.length > 0 && pendingVersion !== diffVersion) {
			flush();
		}
		pending.push(diff);
		pendingVersion = diffVersion;
		scheduleFlush();
	};

	const handle = async (raw: unknown) => {
		const frame = parseFrame(raw);
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
				onDiff: (diff, diffVersion) => {
					bufferDiff(
						{
							id: frame.id,
							added: diff.added,
							removed: diff.removed,
							changed: diff.changed
						},
						diffVersion
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
					version: subscription.version
				});
			} else {
				send({
					type: 'snapshot',
					id: frame.id,
					rows: subscription.initial,
					version: subscription.version
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

	return { handle, close };
};
