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
	| { type: 'mutate'; mutationId: number; name: string; args?: unknown };

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
	| { type: 'error'; id?: string; message: string }
	| { type: 'ack'; mutationId: number; result?: unknown }
	| { type: 'reject'; mutationId: number; message: string };

export type SyncConnectionOptions = {
	engine: SyncEngine;
	/** Resolved auth context for this connection; passed to every subscribe. */
	ctx: unknown;
	/** Send a frame to the client (the transport serializes it). */
	send: (frame: ServerFrame) => void;
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
	send
}: SyncConnectionOptions): SyncConnection => {
	const subscriptions = new Map<string, Subscription<unknown>>();

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
				// The mutation's diffs were sent during runMutation (over the same
				// ordered socket), so the ack arrives after them.
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
					send({
						type: 'diff',
						id: frame.id,
						added: diff.added,
						removed: diff.removed,
						changed: diff.changed,
						version: diffVersion
					});
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
	};

	return { handle, close };
};
