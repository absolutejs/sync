import { Elysia } from 'elysia';
import { createSyncConnection } from './connection';
import type { SyncConnection } from './connection';
import type { PresenceHub } from './presence';
import type { SyncEngine } from './syncEngine';

export type SyncSocketOptions = {
	/** The sync engine whose collections this socket serves. */
	engine: SyncEngine;
	/** WebSocket route. Defaults to `/sync/ws`. */
	path?: string;
	/** Optional presence hub; enables `presence-*` frames on this socket. */
	presence?: PresenceHub;
	/**
	 * Build the per-connection auth context from the upgrade request data
	 * (`ws.data`: query, headers, cookies, and anything you `derive`d/`resolve`d
	 * earlier in the chain). Whatever you return is the `ctx` passed to every
	 * collection's `authorize`/`hydrate`/`match`. Defaults to an empty object.
	 */
	resolveContext?: (
		data: Record<string, unknown>
	) => unknown | Promise<unknown>;
};

/**
 * Elysia WebSocket plugin for the Tier 3 sync engine. One socket multiplexes any
 * number of collection subscriptions: the client sends `subscribe`/`unsubscribe`
 * frames and receives `snapshot`/`diff`/`error` frames (see
 * {@link createSyncConnection}). Mount it once and drive `engine.applyChange`
 * from your mutations.
 *
 * Uses Elysia's first-class `.ws()` rather than a hand-rolled stream — the
 * bidirectional channel carries both subscriptions and (later) mutations, and
 * `ws.send` serializes frames for us.
 */
export const syncSocket = ({
	engine,
	path = '/sync/ws',
	resolveContext,
	presence
}: SyncSocketOptions) => {
	const connections = new Map<string, SyncConnection>();

	return new Elysia({ name: '@absolutejs/sync/socket' }).ws(path, {
		async open(ws) {
			const ctx = resolveContext
				? await resolveContext(ws.data as Record<string, unknown>)
				: {};
			connections.set(
				ws.id,
				createSyncConnection({
					engine,
					ctx,
					presence,
					send: (frame) => {
						ws.send(frame);
					}
				})
			);
		},
		async message(ws, message) {
			await connections.get(ws.id)?.handle(message);
		},
		close(ws) {
			connections.get(ws.id)?.close();
			connections.delete(ws.id);
		}
	});
};
