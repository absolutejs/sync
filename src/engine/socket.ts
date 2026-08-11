import { Elysia } from 'elysia';
import { createSyncConnection } from './connection';
import type { SyncConnection, SyncConnectionStats } from './connection';
import type { PresenceHub } from './presence';
import type { SyncEngine } from './syncEngine';
import type { FrameSerializer } from '../serializer';
import { jsonSerializer } from '../serializer';

/**
 * Diagnostic surfaced via {@link SyncSocketOptions.onSlow} when a connection
 * trips the WS backpressure threshold. The host can log, kick, or charge the
 * tenant extra via the meter.
 */
export type SlowConnectionEvent = {
	/** Stable per-connection id from Elysia's `ws.id`. */
	wsId: string;
	/** Bytes the WS currently has queued waiting to send. */
	bufferedAmount: number;
	/** Per-connection counters at the moment of detection. */
	stats: SyncConnectionStats;
	/** Why the event fired. */
	reason: 'buffer-threshold' | 'send-backpressure';
};

export type SyncSocketDrainOptions = {
	/** WebSocket close code. Defaults to 1012 (Service Restart). */
	code?: number;
	/** Human-readable close reason. Defaults to `Service Restart`. */
	reason?: string;
};

/**
 * Host-owned control plane for a {@link syncSocket}. Calling `drain()` closes
 * every current connection with a protocol-level restart frame and rejects
 * later connections the same way. Clients can reconnect through a switched
 * load balancer without seeing an unclean 1006 transport failure.
 */
export type SyncSocketController = {
	/** Whether this socket endpoint has permanently entered drain mode. */
	readonly draining: boolean;
	/** Number of currently tracked WebSocket connections. */
	connectionCount: () => number;
	/** Enter drain mode and return the number of connections asked to close. */
	drain: (options?: SyncSocketDrainOptions) => number;
};

type DrainableSocket = {
	close?: (code?: number, reason?: string) => void;
};

type SyncSocketControllerState = {
	draining: boolean;
	sockets: Map<string, DrainableSocket>;
};

const controllerStates = new WeakMap<
	SyncSocketController,
	SyncSocketControllerState
>();

export const createSyncSocketController = (): SyncSocketController => {
	const state: SyncSocketControllerState = {
		draining: false,
		sockets: new Map()
	};
	const controller: SyncSocketController = {
		get draining() {
			return state.draining;
		},
		connectionCount: () => state.sockets.size,
		drain: ({ code = 1012, reason = 'Service Restart' } = {}) => {
			state.draining = true;
			const sockets = [...state.sockets.values()];
			for (const socket of sockets) {
				try {
					socket.close?.(code, reason);
				} catch {
					// The runtime may have closed the socket between the snapshot and
					// this call. Its close callback removes it from the controller.
				}
			}
			return sockets.length;
		}
	};
	controllerStates.set(controller, state);
	return controller;
};

export type SyncSocketOptions = {
	/** The sync engine whose collections this socket serves. */
	engine: SyncEngine;
	/** Optional host control plane for graceful blue-green deployment drains. */
	controller?: SyncSocketController;
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
	/**
	 * Bytes threshold for the per-connection WS send buffer. When
	 * `ws.getBufferedAmount()` exceeds this, `onSlow` fires once per
	 * crossing. Default `Infinity` (disabled).
	 *
	 * Added in 1.14.0.
	 */
	maxBufferedBytes?: number;
	/**
	 * Fired when the per-connection WS buffer crosses `maxBufferedBytes`, OR
	 * when `ws.send()` returns `-1` (Bun's backpressure signal). The signal
	 * re-arms once the WS reports `drain`. Pair with `closeOnSlow: true` to
	 * kick slow clients automatically, or use this hook to charge the
	 * tenant extra via `@absolutejs/metering`.
	 *
	 * Added in 1.14.0.
	 */
	onSlow?: (event: SlowConnectionEvent) => void | Promise<void>;
	/**
	 * Close the WS the first time a connection crosses `maxBufferedBytes`
	 * (or the `-1` send threshold). Default `false`. Client will reconnect
	 * and re-hydrate.
	 *
	 * Added in 1.14.0.
	 */
	closeOnSlow?: boolean;
	/**
	 * Wire-format serializer (1.16.0). Default `jsonSerializer` —
	 * preserves the pre-1.16 behavior. Both ends of the connection MUST
	 * use the same serializer; opt into a binary one (msgpack, cbor, or
	 * a custom layout) on BOTH this plugin AND the client to cut the
	 * bandwidth + parse CPU.
	 */
	serializer?: FrameSerializer;
};

type TrackedConnection = {
	// Null until the async `resolveContext` resolves. Frames that arrive in that
	// window are buffered in `pending` and replayed once the connection exists —
	// otherwise a client that sends `subscribe` synchronously on open (every
	// @absolutejs/sync client does) loses it whenever auth is slow (a DB lookup).
	connection: SyncConnection | null;
	pending: unknown[];
	slowSignaled: boolean;
};

/**
 * Elysia WebSocket plugin for the sync engine. One socket multiplexes any
 * number of collection subscriptions: the client sends `subscribe` /
 * `unsubscribe` frames and receives `snapshot` / `diff` / `error` frames
 * (see {@link createSyncConnection}). Mount once and drive
 * `engine.applyChange` from your mutations.
 *
 * Uses Elysia's first-class `.ws()` rather than a hand-rolled stream — the
 * bidirectional channel carries both subscriptions and mutations, and
 * `ws.send` serializes frames for us.
 *
 * 1.14.0 adds WS-layer slow-client detection — see `maxBufferedBytes` /
 * `onSlow` / `closeOnSlow`.
 */
export const syncSocket = ({
	engine,
	controller,
	path = '/sync/ws',
	resolveContext,
	presence,
	maxBufferedBytes,
	onSlow,
	closeOnSlow = false,
	serializer = jsonSerializer
}: SyncSocketOptions) => {
	const connections = new Map<string, TrackedConnection>();
	const controllerState = controller
		? controllerStates.get(controller)
		: undefined;
	if (controller && !controllerState) {
		throw new Error(
			'syncSocket controller must be created with createSyncSocketController()'
		);
	}
	const threshold = maxBufferedBytes ?? Infinity;

	const fireSlow = (event: SlowConnectionEvent) => {
		if (!onSlow) return;
		try {
			const ret = onSlow(event);
			if (ret && typeof (ret as Promise<void>).then === 'function') {
				(ret as Promise<void>).catch((error) => {
					console.error('[sync/socket] onSlow rejected:', error);
				});
			}
		} catch (error) {
			console.error('[sync/socket] onSlow threw:', error);
		}
	};

	return new Elysia({ name: '@absolutejs/sync/socket' }).ws(path, {
		async open(ws) {
			// Permissive shape: we read `getBufferedAmount` + `close` if the
			// runtime supports them (Bun's ServerWebSocket does) — fall back
			// silently for test fakes. Accepts both string and Uint8Array
			// payloads (binary serializers via 1.16.0).
			const bunWs = ws as unknown as {
				id: string;
				send: (data: string | Uint8Array | ArrayBuffer) => number;
				getBufferedAmount?: () => number;
				close?: (code?: number, reason?: string) => void;
			};
			if (controllerState?.draining) {
				bunWs.close?.(1012, 'Service Restart');
				return;
			}

			// Register synchronously BEFORE awaiting `resolveContext`, so frames
			// that arrive during the (possibly slow) auth resolve are buffered in
			// `pending` rather than dropped (the `message` handler queues them
			// when `connection` is still null).
			const tracked: TrackedConnection = {
				connection: null,
				pending: [],
				slowSignaled: false
			};
			connections.set(bunWs.id, tracked);
			controllerState?.sockets.set(bunWs.id, bunWs);

			const ctx = resolveContext
				? await resolveContext(ws.data as Record<string, unknown>)
				: {};
			const connection = createSyncConnection({
				engine,
				ctx,
				presence,
				serializer,
				send: (frame) => {
					const payload = serializer.encodeServer(frame);
					const ret = bunWs.send(
						typeof payload === 'string'
							? payload
							: (payload as Uint8Array)
					);
					const buffered = bunWs.getBufferedAmount?.() ?? 0;

					const overBuffer = buffered > threshold;
					const backpressure = ret === -1;
					if ((overBuffer || backpressure) && !tracked.slowSignaled) {
						tracked.slowSignaled = true;
						fireSlow({
							bufferedAmount: buffered,
							reason: backpressure
								? 'send-backpressure'
								: 'buffer-threshold',
							stats: connection.stats(),
							wsId: bunWs.id
						});
						if (closeOnSlow) bunWs.close?.();
					}
					return ret;
				}
			});

			// The socket may have closed while we were awaiting auth.
			if (!connections.has(bunWs.id)) {
				connection.close();
				return;
			}
			tracked.connection = connection;
			// Replay anything that arrived before auth resolved, in order.
			const buffered = tracked.pending;
			tracked.pending = [];
			for (const frame of buffered) {
				await connection.handle(frame);
			}
		},
		async message(ws, message) {
			const tracked = connections.get(ws.id);
			if (!tracked) return;
			if (tracked.connection) {
				await tracked.connection.handle(message);
			} else {
				tracked.pending.push(message);
			}
		},
		drain(ws) {
			// WS buffer cleared — re-arm slow-client detection so the next
			// over-threshold event fires onSlow again.
			const tracked = connections.get(ws.id);
			if (tracked) tracked.slowSignaled = false;
		},
		close(ws) {
			const tracked = connections.get(ws.id);
			if (tracked) {
				tracked.connection?.close();
				connections.delete(ws.id);
				controllerState?.sockets.delete(ws.id);
			}
		}
	});
};
