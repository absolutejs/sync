import { Elysia } from 'elysia';
import { websocket } from 'elysia/websocket';
import { createSyncConnection } from './connection';
import type { SyncConnection, SyncConnectionStats } from './connection';
import type { PresenceHub } from './presence';
import type { SyncEngine } from './syncEngine';
import type { FrameSerializer } from '../serializer';
import { jsonSerializer } from '../serializer';
import {
	headlessSyncRoute,
	type HeadlessSyncRouteOptions,
	type SyncRouteContext
} from './routes';

const DEFAULT_HEADLESS_PATH = '/__absolute/sync/background';
const DEFAULT_HEADLESS_PRINCIPAL_PATH = '/__absolute/sync/principal';
const TICKET_AUTH_QUERY = '__absolute_auth';

type AbsoluteAuthSyncBridge = {
	consumeSocketTicket: (input: {
		audience?: string;
		ticket: string;
	}) => Promise<unknown | undefined>;
	resolveBearer: (input: {
		authorization?: string;
	}) => Promise<unknown | undefined>;
	resolveSession?: (input: { authPrincipal?: unknown }) => Promise<
		| {
				context: unknown;
				namespace: string;
		  }
		| undefined
	>;
};

type SyncAuthenticator = (
	ticket: string,
	data: Record<string, unknown>
) => unknown | Promise<unknown>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' && value !== null;

const readAuthBridge = (
	context: Record<string, unknown>
): AbsoluteAuthSyncBridge | undefined => {
	const bridge = context.absoluteAuthSync;
	if (
		!isRecord(bridge) ||
		typeof bridge.consumeSocketTicket !== 'function' ||
		typeof bridge.resolveBearer !== 'function'
	)
		return undefined;

	return bridge as AbsoluteAuthSyncBridge;
};

const readAuthorization = (context: Record<string, unknown>) => {
	const headers = context.headers;
	if (isRecord(headers) && typeof headers.authorization === 'string')
		return headers.authorization;
	const request = context.request;
	if (request instanceof Request)
		return request.headers.get('authorization') ?? undefined;

	return undefined;
};

const requestsTicketAuthentication = (context: Record<string, unknown>) => {
	const query = context.query;

	return isRecord(query) && query[TICKET_AUTH_QUERY] === 'ticket';
};

export type SyncHeadlessOptions = Omit<
	HeadlessSyncRouteOptions<unknown>,
	'resolveContext'
> & {
	/** Finite HTTP route. Defaults to `/__absolute/sync/background`. */
	path?: string;
	/** Opaque browser-principal bootstrap route. Defaults to
	 * `/__absolute/sync/principal`. Set false to disable cookie-session PWA
	 * background Sync while retaining Bearer/native work. */
	principalPath?: false | string;
	/** Custom bearer-to-application-context mapping. */
	resolveContext?: HeadlessSyncRouteOptions<unknown>['resolveContext'];
};

const sameOriginSessionRequest = (context: SyncRouteContext) => {
	const request = context.request;
	if (!(request instanceof Request)) return false;
	if (request.method !== 'POST') return false;
	if (
		!request.headers
			.get('content-type')
			?.toLowerCase()
			.startsWith('application/json')
	)
		return false;
	const origin = request.headers.get('origin');
	if (!origin) return false;
	let targetOrigin: string;
	try {
		targetOrigin = new URL(request.url).origin;
	} catch {
		return false;
	}
	if (origin !== targetOrigin) return false;
	const fetchSite = request.headers.get('sec-fetch-site');
	return fetchSite === null || fetchSite === 'same-origin';
};

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
	/** Optional message-level authentication for browser/native clients that
	 * cannot attach Authorization headers to a WebSocket upgrade. The first
	 * frame must contain a short-lived single-use ticket. */
	authenticate?: (
		ticket: string,
		data: Record<string, unknown>
	) => unknown | Promise<unknown>;
	/** Time allowed for the first authentication frame. Defaults to 10 seconds. */
	authenticationTimeoutMs?: number;
	/**
	 * Automatically mounted finite HTTP transport for native/PWA background
	 * work. Set `false` to opt out. Defaults to the managed background path.
	 */
	headless?: false | SyncHeadlessOptions;
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
	activate: (ctx: unknown) => Promise<void>;
	authenticate?: SyncAuthenticator;
	authenticating?: Promise<void>;
	authenticationTimer?: ReturnType<typeof setTimeout>;
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
	authenticate,
	authenticationTimeoutMs = 10_000,
	engine,
	controller,
	path = '/sync/ws',
	resolveContext,
	headless,
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
	const headlessOptions = headless === false ? undefined : (headless ?? {});
	const headlessContext = async (context: SyncRouteContext) => {
		const authorization = readAuthorization(context);
		const bearer = /^Bearer [^\s]+$/iu.test(authorization ?? '');
		const authBridge = readAuthBridge(context);
		let resolved: unknown;
		if (bearer) {
			resolved = headlessOptions?.resolveContext
				? await headlessOptions.resolveContext(context)
				: resolveContext
					? await resolveContext(context)
					: await authBridge?.resolveBearer({ authorization });
		} else if (
			authBridge?.resolveSession &&
			sameOriginSessionRequest(context)
		) {
			resolved = (
				await authBridge.resolveSession({
					authPrincipal: context.authPrincipal
				})
			)?.context;
		}
		if (resolved === undefined || resolved === null)
			throw new Error('ABSOLUTE_SYNC_UNAUTHORIZED');

		return resolved;
	};
	const finiteHandler = headlessOptions
		? headlessSyncRoute(engine, {
				resolveContext: headlessContext,
				...(headlessOptions.maxMutations === undefined
					? {}
					: { maxMutations: headlessOptions.maxMutations }),
				...(headlessOptions.maxPulls === undefined
					? {}
					: { maxPulls: headlessOptions.maxPulls })
			})
		: undefined;
	if (
		!Number.isFinite(authenticationTimeoutMs) ||
		authenticationTimeoutMs <= 0
	)
		throw new TypeError('authenticationTimeoutMs must be positive');

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

	const app = new Elysia({ name: '@absolutejs/sync/socket' })
		// Elysia 2 no longer bundles WebSocket support; without this the
		// route below is silently never served.
		.use(websocket())
		.ws(path, {
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
					activate: async () => undefined,
					connection: null,
					pending: [],
					slowSignaled: false
				};
				connections.set(bunWs.id, tracked);
				controllerState?.sockets.set(bunWs.id, bunWs);
				const socketContext = ws as unknown as Record<string, unknown>;
				const authBridge = readAuthBridge(socketContext);
				tracked.authenticate =
					authenticate ??
					(requestsTicketAuthentication(socketContext) && authBridge
						? async (ticket) => {
								const context =
									await authBridge.consumeSocketTicket({
										ticket
									});
								if (context === undefined)
									throw new Error(
										'Invalid Absolute Auth socket ticket'
									);

								return context;
							}
						: undefined);

				tracked.activate = async (ctx) => {
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
							if (
								(overBuffer || backpressure) &&
								!tracked.slowSignaled
							) {
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
					if (!connections.has(bunWs.id)) {
						connection.close();
						return;
					}
					tracked.connection = connection;
					const buffered = tracked.pending;
					tracked.pending = [];
					for (const frame of buffered)
						await connection.handle(frame);
				};

				if (
					requestsTicketAuthentication(socketContext) &&
					!tracked.authenticate
				) {
					bunWs.close?.(4401, 'Authentication Unavailable');
					return;
				}
				if (tracked.authenticate) {
					tracked.authenticationTimer = setTimeout(() => {
						if (!tracked.connection)
							bunWs.close?.(4401, 'Authentication Timeout');
					}, authenticationTimeoutMs);
					return;
				}
				const ctx = resolveContext
					? // Elysia 2 hands the route context straight to the handler; the
						// upgrade data that used to sit under `ws.data` is now spread on
						// it, and `data` itself holds internal connection state instead.
						await resolveContext(
							ws as unknown as Record<string, unknown>
						)
					: {};
				await tracked.activate(ctx);
			},
			async message(ws, message) {
				const tracked = connections.get(ws.id);
				if (!tracked) return;
				if (tracked.connection) {
					await tracked.connection.handle(message);
				} else if (tracked.authenticate && !tracked.authenticating) {
					const decoded = serializer.decode(message);
					if (
						typeof decoded !== 'object' ||
						decoded === null ||
						Reflect.get(decoded, 'type') !== 'authenticate' ||
						typeof Reflect.get(decoded, 'ticket') !== 'string'
					) {
						(
							ws as unknown as {
								close?: (
									code?: number,
									reason?: string
								) => void;
							}
						).close?.(4401, 'Authentication Required');
						return;
					}
					const authenticateTicket = tracked.authenticate;
					tracked.authenticating = (async () => {
						try {
							const ctx = await authenticateTicket(
								Reflect.get(decoded, 'ticket') as string,
								ws as unknown as Record<string, unknown>
							);
							await tracked.activate(ctx);
							clearTimeout(tracked.authenticationTimer);
							tracked.authenticationTimer = undefined;
						} catch {
							(
								ws as unknown as {
									close?: (
										code?: number,
										reason?: string
									) => void;
								}
							).close?.(4401, 'Authentication Failed');
						}
					})();
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
					clearTimeout(tracked.authenticationTimer);
					tracked.connection?.close();
					connections.delete(ws.id);
					controllerState?.sockets.delete(ws.id);
				}
			}
		});

	if (finiteHandler && headlessOptions) {
		const principalPath =
			headlessOptions.principalPath === false
				? undefined
				: (headlessOptions.principalPath ??
					DEFAULT_HEADLESS_PRINCIPAL_PATH);
		if (principalPath) {
			app.post(principalPath, async (context) => {
				const routeContext = context as unknown as SyncRouteContext;
				context.set.headers['cache-control'] = 'no-store';
				if (!sameOriginSessionRequest(routeContext))
					return context.status(
						'Unauthorized',
						'Sync authentication required'
					);
				try {
					const resolved = await readAuthBridge(
						context as unknown as Record<string, unknown>
					)?.resolveSession?.({
						authPrincipal: routeContext.authPrincipal
					});
					if (!resolved)
						return context.status(
							'Unauthorized',
							'Sync authentication required'
						);

					return {
						namespace: resolved.namespace,
						version: 1 as const
					};
				} catch {
					return context.status(
						'Unauthorized',
						'Sync authentication required'
					);
				}
			});
		}
		app.post(
			headlessOptions.path ?? DEFAULT_HEADLESS_PATH,
			async (context) => {
				try {
					const response = await finiteHandler(
						context as unknown as SyncRouteContext
					);
					context.set.headers['cache-control'] = 'no-store';

					return response;
				} catch (error) {
					if (
						error instanceof Error &&
						error.message === 'ABSOLUTE_SYNC_UNAUTHORIZED'
					) {
						context.set.headers['cache-control'] = 'no-store';
						context.set.headers['www-authenticate'] = 'Bearer';

						return context.status(
							'Unauthorized',
							'Sync authentication required'
						);
					}
					throw error;
				}
			}
		);
	}

	return app;
};
