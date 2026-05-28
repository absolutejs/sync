/**
 * `syncCdc` Elysia plugin — exposes {@link SyncEngine.streamChanges} as a
 * Server-Sent Events route. Use this to feed an external CDC consumer
 * (Kafka producer, search indexer, audit pipeline) over plain HTTP, no
 * WebSocket required.
 *
 * Each delivered change becomes one SSE event:
 *
 *   id: 1234
 *   event: change
 *   data: {"version":1234,"table":"users","change":{"op":"insert","row":{...}}}
 *
 * Consumers track their cursor via the `id` field; on reconnect the browser
 * `EventSource` automatically sends a `Last-Event-ID` header, and this route
 * reads it (or the `?since=N` query param) to resume from the right place.
 *
 * Heartbeats keep the connection alive across idle-proxy timeouts. If the
 * consumer falls so far behind that the engine's in-flight buffer overflows,
 * the engine throws {@link CdcConsumerSlowError}; we forward it as a
 * `error` SSE event so the client knows to resubscribe (vs silently
 * dropping commits).
 */

import type { Elysia as ElysiaType } from 'elysia';
import {
	CdcConsumerSlowError,
	MissedChangesError,
	type LoggedChange,
	type SyncEngine
} from './syncEngine';

// Lazy Elysia loader. Elysia is a peer dependency of `@absolutejs/sync`, and
// callers of `syncCdc` always have it installed (it's an Elysia plugin). The
// reason for the indirection is that this file lives in the engine subpath
// barrel (`@absolutejs/sync/engine`), and any top-level `import { Elysia }
// from 'elysia'` would be hoisted by the bundler so that *every* engine
// subpath consumer eagerly evaluates Elysia at module load — even consumers
// that don't use `syncCdc` (e.g. sync packs in `@absolutejs/sync-packs/`).
// Resolving Elysia on first call instead keeps the engine subpath dependency-
// free at module-load time.
let cachedElysia: typeof ElysiaType | undefined;
const loadElysia = (): typeof ElysiaType => {
	if (cachedElysia !== undefined) return cachedElysia;
	// `require()` resolves synchronously in Bun and Node (via CJS interop),
	// runs at *call* time, and the bundler does not hoist it.
	const mod = (require as (id: string) => unknown)('elysia') as {
		Elysia: typeof ElysiaType;
	};
	cachedElysia = mod.Elysia;
	return cachedElysia;
};

export type SyncCdcOptions = {
	/** The engine whose change log this route streams. */
	engine: SyncEngine;
	/** Route path. Defaults to `/sync/cdc`. */
	path?: string;
	/** Heartbeat comment interval (ms) so idle proxies don't drop us. Default 25000. */
	heartbeatMs?: number;
	/** Per-stream in-flight buffer cap. Passed to {@link SyncEngine.streamChanges}. Default 10000. */
	maxBuffer?: number;
};

const parseSince = (
	query: Record<string, string | undefined>,
	lastEventId: string | null
): number => {
	const raw = query.since ?? lastEventId ?? '0';
	const parsed = Number(raw);
	return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : 0;
};

const encodeEvent = (
	event: string,
	id: number | null,
	data: unknown
): string => {
	const parts: string[] = [];
	if (id !== null) parts.push(`id: ${id}`);
	parts.push(`event: ${event}`);
	parts.push(`data: ${JSON.stringify(data)}`);
	return `${parts.join('\n')}\n\n`;
};

export const syncCdc = ({
	engine,
	path = '/sync/cdc',
	heartbeatMs = 25_000,
	maxBuffer = 10_000
}: SyncCdcOptions) => {
	const Elysia = loadElysia();
	return new Elysia({ name: '@absolutejs/sync/cdc' }).get(path, (context) => {
		const lastEventId = context.request.headers.get('last-event-id');
		const since = parseSince(
			context.query as Record<string, string | undefined>,
			lastEventId
		);
		const encoder = new TextEncoder();

		const stream = new ReadableStream<Uint8Array>({
			async start(controller) {
				const write = (chunk: string) => {
					try {
						controller.enqueue(encoder.encode(chunk));
					} catch {
						// controller already closed by an abort race
					}
				};

				write(
					encodeEvent('open', null, {
						since,
						at: Date.now()
					})
				);

				const heartbeat = setInterval(
					() => write(': ping\n\n'),
					heartbeatMs
				);

				try {
					for await (const entry of engine.streamChanges({
						since,
						signal: context.request.signal,
						maxBuffer
					})) {
						write(
							encodeEvent(
								'change',
								entry.version,
								entry satisfies LoggedChange
							)
						);
					}
				} catch (error) {
					if (error instanceof MissedChangesError) {
						write(
							encodeEvent('error', null, {
								name: 'MissedChangesError',
								message: error.message,
								requestedSince: error.requestedSince,
								availableSince: error.availableSince
							})
						);
					} else if (error instanceof CdcConsumerSlowError) {
						write(
							encodeEvent('error', null, {
								name: 'CdcConsumerSlowError',
								message: error.message,
								lastDeliveredVersion: error.lastDeliveredVersion
							})
						);
					} else {
						write(
							encodeEvent('error', null, {
								name:
									error instanceof Error
										? error.name
										: 'Error',
								message:
									error instanceof Error
										? error.message
										: String(error)
							})
						);
					}
				} finally {
					clearInterval(heartbeat);
					try {
						controller.close();
					} catch {
						// already closed
					}
				}
			}
		});

		return new Response(stream, {
			headers: {
				'cache-control': 'no-cache, no-transform',
				connection: 'keep-alive',
				'content-type': 'text/event-stream'
			}
		});
	});
};
