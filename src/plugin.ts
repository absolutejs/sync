import { Elysia } from 'elysia';
import { SYNC_OPEN_TOPIC } from './reactiveHub';
import type { ReactiveEvent, ReactiveHub } from './reactiveHub';

export type SyncRequestContext = {
	cookies: Record<string, unknown>;
	query: Record<string, string | undefined>;
	request: Request;
};

export type SyncPluginOptions = {
	hub: ReactiveHub;
	/** Route the SSE stream is served from. Defaults to `/sync`. */
	path?: string;
	/**
	 * Which topics a connection subscribes to. Defaults to a comma-separated
	 * `?topics=a,b,c` query param. Override to derive topics from the session,
	 * params, or auth instead of trusting the client.
	 */
	resolveTopics?: (
		context: SyncRequestContext
	) => Promise<string[]> | string[];
	/**
	 * Server→client heartbeat comment, so idle proxies don't drop the SSE stream.
	 * Defaults to 25000ms.
	 */
	heartbeatMs?: number;
};

const parseCookieHeader = (header: string | null) => {
	const cookies: Record<string, string> = {};
	if (!header) return cookies;
	for (const part of header.split(';')) {
		const index = part.indexOf('=');
		if (index === -1) continue;
		const name = part.slice(0, index).trim();
		if (!name) continue;
		const raw = part.slice(index + 1).trim();
		try {
			cookies[name] = decodeURIComponent(raw);
		} catch {
			cookies[name] = raw;
		}
	}

	return cookies;
};

const defaultResolveTopics = (context: SyncRequestContext) =>
	(context.query.topics ?? '')
		.split(',')
		.map((topic) => topic.trim())
		.filter(Boolean);

/**
 * Elysia plugin that streams {@link ReactiveHub} events to browsers over Server-Sent
 * Events. Mount it once, point {@link createSyncSubscriber} at the same path, and
 * `hub.publish(topic)` from your mutations — subscribed clients are notified the
 * moment data changes, so they can refetch (or read the pushed payload) instead of
 * polling on a timer.
 */
export const sync = ({
	hub,
	path = '/sync',
	resolveTopics = defaultResolveTopics,
	heartbeatMs = 25_000
}: SyncPluginOptions) => {
	const app = new Elysia({ name: '@absolutejs/sync' }).get(
		path,
		async (context) => {
			// Elysia 2 materializes undeclared cookies lazily, so the typed
			// cookie map can be empty on this schema-less route. Seed from the
			// raw Cookie header and let any declared cookie override it.
			const cookies: Record<string, unknown> = parseCookieHeader(
				context.request.headers.get('cookie')
			);
			for (const [name, cookie] of Object.entries(context.cookie))
				if (cookie?.value !== undefined) cookies[name] = cookie.value;
			const topics = await resolveTopics({
				cookies,
				query: context.query as Record<string, string | undefined>,
				request: context.request
			});
			const encoder = new TextEncoder();

			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					const write = (chunk: string) => {
						try {
							controller.enqueue(encoder.encode(chunk));
						} catch {
							// controller already closed by an abort race
						}
					};
					const send = (event: ReactiveEvent) => {
						write(`data: ${JSON.stringify(event)}\n\n`);
					};

					send({
						topic: SYNC_OPEN_TOPIC,
						at: Date.now(),
						payload: { topics }
					});

					const unsubscribe =
						topics.length > 0
							? hub.subscribe(topics, send)
							: () => {};
					const heartbeat = setInterval(
						() => write(': ping\n\n'),
						heartbeatMs
					);

					context.request.signal.addEventListener(
						'abort',
						() => {
							clearInterval(heartbeat);
							unsubscribe();
							try {
								controller.close();
							} catch {
								// already closed
							}
						},
						{ once: true }
					);
				}
			});

			return new Response(stream, {
				headers: {
					'cache-control': 'no-cache, no-transform',
					connection: 'keep-alive',
					'content-type': 'text/event-stream',
					// Tell nginx (and other reverse proxies) not to buffer the
					// stream — without this it holds chunks back and the SSE
					// connection tears (ERR_INCOMPLETE_CHUNKED_ENCODING).
					'x-accel-buffering': 'no'
				}
			});
		}
	);

	// The single SSE route uses a CONFIGURABLE path, so Elysia keys it by
	// `string` — a string-indexed route that would pollute a consumer's
	// whole-server type and is reached via EventSource, not Eden. The honest
	// public type is a base Elysia (no typed surface to preserve).
	// eslint-disable-next-line @typescript-eslint/consistent-type-assertions -- see comment above
	return app as unknown as Elysia;
};
