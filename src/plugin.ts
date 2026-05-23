import { Elysia } from 'elysia';
import type { ReactiveEvent, ReactiveHub } from './reactiveHub';

export type SyncRequestContext = {
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
	resolveTopics?: (context: SyncRequestContext) => string[];
	/**
	 * Server→client heartbeat comment, so idle proxies don't drop the SSE stream.
	 * Defaults to 25000ms.
	 */
	heartbeatMs?: number;
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
}: SyncPluginOptions) =>
	new Elysia({ name: '@absolutejs/sync' }).get(path, (context) => {
		const topics = resolveTopics({
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
					topic: '@absolutejs/sync:open',
					at: Date.now(),
					payload: { topics }
				});

				const unsubscribe =
					topics.length > 0 ? hub.subscribe(topics, send) : () => {};
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
				'content-type': 'text/event-stream'
			}
		});
	});
