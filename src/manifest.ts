import {
	defineImplementation,
	defineManifest,
	toolFactory
} from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';
import type { SyncDevtoolsOptions } from './devtools';
import type { SyncEngine, SyncEngineOptions } from './engine/syncEngine';
import type { SyncPluginOptions } from './plugin';
import type { ReactiveHub } from './reactiveHub';
import type { SyncSocketOptions } from './engine/socket';

/* Composite config (v1 convention): sync ships several cooperating entry
 * points rather than one factory, so the drift-checked config type is a
 * structural record of the real exported option types — one key per tier.
 * Function/instance-valued members (hub, engine, resolveTopics,
 * resolveContext, transaction, permissions, schemas, onSlow, …) are wiring
 * concerns and never appear in settings. */
type SyncManifestConfig = {
	devtools?: SyncDevtoolsOptions;
	engine?: SyncEngineOptions;
	push?: SyncPluginOptions;
	socket?: SyncSocketOptions;
};

/* Composite runtime (v1 convention): tier 1's hub and tier 3's engine are
 * independent pieces — the host binds whichever it constructed. */
export type SyncManifestRuntime = {
	engine?: SyncEngine;
	hub?: ReactiveHub;
};

const tool = toolFactory<SyncManifestRuntime>();

const RECENT_CHANGES_LIMIT = 20;

export const manifest = defineManifest<
	SyncManifestConfig,
	SyncManifestRuntime
>()({
	contract: 2,
	identity: {
		accent: '#10b981',
		category: 'sync',
		description:
			'Reactive data over your own database and ORM — no new backend. Three tiers ship in one package: push-on-change over SSE (`createReactiveHub` + the `sync` plugin), ORM-derived topics + `createLiveQuery` (Drizzle/Prisma), and the full sync engine (`/engine`): row-level live collections over WebSocket with optimistic mutations, offline queue, declarative permissions and schemas, CRDT collaborative editing, live full-text/vector search, scheduled functions, CDC for Postgres/MySQL/SQLite, incremental joins/aggregations, point-in-time replay, and cluster fan-out. CRDT backends (`sync/crdt-adapter`) and cluster buses (`sync/cluster-bus`) are pluggable.',
		docsUrl: 'https://github.com/absolutejs/sync',
		name: '@absolutejs/sync',
		tagline: 'Live data everywhere — changes appear instantly, no refresh.'
	},
	implements: [
		defineImplementation<never>()({
			contract: 'sync/crdt-adapter',
			factory: 'rgaText',
			from: '@absolutejs/sync/crdt',
			title: 'Built-in collaborative text (zero dependencies)',
			wiring: {
				code: 'rgaText',
				imports: [{ from: '@absolutejs/sync/crdt', names: ['rgaText'] }]
			}
		}),
		defineImplementation<never>()({
			contract: 'sync/cluster-bus',
			factory: 'createInMemoryClusterBus',
			from: '@absolutejs/sync/engine',
			title: 'In process (development only — does not span machines)',
			wiring: {
				code: 'createInMemoryClusterBus()',
				imports: [
					{
						from: '@absolutejs/sync/engine',
						names: ['createInMemoryClusterBus']
					}
				]
			}
		})
	],
	lifecycle: [
		{
			/* Code-change step (v1 convention: no command): run the SQL from
			 * postgresNotifyTrigger({ tables }) against your database, then
			 * engine.connectSource(postgresChangeSource(...)). */
			docsUrl:
				'https://github.com/absolutejs/sync#live-collections--the-sync-engine-tier-3',
			id: 'cdc-triggers',
			idempotent: true,
			kind: 'migration',
			title: 'Optional: install Postgres notify triggers so writes that bypass mutations still go live (postgresNotifyTrigger + postgresChangeSource)',
			when: 'manual'
		}
	],
	requires: {
		peers: [
			{
				name: 'elysia',
				range: '>= 1.4.26',
				reason: 'plugin host for the SSE, WebSocket, and devtools plugins'
			}
		]
	},
	settings: Type.Object({
		devtools: Type.Optional(
			Type.Object(
				{
					path: Type.Optional(
						Type.String({
							description:
								'Route the live devtools dashboard is served from. Default /sync/devtools.',
							title: 'Devtools route'
						})
					),
					snapshotMs: Type.Optional(
						Type.Number({
							description:
								'How often the dashboard refreshes its counters, in milliseconds. Default 2000.',
							minimum: 100,
							title: 'Dashboard refresh interval'
						})
					)
				},
				{ title: 'Devtools dashboard', 'x-group': 'devtools' }
			)
		),
		engine: Type.Optional(
			Type.Object(
				{
					changeLogRetainMs: Type.Optional(
						Type.Number({
							description:
								'Also drop change-log entries older than this many milliseconds — bounds memory and reconnect catch-up work on high-throughput engines.',
							minimum: 1000,
							title: 'Change history age limit'
						})
					),
					changeLogSize: Type.Optional(
						Type.Integer({
							description:
								'How many recent changes are kept so a briefly-disconnected visitor catches up with a small diff instead of a full reload. Default 1024.',
							minimum: 1,
							title: 'Change history size'
						})
					),
					instanceId: Type.Optional(
						Type.String({
							description:
								'Stable name for this server instance. Only needed when running several servers behind a cluster bus, so reconnecting visitors can resume across shards.',
							title: 'Server instance name'
						})
					),
					mutationConcurrency: Type.Optional(
						Type.Integer({
							description:
								'How many writes may run at the same time. Extra writes wait their turn — protects the server from a flood.',
							minimum: 1,
							title: 'Writes running at once'
						})
					),
					mutationQueueLimit: Type.Optional(
						Type.Integer({
							description:
								'How many writes may wait in line once the limit above is full. Beyond this, writes are rejected cleanly instead of piling up.',
							minimum: 0,
							title: 'Waiting writes limit'
						})
					)
				},
				{ title: 'Sync engine', 'x-group': 'engine' }
			)
		),
		push: Type.Optional(
			Type.Object(
				{
					heartbeatMs: Type.Optional(
						Type.Number({
							description:
								'Keep-alive ping interval so idle proxies don’t drop the update stream, in milliseconds. Default 25000.',
							minimum: 1000,
							title: 'Keep-alive interval'
						})
					),
					path: Type.Optional(
						Type.String({
							description:
								'Route browsers connect to for instant updates. Default /sync.',
							title: 'Update stream route'
						})
					)
				},
				{ title: 'Reactive push (SSE)', 'x-group': 'push' }
			)
		),
		socket: Type.Optional(
			Type.Object(
				{
					closeOnSlow: Type.Optional(
						Type.Boolean({
							description:
								'Disconnect clients that can’t keep up with the update stream (they reconnect and reload fresh). Default off.',
							title: 'Drop slow connections'
						})
					),
					maxBufferedBytes: Type.Optional(
						Type.Number({
							description:
								'How many bytes may queue for one connection before it counts as slow.',
							minimum: 1,
							title: 'Slow-connection threshold'
						})
					),
					path: Type.Optional(
						Type.String({
							description:
								'WebSocket route live collections connect to. Default /sync/ws.',
							title: 'Live-collection route'
						})
					)
				},
				{ title: 'Live-collection socket', 'x-group': 'socket' }
			)
		)
	}),
	slots: {
		clusterBus: {
			/* $self: the bus is consumed by engine.connectCluster(bus), not a
			 * config field (v1 limitation: no cross-recipe instance refs). */
			configPath: '$self',
			contract: 'sync/cluster-bus',
			description:
				'How changes fan out across your server instances when you scale horizontally',
			known: [
				'@absolutejs/sync#memory-bus',
				'@absolutejs/sync-bus-pg',
				'@absolutejs/sync-bus-redis'
			]
		},
		crdt: {
			/* $self: the mergeable is passed per-field to engine.registerCrdt,
			 * not a config field. */
			configPath: '$self',
			contract: 'sync/crdt-adapter',
			description:
				'The engine that merges simultaneous edits in collaborative text',
			known: [
				'@absolutejs/sync#rga-text',
				'@absolutejs/sync-yjs',
				'@absolutejs/sync-automerge',
				'@absolutejs/sync-loro'
			]
		}
	},
	tools: {
		engine_metrics: tool.runtime({
			annotations: { readOnlyHint: true },
			authorization: {
				approval: 'never',
				audience: 'admin',
				effects: ['read'],
				requiredScopes: ['sync:read']
			},
			description:
				'Operator-shaped engine health: uptime, subscription/mutation/change counters, memory estimates, and throughput since start.',
			handler: (_input, runtime) =>
				runtime.engine
					? JSON.stringify(runtime.engine.metrics())
					: 'no sync engine is bound (tier 1/2 apps only have the hub)',
			input: Type.Object({})
		}),
		engine_overview: tool.runtime({
			annotations: { readOnlyHint: true },
			authorization: {
				approval: 'never',
				audience: 'admin',
				effects: ['read'],
				requiredScopes: ['sync:read']
			},
			description:
				'What this sync engine serves: registered collections with live subscription counts, mutations, schedules, readers/writers, installed packs, and the most recent changes.',
			handler: (_input, runtime) => {
				if (!runtime.engine) {
					return 'no sync engine is bound (tier 1/2 apps only have the hub)';
				}
				const inspection = runtime.engine.inspect();

				return JSON.stringify({
					...inspection,
					recentChanges:
						inspection.recentChanges.slice(-RECENT_CHANGES_LIMIT)
				});
			},
			input: Type.Object({})
		}),
		publish_topic: tool.runtime({
			annotations: { idempotentHint: true },
			authorization: {
				approval: 'policy',
				audience: 'admin',
				effects: ['write'],
				idempotency: { mode: 'host' },
				requiredScopes: ['sync:publish'],
				resource: { idField: 'topic', type: 'sync-topic' },
				reversible: false
			},
			description:
				'Publish a reactive topic on the hub so every subscribed view refetches now. Useful after out-of-band data changes. Reports how many subscribers were listening.',
			handler: ({ topic }, runtime) => {
				if (!runtime.hub) {
					return 'no reactive hub is bound (engine-tier apps push through mutations instead)';
				}
				runtime.hub.publish(topic);

				return `published "${topic}" (${runtime.hub.subscriberCount()} active subscribers on the hub)`;
			},
			input: Type.Object({ topic: Type.String({ minLength: 1 }) })
		}),
		replay_at: tool.runtime({
			annotations: { readOnlyHint: true },
			authorization: {
				approval: 'never',
				audience: 'admin',
				effects: ['read'],
				requiredScopes: ['sync:replay']
			},
			description:
				'Point-in-time replay: reconstruct table state as of a past timestamp from the change log. Returns per-table row counts plus asOfVersion/asOfAt; truncated=true means the log doesn’t reach back that far (widen changeLogRetainMs for forensics).',
			handler: async ({ atMs, tables }, runtime) => {
				if (!runtime.engine) return 'no sync engine is bound';
				const result = await runtime.engine.replayTo({
					at: atMs,
					tables
				});

				return JSON.stringify({
					asOfAt: result.asOfAt,
					asOfVersion: result.asOfVersion,
					rowCounts: Object.fromEntries(
						Object.entries(result.rows).map(([table, rows]) => [
							table,
							rows.length
						])
					),
					truncated: result.truncated
				});
			},
			input: Type.Object({
				atMs: Type.Integer({
					description: 'Target timestamp, Unix epoch milliseconds.',
					minimum: 0
				}),
				tables: Type.Optional(Type.Array(Type.String({ minLength: 1 })))
			})
		}),
		run_schedule: tool.runtime({
			annotations: { idempotentHint: true, openWorldHint: true },
			authorization: {
				approval: 'always',
				audience: 'admin',
				destinations: ['configured-schedule-handler'],
				effects: ['write', 'external-network', 'arbitrary-code'],
				idempotency: { mode: 'host' },
				requiredScopes: ['sync:schedules:run'],
				resource: { idField: 'name', type: 'sync-schedule' },
				reversible: false
			},
			description:
				'Fire one registered scheduled function now instead of waiting for its cron pattern. Whatever it writes goes live through the change feed.',
			handler: async ({ name }, runtime) => {
				if (!runtime.engine) return 'no sync engine is bound';
				const known = runtime.engine
					.listSchedules()
					.map((schedule) => schedule.name);
				if (!known.includes(name)) {
					return known.length === 0
						? 'this engine has no registered schedules'
						: `no schedule named "${name}" — registered: ${known.join(', ')}`;
				}
				await runtime.engine.runSchedule(name);

				return `ran schedule "${name}"`;
			},
			input: Type.Object({ name: Type.String({ minLength: 1 }) })
		})
	},
	wiring: [
		{
			description:
				'Push-on-change over SSE: a mutation publishes the topics it touched and every subscribed view refetches instantly — no polling loops.',
			id: 'default',
			server: {
				code: [
					'const hub = createReactiveHub();',
					'',
					'// Mount with .use(syncPush). After a durable write, publish the topics',
					'// it changed and subscribed browsers refetch instantly:',
					"//   hub.publish('orders'); hub.publish('orders:' + order.id);",
					'// Browsers subscribe with createSyncSubscriber (or createLiveQuery)',
					"// from '@absolutejs/sync/client'.",
					'const syncPush = sync({ heartbeatMs: ${settings.push.heartbeatMs}, hub, path: ${settings.push.path} });'
				].join('\n'),
				imports: [
					{
						from: '@absolutejs/sync',
						names: ['createReactiveHub', 'sync']
					}
				],
				placement: 'module-scope'
			},
			title: 'Instant updates over SSE (kill polling)'
		},
		{
			client: {
				client: {
					code: [
						'const orders = createLiveQuery({',
						"\tfetcher: jsonFetcher('/api/orders'),",
						"\ttopics: ['orders']",
						'});',
						'',
						'orders.subscribe((state) => {',
						'\t// TODO: render state.data — refetched the instant an order changes.',
						'});'
					].join('\n'),
					imports: [
						{
							from: '@absolutejs/sync/client',
							names: ['createLiveQuery', 'jsonFetcher']
						}
					],
					placement: 'client-entry'
				}
			},
			description:
				'The ORM adapters derive topics from queries so reads and writes line up automatically — a read of a table subscribes to it, a write publishes it.',
			id: 'live-query',
			server: {
				code: [
					'const hub = createReactiveHub();',
					'',
					'// Mount with .use(syncPush). Reads map to topics automatically',
					"// (a list read of `orders` -> topic 'orders'; a primary-key lookup",
					"// -> 'orders:<id>'). After a write, publish from the same filter:",
					"//   publishWhere(hub, orders, eq(orders.id, id), { op: 'update' });",
					"// (same function names on '@absolutejs/sync/prisma' for Prisma.)",
					'const syncPush = sync({ heartbeatMs: ${settings.push.heartbeatMs}, hub, path: ${settings.push.path} });'
				].join('\n'),
				imports: [
					{
						from: '@absolutejs/sync',
						names: ['createReactiveHub', 'sync']
					},
					{
						from: '@absolutejs/sync/drizzle',
						names: ['deriveReadTopics', 'publishWhere']
					}
				],
				placement: 'module-scope'
			},
			title: 'Auto-reactive queries from your ORM (Drizzle / Prisma)'
		},
		{
			client: {
				react: {
					code: [
						'const { data, mutate, status } = useSyncCollection({',
						"\tcollection: 'orders',",
						"\t// TODO: your server's WebSocket URL (syncSocket path).",
						"\turl: 'ws://localhost:3000/sync/ws'",
						'});'
					].join('\n'),
					imports: [
						{
							from: '@absolutejs/sync/react',
							names: ['useSyncCollection']
						}
					],
					placement: 'client-entry'
				}
			},
			description:
				'Row-level live collections: the client hydrates once, then the server pushes {added, removed, changed} diffs over WebSocket, with optimistic mutations and an offline queue. Declares the module-scope `engine` binding other sync packages wire against.',
			id: 'engine',
			server: {
				code: [
					'// The sync engine. The module-scope `engine` binding is the documented',
					'// anchor @absolutejs/sync-pack-* wiring snippets reference by name.',
					'const engine = createSyncEngine({',
					'\tchangeLogRetainMs: ${settings.engine.changeLogRetainMs},',
					'\tchangeLogSize: ${settings.engine.changeLogSize},',
					'\tinstanceId: ${settings.engine.instanceId},',
					'\tmutationConcurrency: ${settings.engine.mutationConcurrency},',
					'\tmutationQueueLimit: ${settings.engine.mutationQueueLimit}',
					'});',
					'',
					'// TODO: register your first live table — a collection (what clients',
					'// subscribe to), a writer (how rows persist), and a mutation.',
					'// See https://github.com/absolutejs/sync#live-collections--the-sync-engine-tier-3',
					'',
					'// Mount with .use(liveSync) (and .use(liveDevtools) in development).',
					'const liveSync = syncSocket({ closeOnSlow: ${settings.socket.closeOnSlow}, engine, maxBufferedBytes: ${settings.socket.maxBufferedBytes}, path: ${settings.socket.path} });',
					'const liveDevtools = syncDevtools({ engine, path: ${settings.devtools.path}, snapshotMs: ${settings.devtools.snapshotMs} });'
				].join('\n'),
				imports: [
					{
						from: '@absolutejs/sync',
						names: ['syncDevtools', 'syncSocket']
					},
					{
						from: '@absolutejs/sync/engine',
						names: ['createSyncEngine']
					}
				],
				placement: 'module-scope'
			},
			title: 'Live collections (the sync engine)'
		},
		{
			description:
				'Conflict-free collaborative editing: the engine merges the declared CRDT fields on write instead of overwriting, and auto-registers the merge mutation the useCollaborativeText hooks call. Rides the engine recipe.',
			id: 'collaborative-text',
			server: {
				code: [
					"// Rides the 'engine' recipe: `engine` is its module-scope binding.",
					'const textCrdt = ${slot.crdt};',
					'',
					"// TODO: replace 'docs'/'body' with your table and CRDT field.",
					"engine.registerCrdt('docs', { body: textCrdt });"
				].join('\n'),
				imports: [],
				placement: 'module-scope'
			},
			title: 'Collaborative text (CRDT merge on write)'
		},
		{
			description:
				'Horizontal scale: committed changes fan out across server instances over a cluster bus, so a mutation on one shard reaches subscribers on every shard. Rides the engine recipe.',
			id: 'cluster',
			server: {
				code: [
					"// Rides the 'engine' recipe: `engine` is its module-scope binding.",
					'await engine.connectCluster(${slot.clusterBus});'
				].join('\n'),
				imports: [],
				placement: 'module-scope'
			},
			title: 'Fan out across server instances (cluster bus)'
		}
	]
});
