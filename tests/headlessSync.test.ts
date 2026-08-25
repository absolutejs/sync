import { describe, expect, test } from 'bun:test';
import { runHeadlessSync } from '../src/client/headlessSync';
import { createMemorySyncLocalStore } from '../src/client/localStore';
import { defineCollection } from '../src/engine/collection';
import {
	defineMutation,
	type DurableMutationOperation
} from '../src/engine/mutation';
import { headlessSyncRoute } from '../src/engine/routes';
import { createSyncEngine } from '../src/engine/syncEngine';
import { SyncMutationRejectionError } from '../src/reconciliation';

type Task = { id: number; title: string };

const setupRoute = () => {
	const rows: Task[] = [{ id: 1, title: 'one' }];
	const receipts = new Map<string, { name: string; result: unknown }>();
	const engine = createSyncEngine({
		durableMutations: {
			scope: (ctx) => (ctx as { account: string }).account,
			run: async <R>(
				operation: DurableMutationOperation,
				execute: (tx: unknown) => Promise<R>
			) => {
				const key = `${operation.scope}:${operation.operationId}`;
				const existing = receipts.get(key);
				if (existing)
					return { replayed: true, result: existing.result as R };
				const result = await execute(undefined);
				receipts.set(key, { name: operation.name, result });
				return { replayed: false, result };
			}
		}
	});
	engine.register(
		defineCollection<Task>({
			name: 'tasks',
			hydrate: () => rows,
			match: () => true
		})
	);
	engine.registerMutation(
		defineMutation<Task, { account: string }, Task>({
			name: 'addTask',
			handler: async (task, _ctx, actions) => {
				rows.push(task);
				await actions.change('tasks', { op: 'insert', row: task });
				return task;
			}
		})
	);
	engine.registerMutation(
		defineMutation({
			name: 'conflict',
			handler: () => {
				throw new SyncMutationRejectionError('conflict', 'changed', {
					code: 'STALE'
				});
			}
		})
	);
	return {
		engine,
		route: headlessSyncRoute(engine, {
			resolveContext: () => ({ account: 'a' })
		})
	};
};

describe('headless HTTP sync', () => {
	test('pushes durable mutations before returning a finite pull', async () => {
		const { route } = setupRoute();
		const response = await route({
			body: {
				version: 1,
				mutations: [
					{
						operationId: 'install:1',
						name: 'addTask',
						args: { id: 2, title: 'two' }
					}
				],
				pulls: [{ id: 'tasks', collection: 'tasks' }]
			},
			query: {}
		});
		expect(response.mutations).toEqual([
			{
				operationId: 'install:1',
				status: 'ack',
				result: { id: 2, title: 'two' }
			}
		]);
		expect(response.pulls[0]).toMatchObject({
			type: 'snapshot',
			rows: [
				{ id: 1, title: 'one' },
				{ id: 2, title: 'two' }
			]
		});
	});

	test('returns typed mutation rejection metadata', async () => {
		const { route } = setupRoute();
		const response = await route({
			body: {
				version: 1,
				mutations: [{ operationId: 'install:2', name: 'conflict' }]
			},
			query: {}
		});
		expect(response.mutations[0]).toEqual({
			operationId: 'install:2',
			status: 'reject',
			rejection: { kind: 'conflict', message: 'changed', code: 'STALE' }
		});
	});

	test('storage runner atomically acknowledges and applies a pull', async () => {
		const store = createMemorySyncLocalStore();
		await store.transaction('principal', 'readwrite', async (tx) => {
			await tx.putMutation({
				operationId: 'install:3',
				name: 'addTask',
				args: { id: 2, title: 'two' },
				optimistic: [],
				inverse: [],
				createdAt: 1,
				attempts: 0
			});
			await tx.putCollection('tasks-key', {
				rows: [{ id: 1, title: 'one' }],
				version: 1,
				cursor: 'old'
			});
		});
		const result = await runHeadlessSync({
			endpoint: 'https://api.example/sync/background',
			store,
			namespace: 'principal',
			collections: [
				{ collection: 'tasks', localKey: 'tasks-key' },
				{ collection: 'deferred', localKey: 'deferred-key' }
			],
			maxPulls: 1,
			fetch: async (_url, init) => {
				const request = JSON.parse(init.body);
				expect(request.pulls).toHaveLength(1);
				expect(request.pulls[0].since).toBe('old');
				return {
					ok: true,
					status: 200,
					json: async () => ({
						version: 1,
						mutations: [
							{ operationId: 'install:3', status: 'ack' }
						],
						pulls: [
							{
								id: '0',
								type: 'diff',
								added: [{ id: 2, title: 'two' }],
								removed: [],
								changed: [],
								version: 2,
								cursor: 'new'
							}
						]
					})
				};
			}
		});
		expect(result).toEqual({
			acknowledged: 1,
			deadLettered: 0,
			pulled: 1,
			retryScheduled: 0
		});
		await store.transaction('principal', 'readonly', async (tx) => {
			expect(await tx.listMutations()).toEqual([]);
			expect(await tx.getCollection<Task>('tasks-key')).toEqual({
				rows: [
					{ id: 1, title: 'one' },
					{ id: 2, title: 'two' }
				],
				version: 2,
				cursor: 'new',
				collection: 'tasks',
				headlessKey: 'id'
			});
		});
	});

	test('discovers persisted id-keyed collections and refuses redirects', async () => {
		const store = createMemorySyncLocalStore();
		await store.transaction('principal', 'readwrite', async (tx) => {
			await tx.putCollection('tasks-key', {
				collection: 'tasks',
				headlessKey: 'id',
				params: { owner: 'me' },
				rows: [],
				version: 0
			});
			await tx.putCollection('custom-keyed', {
				collection: 'custom',
				rows: [],
				version: 0
			});
		});
		const result = await runHeadlessSync({
			endpoint: '/__absolute/sync/background',
			fetch: async (_url, init) => {
				expect(init.redirect).toBe('error');
				const request = JSON.parse(init.body);
				expect(request.pulls).toEqual([
					{
						collection: 'tasks',
						id: '0',
						params: { owner: 'me' }
					}
				]);
				return {
					json: async () => ({
						mutations: [],
						pulls: [
							{
								cursor: 'cursor-1',
								id: '0',
								rows: [{ id: 1, title: 'one' }],
								type: 'snapshot',
								version: 1
							}
						],
						version: 1
					}),
					ok: true,
					status: 200
				};
			},
			namespace: 'principal',
			store
		});
		expect(result.pulled).toBe(1);
		expect(
			await store.transaction('principal', 'readonly', (tx) =>
				tx.getCollection('tasks-key')
			)
		).toMatchObject({
			cursor: 'cursor-1',
			rows: [{ id: 1, title: 'one' }]
		});
	});

	test('dead-letters permanent responses and bounds retryable responses', async () => {
		const store = createMemorySyncLocalStore();
		await store.transaction('principal', 'readwrite', (tx) =>
			tx.putMutation({
				operationId: 'install:4',
				name: 'conflict',
				args: undefined,
				optimistic: [],
				inverse: [],
				createdAt: 1,
				attempts: 0
			})
		);
		const result = await runHeadlessSync({
			endpoint: 'https://api.example/sync/background',
			store,
			namespace: 'principal',
			fetch: async () => ({
				ok: true,
				status: 200,
				json: async () => ({
					version: 1,
					mutations: [
						{
							operationId: 'install:4',
							status: 'reject',
							rejection: { kind: 'permanent', message: 'invalid' }
						}
					],
					pulls: []
				})
			})
		});
		expect(result.deadLettered).toBe(1);
		const stored = await store.transaction('principal', 'readonly', (tx) =>
			tx.getMutation('install:4')
		);
		expect(stored).toMatchObject({
			state: 'dead-letter',
			attempts: 1,
			lastError: 'invalid'
		});
	});
});
