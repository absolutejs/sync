import { describe, expect, test } from 'bun:test';
import { IDBFactory } from 'fake-indexeddb';
import {
	createIndexedDbSyncLocalStore,
	createMemorySyncLocalStore,
	createSyncOperationId,
	ensureSyncInstallationId,
	resolveSyncLocalSchemaComponents,
	resolveSyncLocalMigrations,
	SyncLocalStoreSchemaError
} from '../src/client/localStore';
import type {
	LocalMutationRecord,
	SyncLocalStore
} from '../src/client/localStore';
import { assertSyncLocalStoreConformance } from '../src/testing';

const operation = (operationId: string): LocalMutationRecord => ({
	operationId,
	name: 'tasks:create',
	args: { title: operationId },
	optimistic: [],
	inverse: [],
	createdAt: 1,
	attempts: 0
});

test('validates contiguous schema plans and compatibility bounds', () => {
	expect(
		resolveSyncLocalMigrations(1, {
			version: 3,
			migrations: [{ toVersion: 2 }, { toVersion: 3 }]
		}).steps.map((step) => step.toVersion)
	).toEqual([2, 3]);
	expect(() =>
		resolveSyncLocalMigrations(1, {
			minimumCompatibleVersion: 1,
			version: 3,
			migrations: [{ toVersion: 3 }]
		})
	).toThrow('1 -> 2 is missing');
	expect(() =>
		resolveSyncLocalMigrations(1, {
			minimumCompatibleVersion: 2,
			version: 3,
			migrations: [{ toVersion: 2 }, { toVersion: 3 }]
		})
	).toThrow('older than the minimum compatible');
});

test('IndexedDB upgrades legacy rows atomically across principal partitions', async () => {
	const indexedDB = new IDBFactory();
	const databaseName = `absolutejs-sync-migrate-${crypto.randomUUID()}`;
	const legacy = createIndexedDbSyncLocalStore({ databaseName, indexedDB });
	for (const namespace of ['account-a', 'account-b'])
		await legacy.transaction(namespace, 'readwrite', async (tx) => {
			await tx.putCollection('tasks', {
				rows: [{ id: 1, title: namespace }],
				version: 1
			});
			await tx.putMutation(operation(`${namespace}:op-1`));
		});

	const upgraded = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: {
			version: 3,
			migrations: [
				{
					toVersion: 2,
					migrateCollection: (record, context) => ({
						...record,
						rows: record.rows.map((row) => ({
							...(row as object),
							migratedFor: context.namespace
						}))
					})
				},
				{
					toVersion: 3,
					migrateMutation: (record) => ({
						...record,
						lastError: 'retained through upgrade'
					})
				}
			]
		}
	});
	await expect(upgraded.getSchemaStatus?.()).resolves.toEqual({
		minimumCompatibleVersion: 1,
		state: 'ready',
		storedVersion: 3,
		targetVersion: 3
	});
	for (const namespace of ['account-a', 'account-b']) {
		const state = await upgraded.transaction(
			namespace,
			'readonly',
			async (tx) => ({
				collection: await tx.getCollection('tasks'),
				mutations: await tx.listMutations()
			})
		);
		expect(state.collection?.rows).toEqual([
			{ id: 1, migratedFor: namespace, title: namespace }
		]);
		expect(state.mutations[0]?.lastError).toBe('retained through upgrade');
	}

	const olderRuntime = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: { version: 2, migrations: [{ toVersion: 2 }] }
	});
	const error = await olderRuntime
		.getSchemaStatus?.()
		.catch((cause) => cause);
	expect(error).toBeInstanceOf(SyncLocalStoreSchemaError);
	expect((error as SyncLocalStoreSchemaError).code).toBe('SCHEMA_TOO_NEW');
});

test('IndexedDB rolls back every row and its version when migration throws', async () => {
	const indexedDB = new IDBFactory();
	const databaseName = `absolutejs-sync-migrate-rollback-${crypto.randomUUID()}`;
	const legacy = createIndexedDbSyncLocalStore({ databaseName, indexedDB });
	await legacy.transaction('account-a', 'readwrite', async (tx) => {
		await tx.putCollection('first', { rows: [{ id: 1 }], version: 1 });
		await tx.putCollection('second', { rows: [{ id: 2 }], version: 1 });
	});
	const failed = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: {
			version: 2,
			migrations: [
				{
					toVersion: 2,
					migrateCollection: (record, context) => {
						if (context.key === 'second')
							throw new Error('simulated crash');
						return { ...record, cursor: 'partially-migrated' };
					}
				}
			]
		}
	});
	await expect(failed.getSchemaStatus?.()).rejects.toThrow('simulated crash');

	const recovered = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: {
			version: 2,
			migrations: [{ toVersion: 2 }]
		}
	});
	await expect(recovered.getSchemaStatus?.()).resolves.toMatchObject({
		storedVersion: 2
	});
	const rows = await recovered.transaction(
		'account-a',
		'readonly',
		async (tx) => [
			await tx.getCollection('first'),
			await tx.getCollection('second')
		]
	);
	expect(rows.map((record) => record?.cursor)).toEqual([
		undefined,
		undefined
	]);
});

test('IndexedDB composes JSON pack migrations with independent version ledgers', async () => {
	const indexedDB = new IDBFactory();
	const databaseName = `absolutejs-sync-components-${crypto.randomUUID()}`;
	const legacy = createIndexedDbSyncLocalStore({ databaseName, indexedDB });
	await legacy.transaction('account-a', 'readwrite', async (tx) => {
		await tx.putCollection('tasks:open', {
			collection: 'tasks',
			rows: [{ id: 1, title: 'ship it' }],
			version: 1
		});
		await tx.putCollection('alerts', {
			collection: 'notifications',
			rows: [{ id: 2, title: 'keep me' }],
			version: 1
		});
	});

	const metadata = JSON.parse(
		JSON.stringify({
			components: [
				{ id: '@absolutejs/app', version: 1 },
				{
					id: '@example/sync-pack-labels',
					migrations: [
						{
							operations: [
								{
									collection: 'tasks',
									from: 'title',
									to: 'label',
									type: 'rename-field'
								}
							],
							toVersion: 2
						}
					],
					version: 2
				},
				{
					id: '@example/sync-pack-tasks',
					migrations: [
						{
							operations: [
								{
									collection: 'tasks',
									field: 'archived',
									type: 'set-default',
									value: false
								}
							],
							toVersion: 2
						}
					],
					version: 2
				}
			]
		})
	);
	const upgraded = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: metadata
	});
	await expect(upgraded.getSchemaStatus?.()).resolves.toEqual({
		components: [
			{
				id: '@absolutejs/app',
				minimumCompatibleVersion: 1,
				storedVersion: 1,
				targetVersion: 1
			},
			{
				id: '@example/sync-pack-labels',
				minimumCompatibleVersion: 1,
				storedVersion: 2,
				targetVersion: 2
			},
			{
				id: '@example/sync-pack-tasks',
				minimumCompatibleVersion: 1,
				storedVersion: 2,
				targetVersion: 2
			}
		],
		minimumCompatibleVersion: 1,
		state: 'ready',
		storedVersion: 1,
		targetVersion: 1
	});
	const migrated = await upgraded.transaction('account-a', 'readonly', (tx) =>
		tx.getCollection('tasks:open')
	);
	expect(migrated?.rows).toEqual([
		{ archived: false, id: 1, label: 'ship it' }
	]);

	const withoutLabels = createIndexedDbSyncLocalStore({
		databaseName,
		indexedDB,
		storageSchema: {
			components: [metadata.components[0], metadata.components[2]]
		}
	});
	await expect(withoutLabels.getSchemaStatus?.()).resolves.toMatchObject({
		orphanedComponents: ['@example/sync-pack-labels']
	});
});

test('rejects duplicate component metadata before opening storage', () => {
	expect(() =>
		createMemorySyncLocalStore({
			storageSchema: {
				components: [
					{ id: '@example/tasks', version: 1 },
					{ id: '@example/tasks', version: 1 }
				]
			}
		})
	).toThrow('declared more than once');
});

test('installs a new component from its supported compatibility baseline', () => {
	const resolved = resolveSyncLocalSchemaComponents(
		{ '@absolutejs/app': 1 },
		{
			components: [
				{ id: '@absolutejs/app', version: 1 },
				{
					id: '@absolutejs/new-pack',
					minimumCompatibleVersion: 3,
					migrations: [{ toVersion: 4 }, { toVersion: 5 }],
					version: 5
				}
			]
		}
	);
	expect(resolved.components[1]).toMatchObject({
		id: '@absolutejs/new-pack',
		steps: [{ toVersion: 4 }, { toVersion: 5 }],
		targetVersion: 5
	});
});

type StoreFactory = () => SyncLocalStore;

const adapters: Array<[string, StoreFactory]> = [
	['memory', createMemorySyncLocalStore],
	[
		'IndexedDB',
		() =>
			createIndexedDbSyncLocalStore({
				databaseName: `absolutejs-sync-test-${crypto.randomUUID()}`,
				indexedDB: new IDBFactory()
			})
	]
];

for (const [adapter, createStore] of adapters) {
	describe(`${adapter} SyncLocalStore conformance`, () => {
		test('passes the portable adapter contract', async () => {
			await expect(
				assertSyncLocalStoreConformance({ store: createStore() })
			).resolves.toBeUndefined();
		});
		test('commits collection state and its outbound operation atomically', async () => {
			const store = createStore();
			await store.transaction('account-a', 'readwrite', async (tx) => {
				await tx.putCollection('tasks', {
					rows: [{ id: 1, title: 'offline' }],
					version: 7,
					cursor: 'cursor-7'
				});
				await tx.putMutation(operation('install-a:op-1'));
			});

			const state = await store.transaction(
				'account-a',
				'readonly',
				async (tx) => ({
					collection: await tx.getCollection('tasks'),
					mutations: await tx.listMutations()
				})
			);
			expect(state.collection).toEqual({
				rows: [{ id: 1, title: 'offline' }],
				version: 7,
				cursor: 'cursor-7'
			});
			expect(state.mutations.map((item) => item.operationId)).toEqual([
				'install-a:op-1'
			]);
		});

		test('rolls back every write when a transaction callback throws', async () => {
			const store = createStore();
			await expect(
				store.transaction('account-a', 'readwrite', async (tx) => {
					await tx.putCollection('tasks', {
						rows: [{ id: 1 }],
						version: 1
					});
					await tx.putMutation(operation('install-a:op-1'));
					throw new Error('process died');
				})
			).rejects.toThrow('process died');

			const state = await store.transaction(
				'account-a',
				'readonly',
				async (tx) => ({
					collection: await tx.getCollection('tasks'),
					mutations: await tx.listMutations()
				})
			);
			expect(state.collection).toBeUndefined();
			expect(state.mutations).toEqual([]);
		});

		test('isolates account namespaces and deletes only the signed-out account', async () => {
			const store = createStore();
			for (const namespace of ['account-a', 'account-b']) {
				await store.transaction(namespace, 'readwrite', async (tx) => {
					await tx.putMutation(operation(`${namespace}:op-1`));
				});
			}
			await store.deleteNamespace('account-a');

			const a = await store.transaction('account-a', 'readonly', (tx) =>
				tx.listMutations()
			);
			const b = await store.transaction('account-b', 'readonly', (tx) =>
				tx.listMutations()
			);
			expect(a).toEqual([]);
			expect(b.map((item) => item.operationId)).toEqual([
				'account-b:op-1'
			]);
		});

		test('rejects writes through a readonly transaction', async () => {
			const store = createStore();
			await expect(
				store.transaction('account-a', 'readonly', (tx) =>
					tx.putMutation(operation('op-1'))
				)
			).rejects.toThrow('readonly');
		});

		test('creates one stable installation id and prefixes operation ids', async () => {
			const store = createStore();
			let calls = 0;
			const createId = () => `generated-${++calls}`;
			const first = await ensureSyncInstallationId(
				store,
				'account-a',
				createId
			);
			const second = await ensureSyncInstallationId(
				store,
				'account-a',
				createId
			);

			expect(first).toBe('generated-1');
			expect(second).toBe(first);
			expect(calls).toBe(1);
			expect(createSyncOperationId(first, () => 'mutation-1')).toBe(
				'generated-1:mutation-1'
			);
		});
	});
}
