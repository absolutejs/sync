import { describe, expect, test } from 'bun:test';
import { IDBFactory } from 'fake-indexeddb';
import {
	createIndexedDbSyncLocalStore,
	createMemorySyncLocalStore,
	createSyncOperationId,
	ensureSyncInstallationId
} from '../src/client/localStore';
import type {
	LocalMutationRecord,
	SyncLocalStore
} from '../src/client/localStore';

const operation = (operationId: string): LocalMutationRecord => ({
	operationId,
	name: 'tasks:create',
	args: { title: operationId },
	optimistic: [],
	inverse: [],
	createdAt: 1,
	attempts: 0
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
