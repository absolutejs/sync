import { describe, expect, test } from 'bun:test';
import { createCollection } from '@tanstack/db';
import {
	createSyncTanStackCollectionOptions,
	type SyncTanStackCollectionOptions
} from '../src/adapters/tanstack-db';
import type {
	MutateOptions,
	SyncCollection,
	SyncCollectionState
} from '../src/client/syncCollection';

type Todo = {
	id: string;
	title: string;
	done?: boolean;
};

const createFakeSyncCollection = <T>(
	initial: SyncCollectionState<T>
): SyncCollection<T> & {
	emit: (state: SyncCollectionState<T>) => void;
	mutations: Array<{ name: string; args: unknown; optimistic: unknown }>;
	closed: boolean;
} => {
	let state = initial;
	let closed = false;
	const listeners = new Set<(state: SyncCollectionState<T>) => void>();
	const mutations: Array<{
		name: string;
		args: unknown;
		optimistic: unknown;
	}> = [];

	const collection = {
		get: () => state,
		subscribe: (listener: (state: SyncCollectionState<T>) => void) => {
			listeners.add(listener);
			return () => listeners.delete(listener);
		},
		mutate: async ({ name, args, optimistic }: MutateOptions<T>) => {
			mutations.push({ name, args, optimistic });
		},
		disconnect: () => {},
		close: () => {
			closed = true;
		},
		emit: (next: SyncCollectionState<T>) => {
			state = next;
			for (const listener of listeners) {
				listener(state);
			}
		},
		mutations,
		get closed() {
			return closed;
		}
	};

	return collection as SyncCollection<T> & {
		emit: (state: SyncCollectionState<T>) => void;
		mutations: Array<{ name: string; args: unknown; optimistic: unknown }>;
		closed: boolean;
	};
};

const createOptions = (
	syncCollection: SyncTanStackCollectionOptions<Todo>['syncCollection']
) =>
	createSyncTanStackCollectionOptions<Todo>({
		id: 'todos',
		url: 'ws://sync.test/ws',
		collection: 'todos',
		getKey: (todo) => todo.id,
		syncCollection
	});

describe('createSyncTanStackCollectionOptions', () => {
	test('forwards Absolute Sync snapshots and diffs to TanStack DB', () => {
		const first = { id: 'a', title: 'Write adapter' };
		const second = { id: 'b', title: 'Add tests' };
		const fake = createFakeSyncCollection<Todo>({
			data: [first, second],
			status: 'ready',
			error: undefined
		});
		const config = createOptions(fake);
		const writes: unknown[] = [];
		let begins = 0;
		let commits = 0;
		let ready = 0;

		const cleanup = config.sync.sync({
			collection: {} as never,
			begin: () => {
				begins += 1;
			},
			write: (message) => {
				writes.push(message);
			},
			commit: () => {
				commits += 1;
			},
			markReady: () => {
				ready += 1;
			},
			truncate: () => {}
		});

		expect(writes).toEqual([
			{ type: 'insert', value: first },
			{ type: 'insert', value: second }
		]);
		expect(begins).toBe(1);
		expect(commits).toBe(1);
		expect(ready).toBe(1);

		writes.length = 0;
		const updated = { id: 'a', title: 'Write adapter', done: true };
		const third = { id: 'c', title: 'Ship beta' };
		fake.emit({
			data: [updated, third],
			status: 'ready',
			error: undefined
		});

		expect(writes).toEqual([
			{ type: 'update', value: updated, previousValue: first },
			{ type: 'insert', value: third },
			{ type: 'delete', key: 'b' }
		]);
		expect(ready).toBe(1);

		if (typeof cleanup === 'function') {
			cleanup();
		}
		expect(fake.closed).toBe(true);
	});

	test('works with TanStack DB createCollection', async () => {
		const first = { id: 'a', title: 'Write adapter' };
		const fake = createFakeSyncCollection<Todo>({
			data: [first],
			status: 'ready',
			error: undefined
		});

		const collection = createCollection(createOptions(fake));
		await collection.preload();

		expect(collection.size).toBe(1);
		expect(collection.get('a')?.title).toBe('Write adapter');
	});

	test('maps TanStack DB mutations to Absolute Sync mutations', async () => {
		const fake = createFakeSyncCollection<Todo>({
			data: [],
			status: 'ready',
			error: undefined
		});
		const config = createSyncTanStackCollectionOptions<Todo>({
			url: 'ws://sync.test/ws',
			collection: 'todos',
			getKey: (todo) => todo.id,
			syncCollection: fake,
			mutations: {
				insert: 'todos.create',
				update: 'todos.update',
				delete: 'todos.delete'
			}
		});

		await config.onInsert?.({
			collection: {} as never,
			transaction: {
				mutations: [
					{
						type: 'insert',
						key: 'a',
						modified: { id: 'a', title: 'New' },
						metadata: { source: 'test' }
					}
				]
			} as never
		});
		await config.onUpdate?.({
			collection: {} as never,
			transaction: {
				mutations: [
					{
						type: 'update',
						key: 'a',
						modified: { id: 'a', title: 'Newer' },
						changes: { title: 'Newer' },
						metadata: undefined
					}
				]
			} as never
		});
		await config.onDelete?.({
			collection: {} as never,
			transaction: {
				mutations: [
					{
						type: 'delete',
						key: 'a',
						original: { id: 'a', title: 'Newer' },
						metadata: undefined
					}
				]
			} as never
		});

		expect(fake.mutations).toEqual([
			{
				name: 'todos.create',
				args: {
					row: { id: 'a', title: 'New' },
					metadata: { source: 'test' }
				},
				optimistic: undefined
			},
			{
				name: 'todos.update',
				args: {
					key: 'a',
					row: { id: 'a', title: 'Newer' },
					changes: { title: 'Newer' },
					metadata: undefined
				},
				optimistic: undefined
			},
			{
				name: 'todos.delete',
				args: {
					key: 'a',
					row: { id: 'a', title: 'Newer' },
					metadata: undefined
				},
				optimistic: undefined
			}
		]);
	});
});
