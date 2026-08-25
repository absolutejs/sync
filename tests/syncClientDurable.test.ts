import { afterEach, describe, expect, test } from 'bun:test';
import { IDBFactory } from 'fake-indexeddb';
import {
	createIndexedDbSyncLocalStore,
	createMemorySyncLocalStore,
	createSyncClient,
	SyncMutationRejectedError
} from '../src/client';
import type { LocalMutationRecord, SyncLocalStore } from '../src/client';
import { installSyncClientRuntimeTransport } from '../src/client/runtimeTransport';
import type { ServerFrame } from '../src/engine/connection';

type Order = { id: number; status: string };

class FakeWebSocket {
	static instances: FakeWebSocket[] = [];
	onopen: (() => void) | undefined;
	onmessage: ((event: { data: string }) => void) | undefined;
	onclose: (() => void) | undefined;
	readonly sent: string[] = [];

	constructor(public url: string) {
		FakeWebSocket.instances.push(this);
	}

	send(data: string) {
		this.sent.push(data);
	}

	close() {
		this.onclose?.();
	}

	open() {
		this.onopen?.();
	}

	emit(frame: ServerFrame) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}

	frames() {
		return this.sent.map((item) => JSON.parse(item));
	}
}

const Impl = FakeWebSocket as unknown as typeof WebSocket;
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const waitForSocket = async (index = 0) => {
	for (let attempt = 0; attempt < 20; attempt++) {
		const socket = FakeWebSocket.instances[index];
		if (socket !== undefined) return socket;
		await tick();
	}
	throw new Error(`Fake WebSocket ${index} was not created`);
};
const waitFor = async (condition: () => boolean, message: string) => {
	for (let attempt = 0; attempt < 50; attempt++) {
		if (condition()) return;
		await tick();
	}
	throw new Error(message);
};

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('createSyncClient durable profile', () => {
	test('uses Auth-provisioned runtime durability without page configuration', async () => {
		const store = createMemorySyncLocalStore();
		await store.transaction('principal-a', 'readwrite', (tx) =>
			tx.putCollection<Order>('orders', {
				rows: [{ id: 9, status: 'runtime-cached' }],
				version: 3
			})
		);
		const uninstall = installSyncClientRuntimeTransport({
			durable: { namespace: 'principal-a', store },
			socketTicket: async () => 'ticket'
		});
		try {
			const client = createSyncClient({
				reconnectMs: 0,
				url: 'ws://test/sync/ws',
				webSocketImpl: Impl
			});
			const orders = client.collection<Order>({ collection: 'orders' });
			const socket = await waitForSocket();
			socket.open();
			await waitFor(
				() =>
					socket.frames().some((frame) => frame.type === 'subscribe'),
				'runtime durable subscribe was not sent'
			);
			expect(orders.get().data).toEqual([
				{ id: 9, status: 'runtime-cached' }
			]);
			client.close();
		} finally {
			uninstall();
		}
	});

	test('hydrates cached rows before subscribing and resumes from the cursor', async () => {
		const store = createIndexedDbSyncLocalStore({
			databaseName: `sync-client-${crypto.randomUUID()}`,
			indexedDB: new IDBFactory()
		});
		await store.transaction('account-a', 'readwrite', (tx) =>
			tx.putCollection<Order>('orders', {
				rows: [{ id: 1, status: 'cached' }],
				version: 7,
				cursor: 'cursor-7'
			})
		);
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => 'installation-a'
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await waitFor(
			() => socket.frames().some((frame) => frame.type === 'subscribe'),
			'durable subscribe was not sent'
		);

		expect(orders.get().data).toEqual([{ id: 1, status: 'cached' }]);
		expect(socket.frames()).toContainEqual({
			type: 'subscribe',
			id: 'c0',
			collection: 'orders',
			since: 'cursor-7'
		});
		client.close();
	});

	test('persists a stable operation and inverse before sending, then removes it on ack', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		socket.emit({
			type: 'snapshot',
			id: 'c0',
			rows: [],
			version: 1
		});
		await tick();

		const result = orders.mutate<{ ok: boolean }>({
			name: 'orders:create',
			args: { id: 2 },
			optimisticOperations: [
				{ type: 'insert', row: { id: 2, status: 'pending' } }
			]
		});
		await tick();

		expect(orders.get().data).toEqual([{ id: 2, status: 'pending' }]);
		const mutate = socket.frames().find((frame) => frame.type === 'mutate');
		expect(mutate).toEqual({
			type: 'mutate',
			mutationId: 1,
			operationId: 'id-1:id-2',
			name: 'orders:create',
			args: { id: 2 }
		});
		const persisted = await store.transaction(
			'account-a',
			'readonly',
			(tx) => tx.listMutations()
		);
		expect(persisted).toEqual([
			expect.objectContaining({
				operationId: 'id-1:id-2',
				owner: 'orders',
				optimistic: [
					{
						type: 'insert',
						collection: 'orders',
						row: { id: 2, status: 'pending' }
					}
				],
				inverse: [{ type: 'delete', collection: 'orders', key: 2 }]
			})
		]);

		socket.emit({
			type: 'diff',
			id: 'c0',
			added: [{ id: 2, status: 'confirmed' }],
			removed: [],
			changed: [],
			version: 2
		});
		socket.emit({
			type: 'ack',
			mutationId: 1,
			operationId: 'id-1:id-2',
			result: { ok: true }
		});

		await expect(result).resolves.toEqual({ ok: true });
		expect(orders.get().data).toEqual([{ id: 2, status: 'confirmed' }]);
		expect(
			await store.transaction('account-a', 'readonly', (tx) =>
				tx.listMutations()
			)
		).toEqual([]);
		client.close();
	});

	test('reconstructs optimism and replays the same operation id after process death', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const first = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const firstOrders = first.collection<Order>({ collection: 'orders' });
		await waitForSocket(0);
		firstOrders
			.mutate({
				name: 'orders:create',
				args: { id: 9 },
				optimisticOperations: [
					{ type: 'insert', row: { id: 9, status: 'offline' } }
				]
			})
			.catch(() => {});
		await tick();
		expect(firstOrders.get().data).toEqual([{ id: 9, status: 'offline' }]);
		first.close();

		const second = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `unexpected-${++sequence}`
			}
		});
		const secondOrders = second.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket(1);
		await tick();
		expect(secondOrders.get().data).toEqual([{ id: 9, status: 'offline' }]);

		socket.open();
		await tick();
		const replay = socket.frames().find((frame) => frame.type === 'mutate');
		expect(replay.operationId).toBe('id-1:id-2');
		expect(replay.args).toEqual({ id: 9 });
		second.close();
	});

	test('one serialized mutation can optimistically update multiple collections', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const counts = client.collection<Order>({ collection: 'counts' });
		await waitForSocket();
		orders
			.mutate({
				name: 'orders:create',
				optimisticOperations: [
					{ type: 'insert', row: { id: 1, status: 'order' } },
					{
						type: 'insert',
						collection: 'counts',
						row: { id: 1, status: 'count' }
					}
				]
			})
			.catch(() => {});
		await tick();

		expect(orders.get().data).toEqual([{ id: 1, status: 'order' }]);
		expect(counts.get().data).toEqual([{ id: 1, status: 'count' }]);
		const records = await store.transaction('account-a', 'readonly', (tx) =>
			tx.listMutations()
		);
		expect(
			records[0]?.optimistic.map((operation) => operation.collection)
		).toEqual(['orders', 'counts']);
		client.close();
	});

	test('persists every collection in one transaction for a consistent frame', async () => {
		const base = createMemorySyncLocalStore();
		const collectionBatches: string[][] = [];
		const store: SyncLocalStore = {
			deleteNamespace: base.deleteNamespace,
			transaction: (namespace, mode, run) =>
				base.transaction(namespace, mode, async (tx) => {
					const keys: string[] = [];
					const result = await run({
						...tx,
						putCollection: async (key, record) => {
							keys.push(key);
							await tx.putCollection(key, record);
						}
					});
					if (keys.length > 0) collectionBatches.push(keys);
					return result;
				})
		};
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => 'installation-a'
			}
		});
		client.collection<Order>({ collection: 'orders' });
		client.collection<Order>({ collection: 'counts' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		socket.emit({
			type: 'frame',
			version: 3,
			cursor: 'cursor-3',
			diffs: [
				{
					id: 'c0',
					added: [{ id: 1, status: 'order' }],
					removed: [],
					changed: []
				},
				{
					id: 'c1',
					added: [{ id: 1, status: 'count' }],
					removed: [],
					changed: []
				}
			]
		});
		await tick();

		expect(collectionBatches.at(-1)?.sort()).toEqual(['counts', 'orders']);
		const saved = await store.transaction(
			'account-a',
			'readonly',
			async (tx) => [
				await tx.getCollection('orders'),
				await tx.getCollection('counts')
			]
		);
		expect(saved.map((item) => item?.cursor)).toEqual([
			'cursor-3',
			'cursor-3'
		]);
		client.close();
	});

	test('loads a 2.16 record without owner using its optimistic collection', async () => {
		const store = createMemorySyncLocalStore();
		const legacy: LocalMutationRecord = {
			operationId: 'installation-a:legacy-op',
			name: 'orders:create',
			args: { id: 4 },
			optimistic: [
				{
					type: 'insert',
					collection: 'orders',
					row: { id: 4, status: 'legacy' }
				}
			],
			inverse: [],
			createdAt: 1,
			attempts: 0
		};
		await store.transaction('account-a', 'readwrite', async (tx) => {
			await tx.setInstallationId('installation-a');
			await tx.putMutation(legacy);
		});
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: { store, namespace: 'account-a' }
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		await waitForSocket();
		await tick();
		expect(orders.get().data).toEqual([{ id: 4, status: 'legacy' }]);
		client.close();
	});

	test('refuses to settle a durable operation without the echoed identity', async () => {
		const errors: unknown[] = [];
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			onError: (error) => errors.push(error),
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		orders
			.mutate({
				name: 'orders:create',
				optimisticOperations: [
					{ type: 'insert', row: { id: 5, status: 'pending' } }
				]
			})
			.catch(() => {});
		await tick();
		socket.emit({ type: 'ack', mutationId: 1, result: { ok: true } });
		await tick();

		expect(orders.get().data).toEqual([{ id: 5, status: 'pending' }]);
		expect(
			errors.some((error) => String(error).includes('identity mismatch'))
		).toBe(true);
		expect(
			await store.transaction('account-a', 'readonly', (tx) =>
				tx.listMutations()
			)
		).toHaveLength(1);
		client.close();
	});

	test('retains conflicts as dead letters and supports explicit retry', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		const result = orders.mutate({
			name: 'orders:update',
			optimisticOperations: [
				{ type: 'insert', row: { id: 3, status: 'optimistic' } }
			]
		});
		await waitFor(
			() => socket.frames().some((frame) => frame.type === 'mutate'),
			'mutation was not sent'
		);
		const sent = socket.frames().find((frame) => frame.type === 'mutate');
		socket.emit({
			type: 'reject',
			mutationId: sent.mutationId,
			operationId: sent.operationId,
			message: 'server row is newer',
			rejection: {
				kind: 'conflict',
				message: 'server row is newer',
				code: 'STALE_ROW',
				details: { version: 9 }
			}
		});

		await expect(result).rejects.toBeInstanceOf(SyncMutationRejectedError);
		expect(orders.get().data).toEqual([]);
		expect(client.status()).toEqual(
			expect.objectContaining({ deadLetters: 1, pending: 0 })
		);
		const [deadLetter] = await client.listDeadLetters();
		expect(deadLetter).toEqual(
			expect.objectContaining({
				operationId: sent.operationId,
				state: 'dead-letter',
				rejection: expect.objectContaining({
					kind: 'conflict',
					code: 'STALE_ROW'
				})
			})
		);

		await client.retryDeadLetter(sent.operationId);
		expect(orders.get().data).toEqual([{ id: 3, status: 'optimistic' }]);
		await waitFor(
			() =>
				socket.frames().filter((frame) => frame.type === 'mutate')
					.length === 2,
			'retried mutation was not sent'
		);
		const retried = socket
			.frames()
			.filter((frame) => frame.type === 'mutate')
			.at(-1);
		expect(retried.operationId).toBe(sent.operationId);
		socket.emit({
			type: 'ack',
			mutationId: retried.mutationId,
			operationId: retried.operationId
		});
		await waitFor(
			() => client.status().pending === 0,
			'retried operation did not settle'
		);
		expect(await client.listDeadLetters()).toEqual([]);
		client.close();
	});

	test('bounds explicitly retryable failures before dead-lettering', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`,
				maxAttempts: 2,
				retryBackoff: () => 0
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		const result = orders.mutate({ name: 'orders:create' });
		await waitFor(
			() => socket.frames().some((frame) => frame.type === 'mutate'),
			'first mutation attempt was not sent'
		);
		const rejection = {
			kind: 'retryable' as const,
			message: 'database temporarily unavailable'
		};
		const first = socket.frames().find((frame) => frame.type === 'mutate');
		socket.emit({
			type: 'reject',
			mutationId: first.mutationId,
			operationId: first.operationId,
			message: rejection.message,
			rejection
		});
		await waitFor(
			() =>
				socket.frames().filter((frame) => frame.type === 'mutate')
					.length === 2,
			'second mutation attempt was not sent'
		);
		const second = socket
			.frames()
			.filter((frame) => frame.type === 'mutate')
			.at(-1);
		socket.emit({
			type: 'reject',
			mutationId: second.mutationId,
			operationId: second.operationId,
			message: rejection.message,
			rejection
		});

		await expect(result).rejects.toBeInstanceOf(SyncMutationRejectedError);
		await tick();
		expect(
			socket.frames().filter((frame) => frame.type === 'mutate')
		).toHaveLength(2);
		expect((await client.listDeadLetters())[0]?.attempts).toBe(2);
		client.close();
	});

	test('flush waits within a finite budget and status observes a successful push', async () => {
		const store = createMemorySyncLocalStore();
		let sequence = 0;
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: {
				store,
				namespace: 'account-a',
				createId: () => `id-${++sequence}`
			}
		});
		const orders = client.collection<Order>({ collection: 'orders' });
		const socket = await waitForSocket();
		socket.open();
		await tick();
		const mutation = orders.mutate({ name: 'orders:create' });
		await waitFor(
			() => socket.frames().some((frame) => frame.type === 'mutate'),
			'mutation was not sent'
		);
		const sent = socket.frames().find((frame) => frame.type === 'mutate');
		const flushing = client.flush({ timeoutMs: 100 });
		socket.emit({
			type: 'ack',
			mutationId: sent.mutationId,
			operationId: sent.operationId
		});
		await mutation;
		expect(await flushing).toEqual({
			deadLetters: 0,
			pending: 0,
			timedOut: false
		});
		expect(client.status()).toEqual(
			expect.objectContaining({
				connection: 'online',
				pending: 0,
				lastSuccessfulPushAt: expect.any(Number)
			})
		);
		client.close();
	});

	test('restores and explicitly discards a retained dead letter', async () => {
		const store = createMemorySyncLocalStore();
		await store.transaction('account-a', 'readwrite', async (tx) => {
			await tx.setInstallationId('installation-a');
			await tx.putMutation({
				args: {},
				attempts: 1,
				createdAt: 1,
				deadLetteredAt: 2,
				inverse: [],
				name: 'orders:update',
				operationId: 'installation-a:operation-a',
				optimistic: [],
				rejection: { kind: 'permanent', message: 'denied' },
				state: 'dead-letter'
			});
		});
		const client = createSyncClient({
			url: 'ws://test/sync/ws',
			webSocketImpl: Impl,
			reconnectMs: 0,
			durable: { store, namespace: 'account-a' }
		});
		await waitForSocket();
		await waitFor(
			() => client.status().deadLetters === 1,
			'dead letter was not restored'
		);
		await client.discardDeadLetter('installation-a:operation-a');
		expect(await client.listDeadLetters()).toEqual([]);
		expect(
			await store.transaction('account-a', 'readonly', (tx) =>
				tx.listMutations()
			)
		).toEqual([]);
		client.close();
	});
});
