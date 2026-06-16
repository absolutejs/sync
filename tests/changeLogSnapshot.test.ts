import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createInMemoryClusterBus } from '../src/engine/cluster';

type Task = { id: number; title: string };

const wireEngine = (instanceId: string) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ instanceId });
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	engine.register(
		defineCollection<Task>({
			hydrate: () => [...store.values()],
			key: (task) => task.id,
			match: () => true,
			name: 'tasks'
		})
	);
	return { engine, store };
};

const wireEngineFromSnapshot = (
	instanceId: string,
	initialChangeLog: ReturnType<
		ReturnType<typeof wireEngine>['engine']['exportChangeLog']
	>
) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ initialChangeLog, instanceId });
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	engine.register(
		defineCollection<Task>({
			hydrate: () => [...store.values()],
			key: (task) => task.id,
			match: () => true,
			name: 'tasks'
		})
	);
	return { engine, store };
};

describe('engine.exportChangeLog() / importChangeLog() — 1.19.0', () => {
	test('exports a shallow snapshot of the bounded log', async () => {
		const { engine } = wireEngine('engine-A');
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'one' }
		});
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'two' }
		});
		const snapshot = engine.exportChangeLog();
		expect(snapshot.instanceId).toBe('engine-A');
		expect(snapshot.version).toBe(2);
		expect(snapshot.entries).toHaveLength(2);
		expect(typeof snapshot.exportedAt).toBe('number');
	});

	test('boot restore via initialChangeLog preserves cursor resumability', async () => {
		const { engine: a } = wireEngine('engine-A');
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'before-snapshot' }
		});
		// Client subscribes, captures a cursor, then "goes offline."
		const sub = await a.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const cursor = sub.cursor;
		sub.unsubscribe();

		// A keeps writing while client is offline.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'while-offline' }
		});

		// Host snapshots the log, "reboots" the shard.
		const snapshot = a.exportChangeLog();

		// New engine on a fresh process, same instanceId, log restored.
		const { engine: aPrime } = wireEngineFromSnapshot('engine-A', snapshot);

		// Client reconnects to the restarted shard with its old cursor.
		const resumed = await aPrime.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: cursor
		});
		expect(resumed.catchup).toBeDefined();
		expect(resumed.catchup!.changed).toContainEqual({
			id: 2,
			title: 'while-offline'
		});
		resumed.unsubscribe();
	});

	test('cluster resume survives a peer reboot', async () => {
		const bus = createInMemoryClusterBus();
		const { engine: a } = wireEngine('engine-A');
		const { engine: b } = wireEngine('engine-B');
		await a.connectCluster(bus);
		const offB = await b.connectCluster(bus);

		// A writes; bus relays to B.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'via-A' }
		});
		await new Promise((resolve) => setTimeout(resolve, 5));

		// B captures a cursor that references both A and B.
		const sub = await b.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const cursor = sub.cursor;
		sub.unsubscribe();

		// B snapshots, "reboots" with the snapshot. Same instanceId.
		const bSnapshot = b.exportChangeLog();
		await offB();
		const { engine: bPrime } = wireEngineFromSnapshot(
			'engine-B',
			bSnapshot
		);

		// Client reconnects to the restarted B with its old cursor.
		// B' has A's prior peer change in its restored log → resumable.
		const resumed = await bPrime.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: cursor
		});
		// Cursor is at-or-after the recorded entries, so catchup is empty
		// but the resume succeeds (no fresh snapshot).
		expect(resumed.catchup).toBeDefined();
		resumed.unsubscribe();
	});

	test('refuses snapshots from a different instanceId', () => {
		const { engine: a } = wireEngine('engine-A');
		const snapshot = a.exportChangeLog();
		expect(() =>
			createSyncEngine({
				initialChangeLog: snapshot,
				instanceId: 'engine-B'
			})
		).toThrow(/does not match/);
	});

	test('refuses import after a local write has committed', async () => {
		const { engine: a } = wireEngine('engine-A');
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'one' }
		});
		const snapshot = a.exportChangeLog();
		const { engine: b } = wireEngine('engine-A');
		await b.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'b-wrote-first' }
		});
		expect(() => b.importChangeLog(snapshot)).toThrow(
			/already has version/
		);
	});

	test('trims imported entries that exceed changeLogSize', () => {
		const { engine: a } =
			createSyncEngine({
				changeLogSize: 1024,
				instanceId: 'engine-A'
			}).constructor === Function
				? // unreachable cast, but it satisfies the constructor-type narrowing
					({} as never)
				: { engine: null as never };
		// Use a fresh tiny engine.
		const fresh = createSyncEngine({
			changeLogSize: 3,
			instanceId: 'engine-A'
		});
		fresh.registerReader('tasks', { all: () => [] });
		fresh.register(
			defineCollection<Task>({
				hydrate: () => [],
				key: (t) => t.id,
				match: () => true,
				name: 'tasks'
			})
		);

		// Hand-build a snapshot with 5 entries — exceeds capacity of 3.
		const now = Date.now();
		const snap = {
			entries: Array.from({ length: 5 }, (_, i) => ({
				at: now - (4 - i),
				change: {
					op: 'insert' as const,
					row: { id: i + 1, title: `t-${i + 1}` }
				},
				origin: 'engine-A',
				originVersion: i + 1,
				table: 'tasks',
				version: i + 1
			})),
			instanceId: 'engine-A',
			version: 5
		};
		const imported = fresh.importChangeLog(snap);
		expect(imported).toBe(5);
		// Engine retains only the last 3 entries; cursor for the latest
		// version still resumes against the bounded tail.
		const metrics = fresh.metrics();
		expect(metrics.changeLog.entries).toBe(3);
		expect(metrics.version).toBe(5);
	});

	test('snapshot is a copy — mutating the engine after export does not affect it', async () => {
		const { engine } = wireEngine('engine-A');
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'one' }
		});
		const snapshot = engine.exportChangeLog();
		const beforeLen = snapshot.entries.length;
		await engine.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'two' }
		});
		expect(snapshot.entries.length).toBe(beforeLen);
	});
});
