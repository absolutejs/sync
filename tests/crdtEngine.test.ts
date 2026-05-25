import { describe, expect, test } from 'bun:test';
import { createSyncEngine } from '../src/engine';
import { createTextCrdt, rgaText, textOf, type TextState } from '../src/crdt';

type DocRow = { id: string; title: string; state: TextState };

// A minimal in-memory "doc" table wired for CRDT auto-merge.
const setup = (registerCrdt: boolean) => {
	const docs = new Map<string, DocRow>();
	docs.set('shared', {
		id: 'shared',
		state: { elements: [] },
		title: 'base'
	});
	const engine = createSyncEngine();
	engine.registerReader('doc', {
		all: () => [...docs.values()],
		get: (id) => docs.get(String(id)),
		key: (row) => (row as DocRow).id
	});
	engine.registerWriter<DocRow>('doc', {
		delete: (row) => {
			docs.delete(row.id);
		},
		insert: (row) => {
			docs.set(row.id, row);
			return row;
		},
		update: (row) => {
			docs.set(row.id, row);
			return row;
		}
	});
	if (registerCrdt) {
		engine.registerCrdt<DocRow>('doc', { state: rgaText });
	}

	return { docs, engine };
};

describe('engine.registerCrdt', () => {
	test('auto-merges declared CRDT fields on write (concurrent edits converge)', async () => {
		const { docs, engine } = setup(true);

		// Two clients edit from the same empty base, then submit through the
		// auto-registered "doc:merge" mutation — no custom server code.
		const a = createTextCrdt('a');
		const b = createTextCrdt('b');
		a.insert(0, 'hello');
		b.insert(0, 'world');
		await engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: a.state() },
			{}
		);
		await engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: b.state() },
			{}
		);

		const text = textOf(docs.get('shared')!.state);
		expect(text).toContain('hello');
		expect(text).toContain('world');
	});

	test('merge is order-independent across replicas', async () => {
		const first = setup(true);
		const second = setup(true);
		const a = createTextCrdt('a');
		const b = createTextCrdt('b');
		a.insert(0, 'alpha');
		b.insert(0, 'omega');

		await first.engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: a.state() },
			{}
		);
		await first.engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: b.state() },
			{}
		);
		// Opposite arrival order on the second engine.
		await second.engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: b.state() },
			{}
		);
		await second.engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: a.state() },
			{}
		);

		expect(textOf(first.docs.get('shared')!.state)).toBe(
			textOf(second.docs.get('shared')!.state)
		);
	});

	test('only declared fields merge — other fields still overwrite', async () => {
		const { docs, engine } = setup(true);
		const a = createTextCrdt('a');
		a.insert(0, 'x');
		await engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: a.state(), title: 'first' },
			{}
		);
		const b = createTextCrdt('b');
		b.insert(0, 'y');
		await engine.runMutation(
			'doc:merge',
			{ id: 'shared', state: b.state(), title: 'second' },
			{}
		);

		const row = docs.get('shared')!;
		// title (not a CRDT field) is last-write-wins…
		expect(row.title).toBe('second');
		// …state (a CRDT field) merged both edits.
		expect(textOf(row.state)).toContain('x');
		expect(textOf(row.state)).toContain('y');
	});

	test('without registerCrdt the field is overwritten (merge is opt-in)', async () => {
		const { docs, engine } = setup(false);
		engine.registerMutation({
			handler: (args, _ctx, actions) => actions.update('doc', args),
			name: 'setDoc'
		});
		const a = createTextCrdt('a');
		const b = createTextCrdt('b');
		a.insert(0, 'hello');
		b.insert(0, 'world');
		await engine.runMutation(
			'setDoc',
			{ id: 'shared', state: a.state(), title: 'base' },
			{}
		);
		await engine.runMutation(
			'setDoc',
			{ id: 'shared', state: b.state(), title: 'base' },
			{}
		);

		// No merge: the second write replaced the first wholesale.
		expect(textOf(docs.get('shared')!.state)).toBe('world');
	});
});
