/**
 * Benchmark harness for @absolutejs/sync. Run with `bun run bench/run.ts`.
 * Prints Markdown tables; the numbers in the README come from this script.
 *
 * Focus: the things that differentiate the engine + CRDT — delta vs full-state
 * upload (the per-keystroke cost), CRDT merge throughput, tombstone compaction,
 * and engine mutation throughput + diff fan-out.
 */
import {
	createSyncEngine,
	defineMutation,
	defineReactiveQuery
} from '../src/engine';
import {
	compact,
	createTextCrdt,
	mergeTextState,
	tombstoneCount
} from '../src/crdt';

const ns = () => Bun.nanoseconds();
const ms = (start: number) => (ns() - start) / 1e6;
const fmt = (n: number) => n.toLocaleString('en-US');

const bytes = (value: unknown) => JSON.stringify(value).length;

console.log('# @absolutejs/sync benchmarks\n');
console.log(`Bun ${Bun.version} · ${new Date().toISOString().slice(0, 10)}\n`);

/* ── 1. Delta vs full-state upload (per keystroke) ── */
console.log('## CRDT upload payload — one keystroke on an N-char document\n');
console.log(
	'| doc size (chars) | full state (bytes) | delta (bytes) | full / delta |'
);
console.log(
	'| ---------------- | ------------------ | ------------- | ------------ |'
);
for (const size of [100, 1_000, 10_000]) {
	const doc = createTextCrdt('a');
	doc.insert(0, 'x'.repeat(size));
	doc.takeDelta(); // flush the initial load
	doc.insert(size, 'y'); // a single keystroke at the end
	const full = bytes(doc.state());
	const delta = bytes(doc.takeDelta());
	console.log(
		`| ${fmt(size)} | ${fmt(full)} | ${fmt(delta)} | ${(full / delta).toFixed(0)}× |`
	);
}

/* ── 2. CRDT merge throughput ── */
console.log(
	'\n## CRDT merge — combining two concurrently-edited N-char states\n'
);
console.log('| doc size (chars) | merge time (ms) | merges / sec |');
console.log('| ---------------- | --------------- | ------------ |');
for (const size of [1_000, 10_000]) {
	const a = createTextCrdt('a');
	a.insert(0, 'x'.repeat(size));
	const b = createTextCrdt('b', a.state());
	b.insert(size, 'z'.repeat(size));
	const stateA = a.state();
	const stateB = b.state();
	const runs = 50;
	const start = ns();
	for (let index = 0; index < runs; index += 1) {
		mergeTextState(stateA, stateB);
	}
	const each = ms(start) / runs;
	console.log(
		`| ${fmt(size)} | ${each.toFixed(2)} | ${fmt(Math.round(1000 / each))} |`
	);
}

/* ── 3. Tombstone compaction ── */
console.log(
	'\n## Tombstone compaction — type N chars, delete the trailing half\n'
);
console.log(
	'| edits (chars) | tombstones | state before (bytes) | after compact (bytes) | shrink |'
);
console.log(
	'| ------------- | ---------- | -------------------- | --------------------- | ------ |'
);
for (const size of [1_000, 10_000]) {
	const doc = createTextCrdt('a');
	doc.insert(0, 'x'.repeat(size));
	doc.delete(size / 2, size / 2); // delete the trailing half
	const before = doc.state();
	const after = compact(before);
	console.log(
		`| ${fmt(size)} | ${fmt(tombstoneCount(before))} | ${fmt(bytes(before))} | ${fmt(bytes(after))} | ${(bytes(before) / bytes(after)).toFixed(1)}× |`
	);
}

/* ── 4. Engine mutation throughput ── */
console.log(
	'\n## Engine — mutation throughput (write + emit) and diff fan-out\n'
);

type Row = { id: string; n: number };
const makeEngine = (subscribers: number) => {
	const rows = new Map<string, Row>([['c', { id: 'c', n: 0 }]]);
	const engine = createSyncEngine();
	engine.registerReader('counter', { all: () => [...rows.values()] });
	engine.registerWriter<Row>('counter', {
		delete: () => {},
		insert: (row) => {
			rows.set(row.id, row);
			return row;
		},
		update: (row) => {
			rows.set(row.id, row);
			return row;
		}
	});
	engine.registerReactive(
		defineReactiveQuery<Row>({
			key: (row) => row.id,
			name: 'counter',
			run: ({ db }) => db.all<Row>('counter')
		})
	);
	engine.registerMutation(
		defineMutation({
			handler: (_args, _ctx, actions) =>
				actions.update<Row>('counter', {
					id: 'c',
					n: (rows.get('c')?.n ?? 0) + 1
				}),
			name: 'inc'
		})
	);
	for (let index = 0; index < subscribers; index += 1) {
		void engine.subscribe({
			collection: 'counter',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
	}

	return engine;
};

const throughput = async (count: number) => {
	const engine = makeEngine(0);
	const start = ns();
	for (let index = 0; index < count; index += 1) {
		await engine.runMutation('inc', {}, {});
	}

	return ms(start);
};

const fanout = async (subscribers: number, count: number) => {
	const engine = makeEngine(subscribers);
	const start = ns();
	for (let index = 0; index < count; index += 1) {
		await engine.runMutation('inc', {}, {});
	}

	return ms(start);
};

const mutationCount = 10_000;
const mutationMs = await throughput(mutationCount);
console.log(
	`- ${fmt(mutationCount)} mutations in ${mutationMs.toFixed(0)} ms = **${fmt(Math.round(mutationCount / (mutationMs / 1000)))} mutations/sec** (no subscribers)`
);
for (const subscribers of [10, 100]) {
	const count = 2_000;
	const elapsed = await fanout(subscribers, count);
	console.log(
		`- ${fmt(count)} mutations fanned to ${subscribers} live subscribers in ${elapsed.toFixed(0)} ms = **${fmt(Math.round(count / (elapsed / 1000)))} mutations/sec**`
	);
}

console.log('');
