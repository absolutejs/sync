import { describe, expect, test } from 'bun:test';
import {
	deriveReadTopics,
	publishChange,
	publishRows,
	publishWhere
} from '../src/adapters/prisma/index';
import { createReactiveHub } from '../src/reactiveHub';
import type { ReactiveEvent } from '../src/reactiveHub';

const collect = (
	hub: ReturnType<typeof createReactiveHub>,
	topics: string[]
) => {
	const events: ReactiveEvent[] = [];
	hub.subscribe(topics, (event) => events.push(event));
	return events;
};

describe('publishChange', () => {
	test('publishes the model topic with no keys', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['user']);

		const topics = publishChange(hub, 'user');

		expect(topics).toEqual(['user']);
		expect(seen.map((event) => event.topic)).toEqual(['user']);
		expect(seen[0]?.payload).toEqual({
			table: 'user',
			op: undefined,
			keys: []
		});
	});

	test('publishes the model topic plus a row topic per key', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['user', 'user:1', 'user:2']);

		const topics = publishChange(hub, 'user', {
			keys: [1, 2],
			op: 'update'
		});

		expect(topics).toEqual(['user', 'user:1', 'user:2']);
		expect(seen.map((event) => event.topic)).toEqual([
			'user',
			'user:1',
			'user:2'
		]);
		expect(seen[1]?.payload).toEqual({
			table: 'user',
			op: 'update',
			keys: [1, 2]
		});
	});

	test('a row-topic change reaches a prefix-wildcard subscriber', () => {
		const hub = createReactiveHub();
		const wildcard = collect(hub, ['user:*']);

		publishChange(hub, 'user', { keys: [7] });

		expect(wildcard.map((event) => event.topic)).toEqual(['user:7']);
	});

	test('de-duplicates repeated keys', () => {
		const hub = createReactiveHub();
		expect(publishChange(hub, 'user', { keys: [1, 1, 2] })).toEqual([
			'user',
			'user:1',
			'user:2'
		]);
	});
});

describe('publishRows', () => {
	test('accepts a single record (create/update/delete result)', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['user', 'user:10']);

		const topics = publishRows(
			hub,
			'user',
			{ id: 10, email: 'a@b.com' },
			{ op: 'insert' }
		);

		expect(topics).toEqual(['user', 'user:10']);
		expect(seen.map((event) => event.topic)).toEqual(['user', 'user:10']);
	});

	test('accepts an array of records', () => {
		const hub = createReactiveHub();
		const topics = publishRows(hub, 'user', [
			{ id: 10, email: 'a@b.com' },
			{ id: 11, email: 'c@d.com' }
		]);
		expect(topics).toEqual(['user', 'user:10', 'user:11']);
	});

	test('skips records missing a usable key', () => {
		const hub = createReactiveHub();
		const topics = publishRows(hub, 'user', [
			{ email: 'no-id@b.com' },
			{ id: 5, email: 'a@b.com' }
		]);
		expect(topics).toEqual(['user', 'user:5']);
	});

	test('honours a keyField override', () => {
		const hub = createReactiveHub();
		const topics = publishRows(
			hub,
			'user',
			{ email: 'a@b.com' },
			{ keyField: 'email' }
		);
		expect(topics).toEqual(['user', 'user:a@b.com']);
	});
});

describe('publishWhere', () => {
	test('a key equality narrows to the row topic', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['user', 'user:5']);

		const topics = publishWhere(hub, 'user', { id: 5 }, { op: 'delete' });

		expect(topics).toEqual(['user', 'user:5']);
		expect(seen.map((event) => event.topic)).toEqual(['user', 'user:5']);
		expect(seen[1]?.payload).toEqual({
			table: 'user',
			op: 'delete',
			keys: [5]
		});
	});

	test('a range filter publishes only the model topic', () => {
		const hub = createReactiveHub();
		expect(publishWhere(hub, 'user', { id: { gt: 5 } })).toEqual(['user']);
	});
});

describe('read/write symmetry', () => {
	test('a write to row 5 fires exactly the topic a row-5 read subscribed to', () => {
		const hub = createReactiveHub();

		const read = deriveReadTopics('user', { id: 5 });
		expect(read).toEqual({ topics: ['user:5'], rowLevel: true });

		const seen = collect(hub, read.topics);
		publishWhere(hub, 'user', { id: 5 }, { op: 'update' });

		expect(seen.map((event) => event.topic)).toEqual(['user:5']);
	});

	test('a model-level read also wakes on a single-row write', () => {
		const hub = createReactiveHub();

		const read = deriveReadTopics('user'); // list query -> ['user']
		const seen = collect(hub, read.topics);

		publishWhere(hub, 'user', { id: 5 }, { op: 'update' });

		expect(seen.map((event) => event.topic)).toEqual(['user']);
	});
});
