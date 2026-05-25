import { describe, expect, test } from 'bun:test';
import { eq, gt } from 'drizzle-orm';
import {
	integer,
	primaryKey,
	sqliteTable,
	text
} from 'drizzle-orm/sqlite-core';
import {
	deriveReadTopics,
	publishChange,
	publishRows,
	publishWhere
} from '../src/adapters/drizzle/index';
import { createReactiveHub } from '../src/reactiveHub';
import type { ReactiveEvent } from '../src/reactiveHub';

const users = sqliteTable('users', {
	id: integer('id').primaryKey(),
	email: text('email').notNull()
});

const memberships = sqliteTable(
	'memberships',
	{
		userId: integer('user_id').notNull(),
		teamId: integer('team_id').notNull()
	},
	(table) => [primaryKey({ columns: [table.userId, table.teamId] })]
);

const collect = (
	hub: ReturnType<typeof createReactiveHub>,
	topics: string[]
) => {
	const events: ReactiveEvent[] = [];
	hub.subscribe(topics, (event) => events.push(event));
	return events;
};

describe('publishChange', () => {
	test('publishes the table topic with no keys and returns it', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['users']);

		const topics = publishChange(hub, users);

		expect(topics).toEqual(['users']);
		expect(seen.map((event) => event.topic)).toEqual(['users']);
		expect(seen[0]?.payload).toEqual({
			table: 'users',
			op: undefined,
			keys: []
		});
	});

	test('publishes the table topic plus a row topic per key', () => {
		const hub = createReactiveHub();
		const tableSeen = collect(hub, ['users']);
		const rowSeen = collect(hub, ['users:1', 'users:2']);

		const topics = publishChange(hub, users, {
			keys: [1, 2],
			op: 'update'
		});

		expect(topics).toEqual(['users', 'users:1', 'users:2']);
		expect(tableSeen.map((event) => event.topic)).toEqual(['users']);
		expect(rowSeen.map((event) => event.topic)).toEqual([
			'users:1',
			'users:2'
		]);
		expect(rowSeen[0]?.payload).toEqual({
			table: 'users',
			op: 'update',
			keys: [1, 2]
		});
	});

	test('a row-topic change reaches a prefix-wildcard subscriber', () => {
		const hub = createReactiveHub();
		const wildcard = collect(hub, ['users:*']);

		publishChange(hub, users, { keys: [7] });

		expect(wildcard.map((event) => event.topic)).toEqual(['users:7']);
	});

	test('de-duplicates repeated keys', () => {
		const hub = createReactiveHub();
		const topics = publishChange(hub, users, { keys: [1, 1, 2] });
		expect(topics).toEqual(['users', 'users:1', 'users:2']);
	});
});

describe('publishRows', () => {
	test('emits a row topic for each returned row key', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['users', 'users:10', 'users:11']);

		// Shape of `await db.insert(users).values(...).returning()`.
		const rows = [
			{ id: 10, email: 'a@b.com' },
			{ id: 11, email: 'c@d.com' }
		];
		const topics = publishRows(hub, users, rows, { op: 'insert' });

		expect(topics).toEqual(['users', 'users:10', 'users:11']);
		expect(seen.map((event) => event.topic)).toEqual([
			'users',
			'users:10',
			'users:11'
		]);
	});

	test('skips rows missing a usable key', () => {
		const hub = createReactiveHub();
		const topics = publishRows(hub, users, [
			{ email: 'no-id@b.com' },
			{ id: 5, email: 'a@b.com' }
		]);
		expect(topics).toEqual(['users', 'users:5']);
	});

	test('a composite-key table publishes only the table topic', () => {
		const hub = createReactiveHub();
		const topics = publishRows(hub, memberships, [
			{ userId: 1, teamId: 2 }
		]);
		expect(topics).toEqual(['memberships']);
	});
});

describe('publishWhere', () => {
	test('a primary-key equality narrows to the row topic', () => {
		const hub = createReactiveHub();
		const seen = collect(hub, ['users', 'users:5']);

		const topics = publishWhere(hub, users, eq(users.id, 5), {
			op: 'delete'
		});

		expect(topics).toEqual(['users', 'users:5']);
		expect(seen.map((event) => event.topic)).toEqual(['users', 'users:5']);
		expect(seen[1]?.payload).toEqual({
			table: 'users',
			op: 'delete',
			keys: [5]
		});
	});

	test('a non-key filter publishes only the table topic', () => {
		const hub = createReactiveHub();
		const topics = publishWhere(hub, users, gt(users.id, 5));
		expect(topics).toEqual(['users']);
	});
});

describe('read/write symmetry', () => {
	test('a write to row 5 fires exactly the topic a row-5 read subscribed to', () => {
		const hub = createReactiveHub();

		const read = deriveReadTopics(users, eq(users.id, 5));
		expect(read).toEqual({ topics: ['users:5'], rowLevel: true });

		const seen = collect(hub, read.topics);
		publishWhere(hub, users, eq(users.id, 5), { op: 'update' });

		expect(seen.map((event) => event.topic)).toEqual(['users:5']);
	});

	test('a table-level read also wakes on a single-row write', () => {
		const hub = createReactiveHub();

		const read = deriveReadTopics(users); // list query -> ['users']
		const seen = collect(hub, read.topics);

		publishWhere(hub, users, eq(users.id, 5), { op: 'update' });

		expect(seen.map((event) => event.topic)).toEqual(['users']);
	});
});
