import { describe, expect, test } from 'bun:test';
import { and, eq, gt, inArray } from 'drizzle-orm';
import {
	integer,
	primaryKey,
	sqliteTable,
	text
} from 'drizzle-orm/sqlite-core';
import {
	deriveReadTopics,
	keyTopic,
	tableTopic
} from '../src/adapters/drizzle/index';

const users = sqliteTable('users', {
	id: integer('id').primaryKey(),
	email: text('email').notNull(),
	teamId: integer('team_id')
});

const sessions = sqliteTable('sessions', {
	token: text('token').primaryKey(),
	userId: integer('user_id')
});

// Composite primary key — no single key column to narrow on.
const memberships = sqliteTable(
	'memberships',
	{
		userId: integer('user_id').notNull(),
		teamId: integer('team_id').notNull()
	},
	(table) => [primaryKey({ columns: [table.userId, table.teamId] })]
);

// No primary key at all.
const events = sqliteTable('events', {
	kind: text('kind'),
	at: integer('at')
});

describe('topic vocabulary', () => {
	test('tableTopic is the table name', () => {
		expect(tableTopic(users)).toBe('users');
	});

	test('keyTopic joins table and key', () => {
		expect(keyTopic(users, 5)).toBe('users:5');
		expect(keyTopic(sessions, 'abc')).toBe('sessions:abc');
	});
});

describe('deriveReadTopics', () => {
	test('an unfiltered read depends on the whole-table topic', () => {
		expect(deriveReadTopics(users)).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});

	test('a numeric primary-key equality narrows to a row topic', () => {
		expect(deriveReadTopics(users, eq(users.id, 5))).toEqual({
			topics: ['users:5'],
			rowLevel: true
		});
	});

	test('a string primary-key equality narrows to a row topic', () => {
		expect(deriveReadTopics(sessions, eq(sessions.token, 'abc'))).toEqual({
			topics: ['sessions:abc'],
			rowLevel: true
		});
	});

	test('equality on a non-key column falls back to the table topic', () => {
		expect(deriveReadTopics(users, eq(users.email, 'a@b.com'))).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});

	test('a range filter falls back to the table topic', () => {
		expect(deriveReadTopics(users, gt(users.id, 5))).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});

	test('a multi-key set filter falls back to the table topic', () => {
		expect(deriveReadTopics(users, inArray(users.id, [1, 2, 3]))).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});

	test('a compound (and) filter falls back to the table topic', () => {
		const where = and(eq(users.id, 5), gt(users.teamId, 1));
		expect(deriveReadTopics(users, where)).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});

	test('an explicit keyColumn narrows on a non-primary column', () => {
		const where = eq(users.email, 'a@b.com');
		expect(deriveReadTopics(users, where, { keyColumn: 'email' })).toEqual({
			topics: ['users:a@b.com'],
			rowLevel: true
		});
	});

	test('a composite primary key cannot narrow to a row', () => {
		expect(
			deriveReadTopics(memberships, eq(memberships.userId, 1))
		).toEqual({ topics: ['memberships'], rowLevel: false });
	});

	test('a table with no primary key cannot narrow to a row', () => {
		expect(deriveReadTopics(events, eq(events.kind, 'click'))).toEqual({
			topics: ['events'],
			rowLevel: false
		});
	});

	test('an equality on a different table is not mistaken for this row key', () => {
		// A filter referencing another table must not produce `users:<id>`.
		expect(deriveReadTopics(users, eq(sessions.userId, 5))).toEqual({
			topics: ['users'],
			rowLevel: false
		});
	});
});
