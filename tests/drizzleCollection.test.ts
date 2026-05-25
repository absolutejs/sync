import { describe, expect, test } from 'bun:test';
import {
	and,
	eq,
	gt,
	gte,
	inArray,
	isNotNull,
	isNull,
	like,
	lt,
	ne,
	not,
	notInArray,
	or
} from 'drizzle-orm';
import type { SQL } from 'drizzle-orm';
import { boolean, integer, pgTable, text } from 'drizzle-orm/pg-core';
import {
	drizzleCollection,
	matchesDrizzleWhere,
	UnsupportedDrizzleFilterError
} from '../src/adapters/drizzle';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

const users = pgTable('users', {
	id: integer('id').primaryKey(),
	name: text('name'),
	age: integer('age'),
	active: boolean('active'),
	// JS property differs from the DB column name on purpose.
	teamId: integer('team_id')
});

type User = {
	id: number;
	name: string | null;
	age: number | null;
	active: boolean;
	teamId: number | null;
};

const user = (over: Partial<User> = {}): User => ({
	id: 1,
	name: 'Ada',
	age: 30,
	active: true,
	teamId: 5,
	...over
});

describe('matchesDrizzleWhere', () => {
	const m = (where: SQL | undefined, row: User) =>
		matchesDrizzleWhere(users, where!, row as Record<string, unknown>);

	test('equality, inequality, and booleans', () => {
		expect(m(eq(users.id, 1), user())).toBe(true);
		expect(m(eq(users.id, 2), user())).toBe(false);
		expect(m(ne(users.name, 'Bob'), user())).toBe(true);
		expect(m(eq(users.active, true), user({ active: true }))).toBe(true);
		expect(m(eq(users.active, true), user({ active: false }))).toBe(false);
	});

	test('comparisons', () => {
		expect(m(gt(users.age, 18), user({ age: 30 }))).toBe(true);
		expect(m(gt(users.age, 30), user({ age: 30 }))).toBe(false);
		expect(m(gte(users.age, 30), user({ age: 30 }))).toBe(true);
		expect(m(lt(users.age, 30), user({ age: 18 }))).toBe(true);
		// null is not comparable → fails ordered comparisons.
		expect(m(gt(users.age, 18), user({ age: null }))).toBe(false);
	});

	test('null checks', () => {
		expect(m(isNull(users.name), user({ name: null }))).toBe(true);
		expect(m(isNull(users.name), user({ name: 'Ada' }))).toBe(false);
		expect(m(isNotNull(users.name), user({ name: 'Ada' }))).toBe(true);
	});

	test('in / not in', () => {
		expect(m(inArray(users.id, [1, 2, 3]), user({ id: 2 }))).toBe(true);
		expect(m(inArray(users.id, [1, 2, 3]), user({ id: 9 }))).toBe(false);
		expect(m(notInArray(users.id, [1, 2]), user({ id: 9 }))).toBe(true);
	});

	test('and / or / not, including nesting', () => {
		expect(
			m(and(eq(users.id, 1), gt(users.age, 18)), user({ id: 1, age: 30 }))
		).toBe(true);
		expect(
			m(and(eq(users.id, 1), gt(users.age, 40)), user({ id: 1, age: 30 }))
		).toBe(false);
		expect(m(or(eq(users.id, 1), eq(users.id, 2)), user({ id: 2 }))).toBe(
			true
		);
		expect(m(not(eq(users.id, 1)), user({ id: 1 }))).toBe(false);
		expect(
			m(
				and(
					eq(users.active, true),
					or(eq(users.id, 1), eq(users.id, 2))
				),
				user({ id: 2, active: true })
			)
		).toBe(true);
	});

	test('resolves a column whose JS property differs from its DB name', () => {
		expect(m(eq(users.teamId, 5), user({ teamId: 5 }))).toBe(true);
		expect(m(eq(users.teamId, 9), user({ teamId: 5 }))).toBe(false);
	});

	test('an unsupported operator throws (engine will refetch)', () => {
		expect(() => m(like(users.name, '%a%'), user())).toThrow(
			UnsupportedDrizzleFilterError
		);
	});
});

type Order = { id: number; userId: number; status: string };

const orders = pgTable('orders', {
	id: integer('id').primaryKey(),
	userId: integer('user_id'),
	status: text('status')
});

const collectDiffs = () => {
	const diffs: ViewDiff<Order>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<Order>) => {
			diffs.push(diff);
		}
	};
};

describe('drizzleCollection', () => {
	test('derives an incremental matcher from the same where', async () => {
		const engine = createSyncEngine();
		engine.register(
			drizzleCollection<Order, { userId: number }>({
				name: 'openOrders',
				table: orders,
				where: (params) =>
					and(
						eq(orders.userId, params.userId),
						eq(orders.status, 'open')
					)!,
				find: () => [] // start empty; we drive changes incrementally
			})
		);

		const { diffs, onDiff } = collectDiffs();
		const sub = await engine.subscribe<Order, { userId: number }>({
			collection: 'openOrders',
			params: { userId: 5 },
			ctx: {},
			onDiff
		});
		expect(sub.initial).toEqual([]);

		// Matches the where (user 5, open) → enters.
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 1, userId: 5, status: 'open' }
		});
		expect(diffs.at(-1)?.added).toEqual([
			{ id: 1, userId: 5, status: 'open' }
		]);

		// Wrong user → no diff.
		const before = diffs.length;
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 2, userId: 9, status: 'open' }
		});
		expect(diffs.length).toBe(before);

		// Status flips away from open → leaves.
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed' }
		});
		expect(diffs.at(-1)?.removed.map((row) => row.id)).toEqual([1]);
	});

	test('defaults the key to the table primary key', async () => {
		const engine = createSyncEngine();
		engine.register(
			drizzleCollection<Order, { userId: number }>({
				name: 'byUser',
				table: orders,
				where: (params) => eq(orders.userId, params.userId),
				find: () => []
			})
		);
		const { diffs, onDiff } = collectDiffs();
		await engine.subscribe<Order, { userId: number }>({
			collection: 'byUser',
			params: { userId: 5 },
			ctx: {},
			onDiff
		});
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 7, userId: 5, status: 'open' }
		});
		// keyed by `id` (the PK) → an update to id 7 replaces, not duplicates.
		await engine.applyChange<Order>('orders', {
			op: 'update',
			row: { id: 7, userId: 5, status: 'closed' }
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 7, userId: 5, status: 'closed' }
		]);
	});

	test('an unsupported filter degrades to a correct refetch', async () => {
		const engine = createSyncEngine();
		const store: Order[] = [];
		engine.register(
			drizzleCollection<Order, void>({
				name: 'searched',
				table: orders,
				// `like` can't be evaluated incrementally → match throws → refetch.
				where: () => like(orders.status, 'op%'),
				find: () =>
					store.filter((order) => order.status.startsWith('op'))
			})
		);
		const { diffs, onDiff } = collectDiffs();
		await engine.subscribe<Order>({
			collection: 'searched',
			params: undefined,
			ctx: {},
			onDiff
		});

		// A real DB write lands in the store, then the change comes through.
		store.push({ id: 1, userId: 5, status: 'open' });
		await engine.applyChange<Order>('orders', {
			op: 'insert',
			row: { id: 1, userId: 5, status: 'open' }
		});

		// Even though the matcher couldn't evaluate `like`, the refetch fallback
		// produced the correct result.
		expect(diffs.at(-1)?.added).toEqual([
			{ id: 1, userId: 5, status: 'open' }
		]);
	});
});
