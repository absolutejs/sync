import { describe, expect, test } from 'bun:test';
import { createEquiJoin } from '../src/engine/equiJoin';

type Order = { id: number; userId: number; total: number };
type User = { id: number; name: string };
type Joined = { orderId: number; userId: number; userName: string };

const order = (id: number, userId: number, total = 0): Order => ({
	id,
	userId,
	total
});
const user = (id: number, name: string): User => ({ id, name });

const join = () =>
	createEquiJoin<Order, User, Joined>({
		leftKey: (o) => o.id,
		rightKey: (u) => u.id,
		leftOn: (o) => o.userId,
		rightOn: (u) => u.id,
		select: (o, u) => ({
			orderId: o.id,
			userId: o.userId,
			userName: u.name
		})
	});

const ids = (rows: Joined[]) => rows.map((r) => r.orderId).sort();

describe('createEquiJoin', () => {
	test('hydrate joins matching pairs (inner join)', () => {
		const j = join();
		j.hydrate(
			[order(1, 5), order(2, 9), order(3, 99)], // order 3 has no user
			[user(5, 'Ada'), user(9, 'Bob')]
		);
		expect(j.size()).toBe(2);
		expect(ids(j.rows())).toEqual([1, 2]);
		expect(j.rows().find((r) => r.orderId === 1)?.userName).toBe('Ada');
	});

	test('inserting a left row that matches a right emits added', () => {
		const j = join();
		j.hydrate([], [user(5, 'Ada')]);
		const diff = j.applyLeft({ op: 'insert', row: order(1, 5) });
		expect(diff.added).toEqual([
			{ orderId: 1, userId: 5, userName: 'Ada' }
		]);
		expect(diff.removed).toEqual([]);
	});

	test('inserting a left row with no matching right emits nothing', () => {
		const j = join();
		j.hydrate([], [user(5, 'Ada')]);
		const diff = j.applyLeft({ op: 'insert', row: order(1, 99) });
		expect(diff).toEqual({ added: [], removed: [], changed: [] });
		expect(j.size()).toBe(0);
	});

	test('inserting a right row matches all waiting lefts', () => {
		const j = join();
		j.hydrate([order(1, 5), order(2, 5)], []); // both waiting for user 5
		const diff = j.applyRight({ op: 'insert', row: user(5, 'Ada') });
		expect(ids(diff.added)).toEqual([1, 2]);
		expect(j.size()).toBe(2);
	});

	test('a right-side update changes all joined rows for that key', () => {
		const j = join();
		j.hydrate([order(1, 5), order(2, 5)], [user(5, 'Ada')]);
		const diff = j.applyRight({ op: 'update', row: user(5, 'Bob') });
		expect(diff.changed.map((r) => r.userName).sort()).toEqual([
			'Bob',
			'Bob'
		]);
		expect(diff.added).toEqual([]);
		expect(diff.removed).toEqual([]);
	});

	test('a left join-key change moves the row to the new match', () => {
		const j = join();
		j.hydrate([order(1, 5)], [user(5, 'Ada'), user(9, 'Bob')]);
		const diff = j.applyLeft({ op: 'update', row: order(1, 9) }); // 5 -> 9
		expect(diff.removed.map((r) => r.userName)).toEqual(['Ada']);
		expect(diff.added.map((r) => r.userName)).toEqual(['Bob']);
		expect(j.rows()[0]?.userName).toBe('Bob');
	});

	test('a left value change (same key) emits changed', () => {
		const j = createEquiJoin<Order, User, Joined & { total: number }>({
			leftKey: (o) => o.id,
			rightKey: (u) => u.id,
			leftOn: (o) => o.userId,
			rightOn: (u) => u.id,
			select: (o, u) => ({
				orderId: o.id,
				userId: o.userId,
				userName: u.name,
				total: o.total
			})
		});
		j.hydrate([order(1, 5, 10)], [user(5, 'Ada')]);
		const diff = j.applyLeft({ op: 'update', row: order(1, 5, 99) });
		expect(diff.changed.map((r) => r.total)).toEqual([99]);
		expect(diff.added).toEqual([]);
	});

	test('deleting a right removes every row joined to it', () => {
		const j = join();
		j.hydrate([order(1, 5), order(2, 5)], [user(5, 'Ada')]);
		const diff = j.applyRight({ op: 'delete', row: user(5, 'Ada') });
		expect(ids(diff.removed)).toEqual([1, 2]);
		expect(j.size()).toBe(0);
	});

	test('deleting a left removes its joined row', () => {
		const j = join();
		j.hydrate([order(1, 5)], [user(5, 'Ada')]);
		const diff = j.applyLeft({ op: 'delete', row: order(1, 5) });
		expect(diff.removed).toEqual([
			{ orderId: 1, userId: 5, userName: 'Ada' }
		]);
		expect(j.size()).toBe(0);
	});

	test('a sequence keeps the joined result consistent', () => {
		const j = join();
		j.hydrate([], []);
		j.applyRight({ op: 'insert', row: user(5, 'Ada') });
		j.applyLeft({ op: 'insert', row: order(1, 5) }); // +order1/Ada
		j.applyLeft({ op: 'insert', row: order(2, 9) }); // no match yet
		j.applyRight({ op: 'insert', row: user(9, 'Bob') }); // +order2/Bob
		j.applyLeft({ op: 'update', row: order(1, 9) }); // order1 -> Bob
		j.applyRight({ op: 'update', row: user(9, 'Zoe') }); // both -> Zoe

		expect(ids(j.rows())).toEqual([1, 2]);
		expect(j.rows().every((r) => r.userName === 'Zoe')).toBe(true);
	});
});
