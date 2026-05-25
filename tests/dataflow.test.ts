import { describe, expect, test } from 'bun:test';
import {
	chain,
	filterOp,
	fromRowChange,
	joinNode,
	mapOp,
	materialize
} from '../src/engine/dataflow';
import type { Change } from '../src/engine/dataflow';
import type { RowChange } from '../src/engine/types';

type Order = { id: number; userId: number; status: string; total: number };
type User = { id: number; name: string };
type Joined = { orderId: number; userName: string; total: number };

const up = <T>(key: number, row: T): Change<T> => ({ op: 'upsert', key, row });

describe('stateless operators', () => {
	test('filterOp passes matches and deletes non-matches', () => {
		const open = filterOp<Order>((o) => o.status === 'open');
		const out = open.push([
			up(1, { id: 1, userId: 5, status: 'open', total: 1 }),
			up(2, { id: 2, userId: 5, status: 'closed', total: 1 })
		]);
		expect(out.map((c) => [c.op, c.key])).toEqual([
			['upsert', 1],
			['delete', 2]
		]);
	});

	test('mapOp transforms rows and can rekey', () => {
		const m = mapOp<Order, { oid: number }>(
			(o) => ({ oid: o.id }),
			(r) => r.oid
		);
		const out = m.push([
			up(1, { id: 1, userId: 5, status: 'open', total: 9 })
		]);
		expect(out).toEqual([{ op: 'upsert', key: 1, row: { oid: 1 } }]);
	});

	test('chain composes operators left to right', () => {
		const pipe = chain(
			filterOp<Order>((o) => o.total > 0),
			mapOp<Order, number>((o) => o.total)
		);
		const out = pipe.push([
			up(1, { id: 1, userId: 5, status: 'open', total: 10 }),
			up(2, { id: 2, userId: 5, status: 'open', total: 0 })
		]);
		expect(out.map((c) => [c.op, c.row])).toEqual([
			['upsert', 10],
			['delete', 0] // total 0 filtered out -> delete (row passed through chain)
		]);
	});
});

describe('materialize sink', () => {
	test('turns a change stream into a result-set diff', () => {
		const sink = materialize<Joined>((j) => j.orderId);
		const d1 = sink.apply([
			up(1, { orderId: 1, userName: 'Ada', total: 1 })
		]);
		expect(d1.added.map((r) => r.orderId)).toEqual([1]);

		const d2 = sink.apply([
			up(1, { orderId: 1, userName: 'Ada', total: 2 })
		]);
		expect(d2.changed.map((r) => r.total)).toEqual([2]);

		const d3 = sink.apply([{ op: 'delete', key: 1, row: {} as Joined }]);
		expect(d3.removed.map((r) => r.orderId)).toEqual([1]);
		expect(sink.rows()).toEqual([]);
	});
});

describe('joinNode', () => {
	test('emits upsert/delete change streams from each side', () => {
		const j = joinNode<Order, User, Joined>({
			leftKey: (o) => o.id,
			rightKey: (u) => u.id,
			leftOn: (o) => o.userId,
			rightOn: (u) => u.id,
			select: (o, u) => ({
				orderId: o.id,
				userName: u.name,
				total: o.total
			}),
			key: (out) => out.orderId
		});
		j.hydrate([], [{ id: 5, name: 'Ada' }]);

		const added = j.pushLeft([
			up(1, { id: 1, userId: 5, status: 'open', total: 1 })
		]);
		expect(added).toEqual([
			{
				op: 'upsert',
				key: 1,
				row: { orderId: 1, userName: 'Ada', total: 1 }
			}
		]);

		const removed = j.pushLeft([
			{ op: 'delete', key: 1, row: { id: 1, userId: 5 } as Order }
		]);
		expect(removed.map((c) => [c.op, c.key])).toEqual([['delete', 1]]);
	});
});

describe('composed graph: orders → filter(open) → join(users) → map → materialize', () => {
	test('maintains the result incrementally across both sources', () => {
		const open = filterOp<Order>((o) => o.status === 'open');
		const join = joinNode<Order, User, Joined>({
			leftKey: (o) => o.id,
			rightKey: (u) => u.id,
			leftOn: (o) => o.userId,
			rightOn: (u) => u.id,
			select: (o, u) => ({
				orderId: o.id,
				userName: u.name,
				total: o.total
			}),
			key: (out) => out.orderId
		});
		const label = mapOp<Joined, Joined & { label: string }>((j) => ({
			...j,
			label: `${j.userName}:${j.total}`
		}));
		const sink = materialize<Joined & { label: string }>((j) => j.orderId);

		const pushOrder = (change: RowChange<Order>) =>
			sink.apply(
				label.push(
					join.pushLeft(
						open.push([fromRowChange(change, (o) => o.id)])
					)
				)
			);
		const pushUser = (change: RowChange<User>) =>
			sink.apply(
				label.push(join.pushRight([fromRowChange(change, (u) => u.id)]))
			);

		// initial: user Ada exists, one open order
		join.hydrate(
			[{ id: 1, userId: 5, status: 'open', total: 10 }],
			[{ id: 5, name: 'Ada' }]
		);
		sink.apply(
			label.push(
				join.rows().map((row) => ({
					op: 'upsert' as const,
					key: row.orderId,
					row
				}))
			)
		);
		expect(sink.rows().map((r) => r.label)).toEqual(['Ada:10']);

		// a new open order enters
		const d1 = pushOrder({
			op: 'insert',
			row: { id: 2, userId: 5, status: 'open', total: 20 }
		});
		expect(d1.added.map((r) => r.label)).toEqual(['Ada:20']);

		// rename the user -> both joined rows change
		const d2 = pushUser({ op: 'update', row: { id: 5, name: 'Bob' } });
		expect(d2.changed.map((r) => r.label).sort()).toEqual([
			'Bob:10',
			'Bob:20'
		]);

		// order 1 closes -> filtered out -> leaves the result
		const d3 = pushOrder({
			op: 'update',
			row: { id: 1, userId: 5, status: 'closed', total: 10 }
		});
		expect(d3.removed.map((r) => r.orderId)).toEqual([1]);
		expect(
			sink
				.rows()
				.map((r) => r.orderId)
				.sort()
		).toEqual([2]);
	});
});
