import { describe, expect, test } from 'bun:test';
import {
	aggregateOp,
	fromRowChange,
	joinNode,
	materialize
} from '../src/engine/dataflow';
import type { Change } from '../src/engine/dataflow';
import type { AggregateGroup } from '../src/engine/aggregate';
import type { RowChange } from '../src/engine/types';

type Order = { id: number; userId: number; total: number };
type User = { id: number; region: string };
type Joined = { orderId: number; region: string; total: number };

const up = <T>(key: number, row: T): Change<T> => ({ op: 'upsert', key, row });

describe('aggregateOp', () => {
	test('emits group-summary upserts and a delete when a group empties', () => {
		const agg = aggregateOp<Order>({
			key: (o) => o.id,
			groupBy: (o) => o.userId,
			value: (o) => o.total
		});

		const a = agg.push([
			up(1, { id: 1, userId: 5, total: 10 }),
			up(2, { id: 2, userId: 5, total: 30 })
		]);
		const group5 = a.find((c) => c.key === 5);
		expect(group5?.op).toBe('upsert');
		expect((group5?.row as AggregateGroup).sum).toBe(40);

		// remove both rows of group 5 -> the group empties -> delete
		const b = agg.push([
			{ op: 'delete', key: 1, row: { id: 1, userId: 5, total: 10 } },
			{ op: 'delete', key: 2, row: { id: 2, userId: 5, total: 30 } }
		]);
		expect(b.find((c) => c.key === 5)?.op).toBe('delete');
	});

	test('a row moving between groups rebalances both', () => {
		const agg = aggregateOp<Order>({
			key: (o) => o.id,
			groupBy: (o) => o.userId,
			value: (o) => o.total
		});
		agg.push([up(1, { id: 1, userId: 5, total: 10 })]);
		const out = agg.push([up(1, { id: 1, userId: 9, total: 10 })]); // 5 -> 9

		expect(out.find((c) => c.key === 5)?.op).toBe('delete'); // group 5 emptied
		expect((out.find((c) => c.key === 9)?.row as AggregateGroup).sum).toBe(
			10
		);
	});
});

describe('composed: join → aggregate → materialize (sum of order totals by region)', () => {
	test('maintains group sums incrementally across both sources', () => {
		const join = joinNode<Order, User, Joined>({
			leftKey: (o) => o.id,
			rightKey: (u) => u.id,
			leftOn: (o) => o.userId,
			rightOn: (u) => u.id,
			select: (o, u) => ({
				orderId: o.id,
				region: u.region,
				total: o.total
			}),
			key: (j) => j.orderId
		});
		const byRegion = aggregateOp<Joined>({
			key: (j) => j.orderId,
			groupBy: (j) => j.region,
			value: (j) => j.total
		});
		const sink = materialize<AggregateGroup>((g) => g.group);

		const pushOrder = (change: RowChange<Order>) =>
			sink.apply(
				byRegion.push(
					join.pushLeft([fromRowChange(change, (o) => o.id)])
				)
			);
		const pushUser = (change: RowChange<User>) =>
			sink.apply(
				byRegion.push(
					join.pushRight([fromRowChange(change, (u) => u.id)])
				)
			);

		// users in regions; one initial order
		join.hydrate(
			[{ id: 1, userId: 5, total: 10 }],
			[
				{ id: 5, region: 'eu' },
				{ id: 9, region: 'us' }
			]
		);
		sink.apply(
			byRegion.push(
				join.rows().map((row) => ({
					op: 'upsert' as const,
					key: row.orderId,
					row
				}))
			)
		);
		expect(sink.rows().find((g) => g.group === 'eu')?.sum).toBe(10);

		// new order for a us user -> us group appears with 20
		pushOrder({ op: 'insert', row: { id: 2, userId: 9, total: 20 } });
		expect(sink.rows().find((g) => g.group === 'us')?.sum).toBe(20);

		// order 1 grows to 100 -> eu sum updates
		pushOrder({ op: 'update', row: { id: 1, userId: 5, total: 100 } });
		expect(sink.rows().find((g) => g.group === 'eu')?.sum).toBe(100);

		// move user 5 to us -> order 1 leaves eu, joins us
		pushUser({ op: 'update', row: { id: 5, region: 'us' } });
		expect(sink.rows().find((g) => g.group === 'eu')).toBeUndefined();
		expect(sink.rows().find((g) => g.group === 'us')?.sum).toBe(120);
	});
});
