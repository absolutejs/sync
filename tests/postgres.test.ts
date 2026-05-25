import { describe, expect, test } from 'bun:test';
import {
	parseNotification,
	postgresChangeSource,
	postgresNotifyTrigger
} from '../src/adapters/postgres/index';
import { defineCollection } from '../src/engine/collection';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

describe('parseNotification', () => {
	test('parses insert/update/delete payloads', () => {
		expect(
			parseNotification(
				JSON.stringify({
					table: 'orders',
					op: 'INSERT',
					row: { id: 1 }
				})
			)
		).toEqual({
			table: 'orders',
			change: { op: 'insert', row: { id: 1 } }
		});
		expect(
			parseNotification(
				JSON.stringify({
					table: 'orders',
					op: 'UPDATE',
					row: { id: 1 }
				})
			)?.change.op
		).toBe('update');
		expect(
			parseNotification(
				JSON.stringify({
					table: 'orders',
					op: 'DELETE',
					row: { id: 1 }
				})
			)?.change.op
		).toBe('delete');
	});

	test('returns undefined for malformed payloads', () => {
		expect(parseNotification('not json')).toBeUndefined();
		expect(
			parseNotification(JSON.stringify({ op: 'INSERT' }))
		).toBeUndefined();
		expect(
			parseNotification(
				JSON.stringify({ table: 'orders', op: 'TRUNCATE', row: {} })
			)
		).toBeUndefined();
		expect(
			parseNotification(
				JSON.stringify({ table: 'orders', op: 'INSERT', row: null })
			)
		).toBeUndefined();
	});
});

describe('postgresChangeSource', () => {
	test('emits parsed notifications and unlistens on stop', async () => {
		let notify: ((payload: string) => void) | undefined;
		let unlistened = false;
		const source = postgresChangeSource({
			listen: (_channel, onNotify) => {
				notify = onNotify;
				return () => {
					unlistened = true;
				};
			}
		});

		const emitted: Array<[string, unknown]> = [];
		await source.start((table, change) => {
			emitted.push([table, change]);
		});

		notify?.(
			JSON.stringify({ table: 'orders', op: 'INSERT', row: { id: 1 } })
		);
		notify?.('garbage'); // skipped, not thrown

		expect(emitted).toEqual([['orders', { op: 'insert', row: { id: 1 } }]]);

		await source.stop();
		expect(unlistened).toBe(true);
	});

	test('passes the configured channel to listen', async () => {
		let usedChannel = '';
		const source = postgresChangeSource({
			channel: 'my_channel',
			listen: (channel) => {
				usedChannel = channel;
				return () => {};
			}
		});
		await source.start(() => {});
		expect(usedChannel).toBe('my_channel');
	});

	test('feeds out-of-band changes through engine.connectSource', async () => {
		const orders = [{ id: 1, userId: 5 }];
		const engine = createSyncEngine();
		engine.register(
			defineCollection<
				{ id: number; userId: number },
				{ userId: number }
			>({
				name: 'orders',
				hydrate: (p) => orders.filter((o) => o.userId === p.userId),
				match: (o, p) => o.userId === p.userId
			})
		);
		const diffs: ViewDiff<{ id: number; userId: number }>[] = [];
		await engine.subscribe<
			{ id: number; userId: number },
			{ userId: number }
		>({
			collection: 'orders',
			params: { userId: 5 },
			ctx: {},
			onDiff: (diff) => diffs.push(diff)
		});

		let notify: ((payload: string) => void) | undefined;
		await engine.connectSource(
			postgresChangeSource({
				listen: (_channel, onNotify) => {
					notify = onNotify;
					return () => {};
				}
			})
		);

		// Simulate an out-of-band INSERT firing the trigger NOTIFY.
		notify?.(
			JSON.stringify({
				table: 'orders',
				op: 'INSERT',
				row: { id: 2, userId: 5 }
			})
		);
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(diffs[0]?.added.map((o) => o.id)).toEqual([2]);
	});
});

describe('postgresNotifyTrigger', () => {
	test('emits a notify function and a trigger per table', () => {
		const sql = postgresNotifyTrigger({ tables: ['orders', 'users'] });
		expect(sql).toContain(
			'CREATE OR REPLACE FUNCTION absolute_sync_notify()'
		);
		expect(sql).toContain("pg_notify('absolute_sync'");
		expect(sql).toContain('row_to_json(COALESCE(NEW, OLD))');
		expect(sql).toContain('AFTER INSERT OR UPDATE OR DELETE ON orders');
		expect(sql).toContain('AFTER INSERT OR UPDATE OR DELETE ON users');
	});

	test('honours channel and function name overrides', () => {
		const sql = postgresNotifyTrigger({
			tables: ['orders'],
			channel: 'feed',
			functionName: 'notify_fn'
		});
		expect(sql).toContain('CREATE OR REPLACE FUNCTION notify_fn()');
		expect(sql).toContain("pg_notify('feed'");
		expect(sql).toContain('CREATE TRIGGER notify_fn_orders');
	});
});
