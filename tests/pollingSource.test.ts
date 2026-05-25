import { describe, expect, test } from 'bun:test';
import {
	createPollingChangeSource,
	parseOutboxRow
} from '../src/engine/pollingSource';
import type { OutboxRow } from '../src/engine/pollingSource';
import { createSyncEngine } from '../src/engine/syncEngine';
import { defineCollection } from '../src/engine/collection';
import type { RowChange } from '../src/engine/types';

type Item = { id: number; name: string };

describe('parseOutboxRow', () => {
	test('parses a JSON-string payload', () => {
		expect(
			parseOutboxRow({
				seq: 1,
				tbl: 'items',
				op: 'insert',
				payload: '{"id":1,"name":"a"}'
			})
		).toEqual({
			table: 'items',
			change: { op: 'insert', row: { id: 1, name: 'a' } }
		});
	});

	test('parses an already-parsed object payload and upper-case op', () => {
		expect(
			parseOutboxRow({
				seq: 2,
				tbl: 'items',
				op: 'UPDATE',
				payload: { id: 1, name: 'b' }
			})
		).toEqual({
			table: 'items',
			change: { op: 'update', row: { id: 1, name: 'b' } }
		});
	});

	test('skips unknown ops, bad JSON, and non-object payloads', () => {
		expect(
			parseOutboxRow({ seq: 1, tbl: 't', op: 'nope', payload: '{}' })
		).toBeUndefined();
		expect(
			parseOutboxRow({ seq: 1, tbl: 't', op: 'insert', payload: '{bad' })
		).toBeUndefined();
		expect(
			parseOutboxRow({ seq: 1, tbl: 't', op: 'insert', payload: 5 })
		).toBeUndefined();
	});
});

describe('createPollingChangeSource', () => {
	test('drains the backlog on start and advances past malformed rows', async () => {
		const log: OutboxRow[] = [
			{
				seq: 1,
				tbl: 'items',
				op: 'insert',
				payload: { id: 1, name: 'a' }
			},
			{ seq: 2, tbl: 'items', op: 'bogus', payload: { id: 9 } }, // skipped
			{
				seq: 3,
				tbl: 'items',
				op: 'update',
				payload: { id: 1, name: 'b' }
			}
		];
		const emitted: { table: string; change: RowChange<unknown> }[] = [];
		let watermark = 0;
		const source = createPollingChangeSource({
			intervalMs: 5,
			poll: (since) => log.filter((row) => row.seq > since),
			onProcessed: (upto) => {
				watermark = upto;
			}
		});

		await source.start((table, change) => {
			emitted.push({ table, change });
		});
		source.stop();

		expect(emitted).toEqual([
			{
				table: 'items',
				change: { op: 'insert', row: { id: 1, name: 'a' } }
			},
			{
				table: 'items',
				change: { op: 'update', row: { id: 1, name: 'b' } }
			}
		]);
		// cursor advances past the skipped row, so it never re-polls it.
		expect(watermark).toBe(3);
	});

	test('feeds the engine so a polled change updates a live view', async () => {
		const engine = createSyncEngine();
		engine.register(
			defineCollection<Item>({
				name: 'items',
				key: (row) => row.id,
				hydrate: () => [],
				match: () => true
			})
		);
		const diffs: Item[] = [];
		await engine.subscribe<Item>({
			collection: 'items',
			params: undefined,
			ctx: {},
			onDiff: (diff) => diffs.push(...diff.added)
		});

		const log: OutboxRow[] = [
			{
				seq: 1,
				tbl: 'items',
				op: 'insert',
				payload: { id: 7, name: 'x' }
			}
		];
		const source = createPollingChangeSource({
			intervalMs: 5,
			poll: (since) => log.filter((row) => row.seq > since)
		});
		const disconnect = await engine.connectSource(source);

		expect(diffs).toEqual([{ id: 7, name: 'x' }]);
		await disconnect();
	});
});
