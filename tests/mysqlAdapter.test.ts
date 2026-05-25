import { describe, expect, test } from 'bun:test';
import {
	mysqlBinlogChangeSource,
	mysqlChangelogSchema,
	normalizeBinlogEvent
} from '../src/adapters/mysql';
import type { BinlogRowEvent } from '../src/adapters/mysql';
import type { RowChange } from '../src/engine/types';

describe('mysqlChangelogSchema', () => {
	const sql = mysqlChangelogSchema({
		tables: { users: ['id', 'name'] }
	});

	test('creates a JSON changelog table with backticked identifiers', () => {
		expect(sql).toContain(
			'CREATE TABLE IF NOT EXISTS `absolute_sync_changelog`'
		);
		expect(sql).toContain('seq BIGINT AUTO_INCREMENT PRIMARY KEY');
		expect(sql).toContain('payload JSON NOT NULL');
	});

	test('emits FOR EACH ROW triggers using JSON_OBJECT', () => {
		expect(sql.match(/CREATE TRIGGER/g)?.length).toBe(3);
		expect(sql).toContain(
			'CREATE TRIGGER `absolute_sync_users_insert` AFTER INSERT ON `users` FOR EACH ROW'
		);
		expect(sql).toContain(
			"JSON_OBJECT('id', NEW.`id`, 'name', NEW.`name`)"
		);
		expect(sql).toContain(
			"JSON_OBJECT('id', OLD.`id`, 'name', OLD.`name`)"
		);
	});
});

describe('normalizeBinlogEvent', () => {
	test('writerows -> one insert per row', () => {
		const event: BinlogRowEvent = {
			type: 'WriteRows',
			table: 'users',
			rows: [
				{ id: 1, name: 'a' },
				{ id: 2, name: 'b' }
			]
		};
		expect(normalizeBinlogEvent(event)).toEqual([
			{
				table: 'users',
				change: { op: 'insert', row: { id: 1, name: 'a' } }
			},
			{
				table: 'users',
				change: { op: 'insert', row: { id: 2, name: 'b' } }
			}
		]);
	});

	test('updaterows takes the after image', () => {
		const event: BinlogRowEvent = {
			type: 'updaterows',
			table: 'users',
			rows: [
				{ before: { id: 1, name: 'a' }, after: { id: 1, name: 'z' } }
			]
		};
		expect(normalizeBinlogEvent(event)).toEqual([
			{
				table: 'users',
				change: { op: 'update', row: { id: 1, name: 'z' } }
			}
		]);
	});

	test('deleterows takes the row (or its before image)', () => {
		expect(
			normalizeBinlogEvent({
				type: 'DeleteRows',
				table: 'users',
				rows: [{ id: 3, name: 'c' }]
			})
		).toEqual([
			{
				table: 'users',
				change: { op: 'delete', row: { id: 3, name: 'c' } }
			}
		]);
		expect(
			normalizeBinlogEvent({
				type: 'deleterows',
				table: 'users',
				rows: [{ before: { id: 4 } }]
			})
		).toEqual([
			{ table: 'users', change: { op: 'delete', row: { id: 4 } } }
		]);
	});

	test('ignores unknown event types', () => {
		expect(
			normalizeBinlogEvent({ type: 'rotate', table: 'users', rows: [] })
		).toEqual([]);
	});
});

describe('mysqlBinlogChangeSource', () => {
	test('emits normalized changes and tears down on stop', async () => {
		let handler: ((event: BinlogRowEvent) => void) | undefined;
		let stopped = false;
		const source = mysqlBinlogChangeSource({
			subscribe: (onEvent) => {
				handler = onEvent;
				return () => {
					stopped = true;
				};
			}
		});

		const emitted: { table: string; change: RowChange<unknown> }[] = [];
		await source.start((table, change) => {
			emitted.push({ table, change });
		});

		handler?.({
			type: 'WriteRows',
			table: 'users',
			rows: [{ id: 1, name: 'a' }]
		});
		expect(emitted).toEqual([
			{
				table: 'users',
				change: { op: 'insert', row: { id: 1, name: 'a' } }
			}
		]);

		await source.stop();
		expect(stopped).toBe(true);
	});
});
