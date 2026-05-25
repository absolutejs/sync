import { describe, expect, test } from 'bun:test';
import { sqliteChangelogSchema } from '../src/adapters/sqlite';

describe('sqliteChangelogSchema', () => {
	const sql = sqliteChangelogSchema({
		tables: { users: ['id', 'name'], orders: ['id', 'userId', 'total'] }
	});

	test('creates the changelog table once', () => {
		expect(sql).toContain(
			'CREATE TABLE IF NOT EXISTS absolute_sync_changelog'
		);
		expect(sql).toContain('seq INTEGER PRIMARY KEY AUTOINCREMENT');
	});

	test('emits an insert/update/delete trigger per table', () => {
		for (const op of ['insert', 'update', 'delete']) {
			expect(sql).toContain(`absolute_sync_users_${op}`);
			expect(sql).toContain(`absolute_sync_orders_${op}`);
		}
		// 2 tables * 3 ops = 6 triggers.
		expect(sql.match(/CREATE TRIGGER/g)?.length).toBe(6);
	});

	test('delete triggers capture OLD; insert/update capture NEW', () => {
		expect(sql).toContain(
			"AFTER DELETE ON users\nBEGIN\n\tINSERT INTO absolute_sync_changelog (tbl, op, payload)\n\tVALUES ('users', 'delete', json_object('id', OLD.id, 'name', OLD.name));"
		);
		expect(sql).toContain("json_object('id', NEW.id, 'name', NEW.name)");
	});

	test('honors a custom changelog table and prefix', () => {
		const custom = sqliteChangelogSchema({
			tables: { t: ['id'] },
			changelogTable: 'cdc_log',
			prefix: 'cdc'
		});
		expect(custom).toContain('CREATE TABLE IF NOT EXISTS cdc_log');
		expect(custom).toContain('CREATE TRIGGER cdc_t_insert');
		expect(custom).toContain('INSERT INTO cdc_log');
	});
});
