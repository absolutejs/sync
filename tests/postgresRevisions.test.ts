import { test, expect } from 'bun:test';
import {
	createPostgresTableRevisionSource,
	postgresTableRevisionsMigration
} from '../src/adapters/postgres/revisions';
test('revision wakeups emit changed tables only and retry a failed delivery', async () => {
	let wake: () => void = () => {};
	let revision = '1',
		fail = false,
		calls = 0;
	const events: string[] = [];
	const source = createPostgresTableRevisionSource({
		read: async () => [{ table: 'tasks', revision }],
		listen: async (_channel, fn) => {
			wake = fn;
			return () => {};
		},
		onError: () => {},
		reconcileMs: 10000
	});
	await source.start(async (_table, change) => {
		calls++;
		if (fail) throw Error('delivery failed');
		events.push((change.row as { revision: string }).revision);
	});
	const settle = () => new Promise((resolve) => setTimeout(resolve, 10));
	wake();
	await settle();
	expect(events).toEqual(['1']);
	revision = '2';
	fail = true;
	wake();
	await settle();
	expect(events).toEqual(['1']);
	fail = false;
	wake();
	await settle();
	expect(events).toEqual(['1', '2']);
	await source.stop();
	revision = '3';
	wake();
	await settle();
	expect(calls).toBe(3);
});
test('revision migrations reject unsafe table names and capture statements without row payloads', () => {
	expect(() =>
		postgresTableRevisionsMigration(['tasks; DROP TABLE users'])
	).toThrow();
	const sql = postgresTableRevisionsMigration(['tasks', 'tasks']);
	expect(sql.match(/CREATE TRIGGER/g)?.length).toBe(1);
	expect(sql).toContain('OR TRUNCATE');
	expect(sql).not.toContain('row_to_json');
});
