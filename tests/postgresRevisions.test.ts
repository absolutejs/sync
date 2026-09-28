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
test('schema-qualified revisions retain distinct subscription keys', async () => {
	const sql = postgresTableRevisionsMigration([
		'management_agent.runs',
		'another_agent.runs',
		'tasks'
	]);
	expect(sql).toContain('ON "management_agent"."runs"');
	expect(sql).toContain(
		"absolute_sync_bump_table_revision('management_agent.runs')"
	);
	expect(sql).toContain(
		"absolute_sync_bump_table_revision('another_agent.runs')"
	);
	expect(sql).toContain("absolute_sync_bump_table_revision('tasks')");
	expect(sql).toContain('COALESCE(TG_ARGV[0], TG_TABLE_NAME)');
	for (const table of [
		'a.b.c',
		'.runs',
		'a.',
		"a.runs'); SELECT 1;--",
		'a'.repeat(64)
	])
		expect(() => postgresTableRevisionsMigration([table])).toThrow();
	const events: string[] = [];
	const source = createPostgresTableRevisionSource({
		read: async () => [
			{ table: 'management_agent.runs', revision: '1' },
			{ table: 'another_agent.runs', revision: '1' }
		]
	});
	await source.start(async (table) => {
		events.push(table);
	});
	await source.stop();
	expect(events).toEqual(['management_agent.runs', 'another_agent.runs']);
});

test('ambiguous public table aliases cannot silently replace a subscription trigger', () => {
	expect(() =>
		postgresTableRevisionsMigration(['tasks', 'public.tasks'])
	).toThrow();
});
