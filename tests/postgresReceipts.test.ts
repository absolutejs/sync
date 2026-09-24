import { describe, test, expect } from 'bun:test';
import { SQL } from 'bun';
import {
	createPostgresMutationRunner,
	postgresMutationReceiptsMigration
} from '../src/adapters/postgres/receipts';

const url = process.env.ABS_SYNC_TEST_DATABASE_URL;
describe.skipIf(!url)(
	'Postgres durable mutation receipts (real database)',
	() => {
		test('concurrent replay, scope isolation, argument mismatch and atomic rollback', async () => {
			const sql = new SQL(url!, { max: 4 });
			const schema = `sync_receipt_test_${crypto.randomUUID().replaceAll('-', '')}`;
			try {
				await sql.unsafe(`CREATE SCHEMA "${schema}"`);
				await sql.begin(async (tx) => {
					await tx.unsafe(`SET LOCAL search_path TO "${schema}"`);
					await tx.unsafe(postgresMutationReceiptsMigration);
					await tx.unsafe(
						'CREATE TABLE effects (id text PRIMARY KEY)'
					);
				});
				const run = createPostgresMutationRunner({
					transaction: <R>(fn: (tx: SQL) => Promise<R>) =>
						sql.begin(async (tx) => {
							await tx.unsafe(
								`SET LOCAL search_path TO "${schema}"`
							);
							return fn(tx as unknown as SQL);
						}) as Promise<R>,
					query: async (tx, statement, params) =>
						Array.from(await tx.unsafe(statement, params))
				});
				let executions = 0;
				const operation = {
					scope: 'alice',
					operationId: 'one',
					name: 'create',
					args: { b: 2, a: 1 }
				};
				const execute = async (handle: unknown) => {
					executions++;
					await (handle as SQL).unsafe(
						'INSERT INTO effects VALUES ($1)',
						['alice']
					);
					return { id: 'alice' };
				};
				const results = await Promise.all([
					run(operation, execute),
					run(operation, execute)
				]);
				expect(executions).toBe(1);
				expect(
					results.filter((result) => result.replayed)
				).toHaveLength(1);
				expect(results[0]?.result).toEqual({ id: 'alice' });
				expect(
					(await run({ ...operation, args: { a: 1, b: 2 } }, execute))
						.replayed
				).toBe(true);
				await expect(
					run({ ...operation, args: { a: 3 } }, execute)
				).rejects.toThrow('different arguments');
				await run({ ...operation, scope: 'bob' }, async (handle) => {
					await (handle as SQL).unsafe(
						'INSERT INTO effects VALUES ($1)',
						['bob']
					);
					return undefined;
				});
				expect(
					(await run({ ...operation, scope: 'bob' }, execute)).result
				).toBeUndefined();
				const failed = { ...operation, operationId: 'failed' };
				await expect(
					run(failed, async (handle) => {
						await (handle as SQL).unsafe(
							'INSERT INTO effects VALUES ($1)',
							['rollback']
						);
						throw new Error('injected failure');
					})
				).rejects.toThrow('injected failure');
				const recovered = await run(failed, async (handle) => {
					await (handle as SQL).unsafe(
						'INSERT INTO effects VALUES ($1)',
						['rollback']
					);
					return true;
				});
				expect(recovered.replayed).toBe(false);
				await expect(
					run(
						{ ...operation, operationId: 'bad-result' },
						async (handle) => {
							await (handle as SQL).unsafe(
								'INSERT INTO effects VALUES ($1)',
								['invalid']
							);
							return new Date();
						}
					)
				).rejects.toThrow('JSON');
				await run(
					{ ...operation, operationId: 'bad-result' },
					async (handle) => {
						await (handle as SQL).unsafe(
							'INSERT INTO effects VALUES ($1)',
							['invalid']
						);
						return null;
					}
				);
			} finally {
				await sql.unsafe(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
				await sql.close();
			}
		}, 30000);
	}
);
