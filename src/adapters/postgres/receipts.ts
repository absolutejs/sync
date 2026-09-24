import type { DurableMutationRunner } from '../../engine/mutation';

/** Minimal parameterized SQL seam; use the SAME transaction passed to execute. */
export type PostgresReceiptDatabase<Tx> = {
	transaction: <R>(run: (tx: Tx) => Promise<R>) => Promise<R>;
	query: (
		tx: Tx,
		statement: string,
		parameters: unknown[]
	) => Promise<Record<string, unknown>[]>;
};

export const postgresMutationReceiptsMigration = `CREATE TABLE IF NOT EXISTS absolute_sync_mutation_receipts (
 scope text NOT NULL,
 operation_id text NOT NULL,
 fingerprint text NOT NULL,
 result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (scope, operation_id)
);`;

const canonical = (value: unknown): string => {
	if (
		value === null ||
		typeof value === 'string' ||
		typeof value === 'boolean'
	)
		return JSON.stringify(value);
	if (typeof value === 'number' && Number.isFinite(value))
		return JSON.stringify(value);
	if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
	if (
		typeof value === 'object' &&
		value !== null &&
		Object.getPrototypeOf(value) === Object.prototype
	)
		return (
			'{' +
			Object.keys(value)
				.sort()
				.map(
					(key) =>
						JSON.stringify(key) +
						':' +
						canonical((value as Record<string, unknown>)[key])
				)
				.join(',') +
			'}'
		);
	throw new TypeError(
		'Durable Postgres mutations require JSON arguments/results'
	);
};

/** Atomic receipt and business writes, serialized by principal + operation id.
 * Keep receipts at least as long as clients can replay operations. Deleting them
 * permits old operations to execute again. No runtime DDL or automatic pruning.
 */
export const createPostgresMutationRunner =
	<Tx>(database: PostgresReceiptDatabase<Tx>): DurableMutationRunner =>
	async <R>(
		operation: Parameters<DurableMutationRunner>[0],
		execute: (tx: unknown) => Promise<R>
	) => {
		if (!operation.scope || !operation.operationId || !operation.name)
			throw new TypeError(
				'A scope, operation id and mutation name are required'
			);
		const input = canonical({
			name: operation.name,
			args: operation.args ?? null
		});
		const hash = await crypto.subtle.digest(
			'SHA-256',
			new TextEncoder().encode(input)
		);
		const fingerprint = Array.from(new Uint8Array(hash), (byte) =>
			byte.toString(16).padStart(2, '0')
		).join('');
		return database.transaction(async (tx) => {
			// Hash collisions only serialize unrelated work; the primary key remains exact.
			await database.query(
				tx,
				'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))',
				[JSON.stringify([operation.scope, operation.operationId])]
			);
			const [receipt] = await database.query(
				tx,
				'SELECT fingerprint, result FROM absolute_sync_mutation_receipts WHERE scope = $1 AND operation_id = $2',
				[operation.scope, operation.operationId]
			);
			if (receipt) {
				if (receipt.fingerprint !== fingerprint)
					throw new Error(
						'Mutation operation id was already used with different arguments'
					);
				const envelope = (
					typeof receipt.result === 'string'
						? JSON.parse(receipt.result)
						: receipt.result
				) as { value?: R };
				return { result: envelope.value as R, replayed: true };
			}
			const result = await execute(tx);
			// Serialize before committing so unsupported results roll back the writes.
			const envelope =
				result === undefined ? '{}' : canonical({ value: result });
			await database.query(
				tx,
				'INSERT INTO absolute_sync_mutation_receipts (scope, operation_id, fingerprint, result) VALUES ($1, $2, $3, $4::jsonb)',
				[operation.scope, operation.operationId, fingerprint, envelope]
			);
			return { result, replayed: false };
		});
	};
