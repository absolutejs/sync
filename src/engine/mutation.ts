import type { CollectionContext } from './collection';
import type { RowChange } from './types';

/**
 * How to persist a table — register one with {@link SyncEngine.registerWriter} so
 * `insert`/`update`/`delete` on the mutation actions write to your store (any
 * ORM) and emit the live change in one step. Each function returns the stored
 * row so the emitted diff carries DB-generated fields (ids, timestamps).
 */
export type TableWriter<Row = any, Ctx = unknown> = {
	insert: (data: any, ctx: Ctx) => Promise<Row> | Row;
	update: (data: any, ctx: Ctx) => Promise<Row> | Row;
	/** Persist the delete; receives the row/identifier passed to `actions.delete`. */
	delete: (row: any, ctx: Ctx) => Promise<unknown> | unknown;
};

/**
 * Actions a mutation handler uses to write and publish changes.
 *
 * Prefer `insert`/`update`/`delete`: they persist via the table's registered
 * {@link TableWriter} and emit the live change in one fused call, so you can't
 * forget to go live (and the change always reflects the stored row). `change` is
 * the lower-level escape hatch for when you wrote some other way.
 */
export type MutationActions = {
	/** Persist a new row to `table` and emit it. Returns the stored row. */
	insert: <Row = unknown>(table: string, data: unknown) => Promise<Row>;
	/** Persist an update to `table` and emit it. Returns the stored row. */
	update: <Row = unknown>(table: string, data: unknown) => Promise<Row>;
	/** Persist a delete to `table` (pass the row or its key) and emit it. */
	delete: (table: string, row: unknown) => Promise<void>;
	/** Escape hatch: emit a change you persisted yourself (no writer call). */
	change: <T>(collection: string, change: RowChange<T>) => Promise<void>;
};

export type MutationHandler<Args, Ctx, Result> = (
	args: Args,
	ctx: Ctx,
	actions: MutationActions
) => Promise<Result> | Result;

export type MutationDefinition<
	Args = unknown,
	Ctx = CollectionContext,
	Result = unknown
> = {
	/** Mutation name the client invokes. */
	name: string;
	/** Access control: return false (or throw) to reject the mutation. */
	authorize?: (args: Args, ctx: Ctx) => boolean | Promise<boolean>;
	/**
	 * Apply the mutation: write to your durable store, then call
	 * `actions.change(...)` for each affected row. Return value (e.g. the created
	 * record) is sent back to the caller in the ack.
	 */
	handler: MutationHandler<Args, Ctx, Result>;
};

/**
 * Define a server mutation. Identity at runtime — it exists for type inference
 * (args/ctx/result flow through). Register it with {@link SyncEngine.registerMutation}
 * and invoke it from the client; the engine authorizes, runs the handler, fans
 * the resulting diffs to subscribers, and acks the caller.
 */
export const defineMutation = <
	Args = unknown,
	Ctx = CollectionContext,
	Result = unknown
>(
	definition: MutationDefinition<Args, Ctx, Result>
): MutationDefinition<Args, Ctx, Result> => definition;
