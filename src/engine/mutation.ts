import type { CollectionContext } from './collection';
import type { RowChange } from './types';

/**
 * Actions a mutation handler uses to publish what it changed. Call `change`
 * after each durable write so live views update and subscribers (including the
 * caller) receive the authoritative diff.
 */
export type MutationActions = {
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
