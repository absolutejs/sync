import type { CollectionContext } from './collection';

/**
 * Declarative, row-level access control keyed by table — the engine enforces it,
 * so a rule lives in one place instead of being restated across a collection's
 * `authorize` (gate), `hydrate` (DB filter), and `match` (incremental filter).
 * This is the BYO-database analogue of Convex/Zero permissions: plain predicate
 * functions over `(ctx, row)`, applied uniformly to reads and writes.
 */

/**
 * A row-level read rule: may `ctx` see `row`? Return `true` to allow. The engine
 * applies it to every row it would emit for the table — the initial snapshot, an
 * incremental diff, a catch-up diff, and the one-shot hydrate — and to the reads
 * a reactive query makes through `ctx.db`. So a row a caller can't see never
 * reaches them, even if the collection's `hydrate`/`match` are too loose.
 */
export type ReadRule<Row = unknown, Ctx = CollectionContext> = (
	ctx: Ctx,
	row: Row
) => boolean;

/**
 * A row-level write rule: may `ctx` perform this write? Return `true` to allow; a
 * `false` rejects the mutation with `UnauthorizedError`, rolling back its
 * transaction. For `insert` the rule sees the row being created. For
 * `update`/`delete` it sees the *existing* row when the table has a reader with
 * `get` (so the check can't be spoofed by the client payload), otherwise the row
 * passed to the action.
 */
export type WriteRule<Row = unknown, Ctx = CollectionContext> = (
	ctx: Ctx,
	row: Row
) => boolean;

/** Declarative permissions for one table's rows. An omitted rule allows. */
export type TablePermissions<Row = unknown, Ctx = CollectionContext> = {
	/** Who may read a row — filters every row the engine emits for this table. */
	read?: ReadRule<Row, Ctx>;
	/** Who may insert a row. Falls back to {@link TablePermissions.write}. */
	insert?: WriteRule<Row, Ctx>;
	/** Who may update a row. Falls back to {@link TablePermissions.write}. */
	update?: WriteRule<Row, Ctx>;
	/** Who may delete a row. Falls back to {@link TablePermissions.write}. */
	delete?: WriteRule<Row, Ctx>;
	/** Default write rule for any of insert/update/delete without a specific one. */
	write?: WriteRule<Row, Ctx>;
};

/** A `table` → {@link TablePermissions} map. */
export type PermissionsDefinition<Ctx = CollectionContext> = Record<
	string,
	// `any` row type per table: the map is heterogeneous, like the engine registry.
	TablePermissions<any, Ctx>
>;

/**
 * Define declarative, row-level permissions keyed by table. Identity at runtime —
 * it exists for type inference. Pass to `createSyncEngine({ permissions })` or
 * register incrementally with `engine.registerPermissions(table, rules)`.
 *
 * Scope: read rules are enforced for single-table `view` collections and for the
 * reads a reactive query makes through `ctx.db`; join/graph collections scope via
 * their own `hydrate`/`match`. Write rules are enforced in
 * `actions.insert/update/delete` (not the low-level `actions.change`).
 */
export const definePermissions = <Ctx = CollectionContext>(
	permissions: PermissionsDefinition<Ctx>
): PermissionsDefinition<Ctx> => permissions;
