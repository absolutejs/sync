/**
 * Sync packs — Convex Components without the lock-in. A pack bundles
 * schemas + collections + mutations + scheduled + permissions + readers/
 * writers + CRDT field declarations into one npm-distributable module
 * registered with a single {@link SyncEngine.registerPack} call.
 *
 * See `syncPacks.design.md` for the rationale and the worked examples.
 *
 * Pack design rules (enforced at register time):
 *
 * - A pack declares which tables it `ownsTables`. Two registered packs
 *   cannot claim the same table; the host app's directly-registered
 *   tables are NOT counted (host registrations always win).
 * - Name construction (namespacing) is the pack's job — each published
 *   pack ships as a factory `create<Name>Pack(config)` that builds names
 *   from a `tablePrefix` and the host's user/auth context.
 * - Packs compose via the subscription layer (read each other's
 *   collections), never via cross-pack `runMutation` calls.
 */

import type {
	CollectionContext,
	CollectionDefinition,
	JoinCollectionDefinition
} from './collection';
import type { GraphCollectionDefinition } from './graph';
import type { MutationDefinition, TableWriter } from './mutation';
import type { PermissionsDefinition, TablePermissions } from './permissions';
import type { ReactiveQueryDefinition, TableReader } from './reactive';
import type { ScheduleDefinition } from './schedule';
import type { SchemaDefinition, TableSchema } from './schema';
import type { SearchCollectionDefinition } from './search';
import type { CrdtMergeable } from '../crdt';

/**
 * Same shape as the engine's per-table CRDT field map (see
 * {@link SyncEngine.registerCrdt}). A pack declares CRDT field types
 * here; the engine wires them on registration.
 */
export type CrdtFieldsMap = Record<
	string,
	Record<string, CrdtMergeable<unknown>>
>;

/**
 * A pack — a self-contained bundle of every registration the engine
 * already accepts. The engine's `registerPack(pack)` dispatches each
 * field to the matching `engine.register*` method. There is no new
 * persistence path; packs are pure composition.
 */
export type SyncPack = {
	/**
	 * Pack identifier. Used for devtools labelling and conflict
	 * diagnostics. Should match the npm package name (e.g.
	 * "@absolutejs/sync-pack-presence").
	 */
	name: string;
	/**
	 * Pack semver. Surfaced in {@link EngineInspection.packs} and in
	 * conflict diagnostics (e.g. "table 'comments' is owned by
	 * sync-pack-comments@2.1.0").
	 */
	version: string;
	/**
	 * Tables this pack OWNS. The engine rejects another pack that also
	 * claims one of these. Direct host registrations (e.g.
	 * `engine.registerSchema("foo", ...)`) are NOT tracked as ownership,
	 * so the host can still extend a pack's table or override its
	 * schema/permissions.
	 */
	ownsTables: string[];
	/**
	 * Tables this pack reads but does NOT own (e.g. a comments pack
	 * reads the host's `users` table for author info). Reported in
	 * {@link EngineInspection.packs}; not enforced unless
	 * {@link requireDependencies} is `true`.
	 */
	readsTables?: string[];
	/**
	 * When `true`, the engine throws {@link PackMissingDependencyError}
	 * at register time if any table in `readsTables` has no registered
	 * reader. Default `false` — host-app reads can be wired lazily.
	 */
	requireDependencies?: boolean;

	schemas?: SchemaDefinition;
	permissions?: PermissionsDefinition<any>;
	readers?: Record<string, TableReader<any>>;
	writers?: Record<string, TableWriter<any, any, any>>;
	crdt?: CrdtFieldsMap;

	// Generic params use `any` (not `unknown`) on purpose: TypeScript's
	// function-parameter contravariance would reject a
	// `MutationDefinition<{id: string}, ...>` if the slot were
	// `MutationDefinition<unknown, ...>`. `any` keeps the slot permissive
	// while each `defineMutation` call retains its inferred shape. Same
	// shape as the engine's internal maps.
	collections?: CollectionDefinition<any, any, any>[];
	joinCollections?: JoinCollectionDefinition<any, any, any, any, any>[];
	graphCollections?: GraphCollectionDefinition<any, any, any>[];
	searchCollections?: SearchCollectionDefinition<any, any, any>[];
	reactiveQueries?: ReactiveQueryDefinition<any, any, any>[];

	mutations?: MutationDefinition<any, any, any>[];
	schedules?: ScheduleDefinition[];
};

/**
 * Pack metadata stored on the engine and surfaced via
 * {@link EngineInspection.packs}.
 */
export type RegisteredPack = {
	name: string;
	version: string;
	ownsTables: string[];
	readsTables: string[];
};

/**
 * Thrown by {@link SyncEngine.registerPack} when a pack claims a table
 * that another registered pack already owns. The message names both
 * packs and the colliding table so the operator can pick a
 * `tablePrefix`.
 */
export class PackTableConflictError extends Error {
	readonly table: string;
	readonly existingPack: string;
	readonly newPack: string;
	constructor(table: string, existingPack: string, newPack: string) {
		super(
			`Pack "${newPack}" claims table "${table}", but "${existingPack}" already owns it. Use a tablePrefix on one of them.`
		);
		this.name = 'PackTableConflictError';
		this.table = table;
		this.existingPack = existingPack;
		this.newPack = newPack;
	}
}

/**
 * Thrown by {@link SyncEngine.registerPack} when a pack has
 * `requireDependencies: true` and at least one table in
 * {@link SyncPack.readsTables} has no registered reader at register
 * time. Pack authors opt into this when their pack cannot function
 * without the host having wired the dependency up front.
 */
export class PackMissingDependencyError extends Error {
	readonly pack: string;
	readonly missingTable: string;
	constructor(pack: string, missingTable: string) {
		super(
			`Pack "${pack}" requires a reader for table "${missingTable}" but none is registered. Call engine.registerReader("${missingTable}", ...) before engine.registerPack.`
		);
		this.name = 'PackMissingDependencyError';
		this.pack = pack;
		this.missingTable = missingTable;
	}
}

/**
 * Identity helper. A pack is plain data — the helper exists for type
 * inference, not for runtime behavior.
 *
 * @example
 * export const createPresencePack = (config: PresencePackConfig) =>
 *   defineSyncPack({
 *     name: '@absolutejs/sync-pack-presence',
 *     version: '0.1.0',
 *     ownsTables: [resolveTableName('presence', config.tablePrefix)],
 *     schemas: { ... },
 *     collections: [ ... ],
 *     mutations: [ ... ],
 *     schedules: [ ... ],
 *   });
 */
export const defineSyncPack = (pack: SyncPack): SyncPack => pack;
