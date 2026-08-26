import type { RowKey } from '../engine/types';
import type { SyncMutationRejection } from '../reconciliation';

/** Serializable optimistic effect that can be replayed after process death. */
export type LocalOptimisticOperation =
	| { type: 'insert' | 'update'; collection: string; row: unknown }
	| { type: 'delete'; collection: string; key: RowKey };

/** Durable outbound operation. Functions are deliberately excluded. */
export type LocalMutationRecord = {
	/** Stable for the lifetime of this operation, including every retry. */
	operationId: string;
	/** Local collection key whose handle initiated the mutation. */
	owner?: string;
	name: string;
	args: unknown;
	optimistic: LocalOptimisticOperation[];
	/** Inverse effects captured before optimism, used for deterministic rollback. */
	inverse: LocalOptimisticOperation[];
	createdAt: number;
	attempts: number;
	lastError?: string;
	/** Missing means pending for compatibility with records written before 2.22. */
	state?: 'dead-letter' | 'pending';
	/** Earliest wall-clock time at which an automatic retry may be sent. */
	nextAttemptAt?: number;
	/** Typed server outcome retained for remediation and diagnostics. */
	rejection?: SyncMutationRejection;
	deadLetteredAt?: number;
	/** Conflict behavior captured when the intent entered the outbox. */
	conflictPolicy?: SyncLocalConflictPolicy;
	/** Automatic conflict retries already attempted for this unchanged intent. */
	conflictAttempts?: number;
	/** Previous intent replaced by an explicit argument-changing rebase. */
	supersedesOperationId?: string;
};

/** Server-authoritative collection state saved for offline reads and resume. */
export type LocalCollectionRecord<T = unknown> = {
	rows: T[];
	version: number;
	/** Last durable write, used for whole-snapshot retention and eviction. */
	storedAt?: number;
	/** Opaque cross-instance cursor; round-trip without inspecting it. */
	cursor?: string;
	/** Durable descriptor used by a separate background process. */
	collection?: string;
	params?: unknown;
	/** Resume diffs are safe only when the background process knows the row key. */
	headlessKey?: 'id';
};

export type SyncLocalStoreMode = 'readonly' | 'readwrite';

export type SyncLocalMigrationContext = {
	/** Principal partition that owns the record. Never changes during migration. */
	namespace: string;
	/** Collection key or stable mutation operation id. */
	key: string;
};

export type SyncLocalMigrationResult<T> = T | null | undefined | void;
export type SyncLocalJsonValue =
	| boolean
	| null
	| number
	| string
	| SyncLocalJsonValue[]
	| { [key: string]: SyncLocalJsonValue };

/** JSON-safe operations that package metadata and service workers can carry. */
export type SyncLocalCollectionMigrationOperation =
	| { collection: string; type: 'delete-collection' }
	| {
			collection: string;
			field: string;
			type: 'remove-field';
	  }
	| {
			collection: string;
			field: string;
			type: 'set-default';
			value: SyncLocalJsonValue;
	  }
	| {
			collection: string;
			from: string;
			to: string;
			type: 'rename-field';
	  };

/**
 * One synchronous, deterministic local-data upgrade. A returned value replaces
 * the record, `null` deletes it, and `undefined` leaves it unchanged. Keeping
 * transforms synchronous lets IndexedDB and SQLite apply the same plan inside
 * one native transaction without an unsafe gap where the host may suspend.
 */
export type SyncLocalStoreMigration = {
	/** Version produced by this step. Steps must be contiguous. */
	toVersion: number;
	/** Serializable operations generated from application/pack metadata. */
	operations?: readonly SyncLocalCollectionMigrationOperation[];
	migrateCollection?: (
		record: LocalCollectionRecord,
		context: SyncLocalMigrationContext
	) => SyncLocalMigrationResult<LocalCollectionRecord>;
	migrateMutation?: (
		record: LocalMutationRecord,
		context: SyncLocalMigrationContext
	) => SyncLocalMigrationResult<LocalMutationRecord>;
};

/** Versioned logical schema shared by web IndexedDB and native SQLite. */
export type SyncLocalStoreSchema = {
	/** Desired schema version. Version 1 is the legacy durable-store shape. */
	version: number;
	/** Oldest on-device version this build can upgrade. */
	minimumCompatibleVersion?: number;
	migrations?: readonly SyncLocalStoreMigration[];
};

export type SyncLocalStoreSchemaComponent = SyncLocalStoreSchema & {
	/** Stable package/application identity, normally an npm package name. */
	id: string;
	/** Declarative persistence rules contributed by this component. */
	localData?: SyncLocalDataPolicy;
};

export type SyncLocalProtection = 'none' | 'required';
export type SyncLocalPersistence = 'durable' | 'memory-only';
export type SyncLocalEvictionPriority = 'critical' | 'normal' | 'disposable';
export type SyncLocalConflictStrategy =
	| 'client-wins'
	| 'manual'
	| 'server-wins';

/** JSON-safe conflict behavior that can run in foreground or headless hosts. */
export type SyncLocalConflictPolicy = {
	strategy: SyncLocalConflictStrategy;
	/** Automatic retries of an unchanged client-wins intent. Defaults to one. */
	maxAttempts?: number;
};

export type SyncLocalCollectionPolicy = {
	/** Exact collection name, or one trailing `*` prefix wildcard. */
	match: string;
	sensitivity?: 'public' | 'private' | 'secret';
	persistence?: SyncLocalPersistence;
	protection?: SyncLocalProtection;
	/** Fail closed without a protector; optionally degrade this cache to memory-only. */
	onProtectionUnavailable?: 'error' | 'memory-only';
	/** Evict the complete cached projection after this age; never truncate rows. */
	maxAgeMs?: number;
	evictionPriority?: SyncLocalEvictionPriority;
};

export type SyncLocalMutationPolicy = {
	/** Exact mutation name, or one trailing `*` prefix wildcard. */
	match: string;
	sensitivity?: 'public' | 'private' | 'secret';
	persistence?: SyncLocalPersistence;
	protection?: SyncLocalProtection;
	/** Fail closed without a protector; optionally keep the live mutation memory-only. */
	onProtectionUnavailable?: 'error' | 'memory-only';
	/** Manual by default. Automatic strategies must be explicitly declared. */
	conflict?: SyncLocalConflictPolicy;
};

export type SyncLocalDataPolicy = {
	/** Logical JSON-byte ceiling per principal. Pending mutations are never evicted. */
	maxBytesPerNamespace?: number;
	collections?: readonly SyncLocalCollectionPolicy[];
	mutations?: readonly SyncLocalMutationPolicy[];
};

export type SyncLocalRecordProtectionContext = {
	kind: 'collection' | 'mutation';
	name: string;
	namespace: string;
};

/** Prepared synchronous codec so an IndexedDB transaction cannot auto-close. */
export type SyncLocalRecordProtector = {
	id: string;
	open(value: string, context: SyncLocalRecordProtectionContext): string;
	seal(value: string, context: SyncLocalRecordProtectionContext): string;
};

export type SyncLocalProtectionProvider = {
	prepare(): Promise<SyncLocalRecordProtector>;
};

export class SyncLocalDataPolicyError extends Error {
	readonly code: 'INVALID_POLICY' | 'PROTECTION_REQUIRED' | 'QUOTA_EXCEEDED';

	constructor(code: SyncLocalDataPolicyError['code'], message: string) {
		super(message);
		this.name = 'SyncLocalDataPolicyError';
		this.code = code;
	}
}

export type SyncLocalStoreSchemaBundle = {
	components: readonly SyncLocalStoreSchemaComponent[];
};

export type SyncLocalStoreSchemaInput =
	| SyncLocalStoreSchema
	| SyncLocalStoreSchemaBundle;

export type SyncLocalStoreComponentStatus = {
	id: string;
	minimumCompatibleVersion: number;
	storedVersion: number;
	targetVersion: number;
};

export type SyncLocalStoreSchemaStatus = {
	storedVersion: number;
	targetVersion: number;
	minimumCompatibleVersion: number;
	state: 'ready';
	components?: SyncLocalStoreComponentStatus[];
	orphanedComponents?: string[];
};

export class SyncLocalStoreSchemaError extends Error {
	readonly code:
		| 'INVALID_PLAN'
		| 'MIGRATION_MISSING'
		| 'SCHEMA_TOO_NEW'
		| 'SCHEMA_TOO_OLD';
	readonly storedVersion?: number;
	readonly targetVersion?: number;

	constructor(
		code: SyncLocalStoreSchemaError['code'],
		message: string,
		versions: { storedVersion?: number; targetVersion?: number } = {}
	) {
		super(message);
		this.name = 'SyncLocalStoreSchemaError';
		this.code = code;
		this.storedVersion = versions.storedVersion;
		this.targetVersion = versions.targetVersion;
	}
}

const positiveVersion = (value: number, label: string): number => {
	if (!Number.isSafeInteger(value) || value < 1)
		throw new SyncLocalStoreSchemaError(
			'INVALID_PLAN',
			`${label} must be a positive safe integer`
		);
	return value;
};

const isSchemaBundle = (
	schema: SyncLocalStoreSchemaInput
): schema is SyncLocalStoreSchemaBundle => 'components' in schema;

const validatePolicyMatch = (match: string, label: string) => {
	if (
		match.length === 0 ||
		match.trim() !== match ||
		/^\*+$/.test(match) ||
		match.includes('**')
	)
		throw new SyncLocalDataPolicyError(
			'INVALID_POLICY',
			`${label}.match must be an exact name or a non-empty glob without adjacent wildcards.`
		);
};

export const validateSyncLocalDataPolicy = (
	policy: SyncLocalDataPolicy,
	label = 'localData'
): SyncLocalDataPolicy => {
	if (
		policy.maxBytesPerNamespace !== undefined &&
		(!Number.isSafeInteger(policy.maxBytesPerNamespace) ||
			policy.maxBytesPerNamespace < 1)
	)
		throw new SyncLocalDataPolicyError(
			'INVALID_POLICY',
			`${label}.maxBytesPerNamespace must be a positive safe integer.`
		);
	for (const [index, rule] of (policy.collections ?? []).entries()) {
		validatePolicyMatch(rule.match, `${label}.collections[${index}]`);
		if (
			rule.maxAgeMs !== undefined &&
			(!Number.isSafeInteger(rule.maxAgeMs) || rule.maxAgeMs < 1)
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.collections[${index}].maxAgeMs must be a positive safe integer.`
			);
		if (
			rule.persistence === 'memory-only' &&
			rule.protection === 'required'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.collections[${index}] cannot require at-rest protection when it is memory-only.`
			);
		if (
			rule.sensitivity !== undefined &&
			rule.sensitivity !== 'public' &&
			rule.protection !== 'required' &&
			rule.persistence !== 'memory-only'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.collections[${index}] declares ${rule.sensitivity} data without required protection or memory-only persistence.`
			);
	}
	for (const [index, rule] of (policy.mutations ?? []).entries()) {
		validatePolicyMatch(rule.match, `${label}.mutations[${index}]`);
		if (
			rule.conflict !== undefined &&
			rule.conflict.strategy !== 'client-wins' &&
			rule.conflict.strategy !== 'manual' &&
			rule.conflict.strategy !== 'server-wins'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.mutations[${index}].conflict.strategy is invalid.`
			);
		if (
			rule.conflict?.maxAttempts !== undefined &&
			(!Number.isSafeInteger(rule.conflict.maxAttempts) ||
				rule.conflict.maxAttempts < 1)
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.mutations[${index}].conflict.maxAttempts must be a positive safe integer.`
			);
		if (
			rule.conflict?.maxAttempts !== undefined &&
			rule.conflict.strategy !== 'client-wins'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.mutations[${index}].conflict.maxAttempts is only valid for client-wins.`
			);
		if (
			rule.persistence === 'memory-only' &&
			rule.protection === 'required'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.mutations[${index}] cannot require at-rest protection when it is memory-only.`
			);
		if (
			rule.sensitivity !== undefined &&
			rule.sensitivity !== 'public' &&
			rule.protection !== 'required' &&
			rule.persistence !== 'memory-only'
		)
			throw new SyncLocalDataPolicyError(
				'INVALID_POLICY',
				`${label}.mutations[${index}] declares ${rule.sensitivity} arguments without required protection.`
			);
	}
	return policy;
};

export type ResolvedSyncLocalDataPolicy = {
	maxBytesPerNamespace?: number;
	collections: readonly SyncLocalCollectionPolicy[];
	mutations: readonly SyncLocalMutationPolicy[];
};

export const resolveSyncLocalDataPolicy = (
	schema: SyncLocalStoreSchemaInput = { version: 1 }
): ResolvedSyncLocalDataPolicy => {
	const policies = normalizeSyncLocalSchemaComponents(schema)
		.map((component) =>
			component.localData
				? validateSyncLocalDataPolicy(
						component.localData,
						`${component.id}.localData`
					)
				: undefined
		)
		.filter(
			(policy): policy is SyncLocalDataPolicy => policy !== undefined
		);
	const quotas = policies
		.map((policy) => policy.maxBytesPerNamespace)
		.filter((value): value is number => value !== undefined);
	return {
		...(quotas.length > 0
			? { maxBytesPerNamespace: Math.min(...quotas) }
			: {}),
		collections: policies.flatMap((policy) => policy.collections ?? []),
		mutations: policies.flatMap((policy) => policy.mutations ?? [])
	};
};

const policyMatches = (pattern: string, name: string) =>
	pattern === name ||
	(() => {
		const pieces = pattern.split('*');
		if (pieces.length === 1) return false;
		let offset = 0;
		for (const [index, piece] of pieces.entries()) {
			if (piece.length === 0) continue;
			const found = name.indexOf(piece, offset);
			if (found < 0 || (index === 0 && found !== 0)) return false;
			offset = found + piece.length;
		}
		const last = pieces.at(-1)!;
		return last.length === 0 || name.endsWith(last);
	})();

const matchingPolicy = <T extends { match: string }>(
	rules: readonly T[],
	name: string
): T | undefined => {
	const matches = rules.filter((rule) => policyMatches(rule.match, name));
	if (matches.length === 0) return undefined;
	const specificity = (rule: T) => rule.match.replaceAll('*', '').length;
	matches.sort((left, right) => specificity(right) - specificity(left));
	if (
		matches[1] !== undefined &&
		specificity(matches[0]!) === specificity(matches[1]!)
	)
		throw new SyncLocalDataPolicyError(
			'INVALID_POLICY',
			`Local-data policy has equally specific rules for "${name}".`
		);
	return matches[0];
};

export const resolveSyncLocalCollectionPolicy = (
	policy: ResolvedSyncLocalDataPolicy,
	name: string
): SyncLocalCollectionPolicy =>
	matchingPolicy(policy.collections, name) ?? { match: name };

export const resolveSyncLocalMutationPolicy = (
	policy: ResolvedSyncLocalDataPolicy,
	name: string
): SyncLocalMutationPolicy =>
	matchingPolicy(policy.mutations, name) ?? { match: name };

export const normalizeSyncLocalSchemaComponents = (
	schema: SyncLocalStoreSchemaInput = { version: 1 }
): SyncLocalStoreSchemaComponent[] => {
	const components = isSchemaBundle(schema)
		? [...schema.components]
		: [{ ...schema, id: '@absolutejs/app' }];
	const ids = new Set<string>();
	for (const component of components) {
		if (
			typeof component.id !== 'string' ||
			component.id.trim() !== component.id ||
			component.id.length === 0
		)
			throw new SyncLocalStoreSchemaError(
				'INVALID_PLAN',
				'Sync schema component id must be non-empty and trimmed'
			);
		if (ids.has(component.id))
			throw new SyncLocalStoreSchemaError(
				'INVALID_PLAN',
				`Sync schema component "${component.id}" is declared more than once`
			);
		ids.add(component.id);
		if (component.localData)
			validateSyncLocalDataPolicy(
				component.localData,
				`${component.id}.localData`
			);
	}
	return components.sort((a, b) => a.id.localeCompare(b.id));
};

export type ResolvedSyncLocalSchemaComponent = {
	id: string;
	minimumCompatibleVersion: number;
	steps: readonly SyncLocalStoreMigration[];
	targetVersion: number;
};

export const createSyncLocalSchemaStatus = (
	components: readonly ResolvedSyncLocalSchemaComponent[],
	orphanedComponents: readonly string[] = [],
	includeComponents = true
): SyncLocalStoreSchemaStatus => {
	const app = components.find(
		(component) => component.id === '@absolutejs/app'
	);
	return {
		...(includeComponents
			? {
					components: components.map((component) => ({
						id: component.id,
						minimumCompatibleVersion:
							component.minimumCompatibleVersion,
						storedVersion: component.targetVersion,
						targetVersion: component.targetVersion
					}))
				}
			: {}),
		minimumCompatibleVersion: app?.minimumCompatibleVersion ?? 1,
		...(orphanedComponents.length > 0
			? { orphanedComponents: [...orphanedComponents] }
			: {}),
		state: 'ready',
		storedVersion: app?.targetVersion ?? 1,
		targetVersion: app?.targetVersion ?? 1
	};
};

export const resolveSyncLocalSchemaComponents = (
	storedVersions: Readonly<Record<string, number>>,
	schema: SyncLocalStoreSchemaInput = { version: 1 }
): {
	components: ResolvedSyncLocalSchemaComponent[];
	orphanedComponents: string[];
} => {
	const components = normalizeSyncLocalSchemaComponents(schema).map(
		(component) => {
			const current = resolveSyncLocalMigrations(
				component.version,
				component
			);

			return {
				id: component.id,
				...resolveSyncLocalMigrations(
					storedVersions[component.id] ??
						current.minimumCompatibleVersion,
					component
				)
			};
		}
	);
	const active = new Set(components.map((component) => component.id));
	const orphanedComponents = Object.keys(storedVersions)
		.filter((id) => !active.has(id))
		.sort();
	return { components, orphanedComponents };
};

const migrationCollectionName = (
	record: LocalCollectionRecord,
	context: SyncLocalMigrationContext
) => record.collection ?? context.key;

const migrateRow = (
	row: unknown,
	operation: Exclude<
		SyncLocalCollectionMigrationOperation,
		{ type: 'delete-collection' }
	>
) => {
	if (typeof row !== 'object' || row === null || Array.isArray(row))
		throw new SyncLocalStoreSchemaError(
			'INVALID_PLAN',
			`Sync ${operation.type} requires object rows`
		);
	const next = { ...row } as Record<string, unknown>;
	if (operation.type === 'set-default') {
		if (!Object.hasOwn(next, operation.field))
			next[operation.field] = structuredClone(operation.value);
	} else if (operation.type === 'remove-field') {
		delete next[operation.field];
	} else if (Object.hasOwn(next, operation.from)) {
		if (Object.hasOwn(next, operation.to))
			throw new SyncLocalStoreSchemaError(
				'INVALID_PLAN',
				`Sync rename-field cannot overwrite existing field "${operation.to}"`
			);
		next[operation.to] = next[operation.from];
		delete next[operation.from];
	}
	return next;
};

const applyDeclarativeCollectionOperations = (
	record: LocalCollectionRecord,
	context: SyncLocalMigrationContext,
	operations: readonly SyncLocalCollectionMigrationOperation[]
): LocalCollectionRecord | null => {
	let current = structuredClone(record);
	for (const operation of operations) {
		if (operation.collection !== migrationCollectionName(current, context))
			continue;
		if (operation.type === 'delete-collection') return null;
		current = {
			...current,
			rows: current.rows.map((row) => migrateRow(row, operation))
		};
	}
	return current;
};

/** Validate a plan and return the exact ordered upgrade path. */
export const resolveSyncLocalMigrations = (
	storedVersion: number,
	schema: SyncLocalStoreSchema = { version: 1 }
): {
	minimumCompatibleVersion: number;
	steps: readonly SyncLocalStoreMigration[];
	targetVersion: number;
} => {
	positiveVersion(storedVersion, 'Stored Sync schema version');
	const targetVersion = positiveVersion(
		schema.version,
		'Target Sync schema version'
	);
	const migrations = [...(schema.migrations ?? [])].sort(
		(a, b) => a.toVersion - b.toVersion
	);
	const versions = new Set<number>();
	for (const migration of migrations) {
		positiveVersion(migration.toVersion, 'Sync migration toVersion');
		if (versions.has(migration.toVersion))
			throw new SyncLocalStoreSchemaError(
				'INVALID_PLAN',
				`Sync migration ${migration.toVersion} is declared more than once`
			);
		versions.add(migration.toVersion);
	}
	const inferredMinimum = migrations[0]
		? migrations[0].toVersion - 1
		: targetVersion;
	const minimumCompatibleVersion = positiveVersion(
		schema.minimumCompatibleVersion ?? inferredMinimum,
		'Minimum compatible Sync schema version'
	);
	if (minimumCompatibleVersion > targetVersion)
		throw new SyncLocalStoreSchemaError(
			'INVALID_PLAN',
			'Minimum compatible Sync schema version cannot exceed its target'
		);
	if (storedVersion > targetVersion)
		throw new SyncLocalStoreSchemaError(
			'SCHEMA_TOO_NEW',
			`Stored Sync schema ${storedVersion} is newer than this runtime's schema ${targetVersion}`,
			{ storedVersion, targetVersion }
		);
	if (storedVersion < minimumCompatibleVersion)
		throw new SyncLocalStoreSchemaError(
			'SCHEMA_TOO_OLD',
			`Stored Sync schema ${storedVersion} is older than the minimum compatible schema ${minimumCompatibleVersion}`,
			{ storedVersion, targetVersion }
		);

	const steps: SyncLocalStoreMigration[] = [];
	for (let version = storedVersion + 1; version <= targetVersion; version++) {
		const migration = migrations.find(
			(candidate) => candidate.toVersion === version
		);
		if (migration === undefined)
			throw new SyncLocalStoreSchemaError(
				'MIGRATION_MISSING',
				`Sync migration ${version - 1} -> ${version} is missing`,
				{ storedVersion, targetVersion }
			);
		steps.push(migration);
	}
	return { minimumCompatibleVersion, steps, targetVersion };
};

export const migrateSyncLocalCollectionRecord = (
	record: LocalCollectionRecord,
	context: SyncLocalMigrationContext,
	steps: readonly SyncLocalStoreMigration[]
): LocalCollectionRecord | null => {
	let current: LocalCollectionRecord | null = structuredClone(record);
	for (const step of steps) {
		if (current === null) break;
		current = applyDeclarativeCollectionOperations(
			current,
			context,
			step.operations ?? []
		);
		if (current === null) break;
		const next: SyncLocalMigrationResult<LocalCollectionRecord> =
			step.migrateCollection?.(structuredClone(current), context);
		if (next === null) current = null;
		else if (next !== undefined) current = structuredClone(next);
	}
	return current;
};

export const migrateSyncLocalMutationRecord = (
	record: LocalMutationRecord,
	context: SyncLocalMigrationContext,
	steps: readonly SyncLocalStoreMigration[]
): LocalMutationRecord | null => {
	let current: LocalMutationRecord | null = structuredClone(record);
	for (const step of steps) {
		if (current === null) break;
		const next: SyncLocalMigrationResult<LocalMutationRecord> =
			step.migrateMutation?.(structuredClone(current), context);
		if (next === null) current = null;
		else if (next !== undefined) {
			if (next.operationId !== context.key)
				throw new SyncLocalStoreSchemaError(
					'INVALID_PLAN',
					'Sync migrations cannot change a mutation operationId'
				);
			current = structuredClone(next);
		}
	}
	return current;
};

/**
 * One atomic view of an account/tenant namespace.
 *
 * Adapters must make every method participate in the enclosing transaction.
 * A callback throw aborts all writes, including collection state and queue
 * changes, so a crash cannot leave the two halves out of sync.
 */
export type SyncLocalTransaction = {
	/** Effective generated rule, when the adapter was created with a policy bundle. */
	resolveMutationPolicy?: (name: string) => SyncLocalMutationPolicy;
	getInstallationId: () => Promise<string | undefined>;
	setInstallationId: (installationId: string) => Promise<void>;
	getCollection: <T = unknown>(
		key: string
	) => Promise<LocalCollectionRecord<T> | undefined>;
	/** Enumerate this principal's durable collection descriptors for a
	 * service-worker/native runner. Rows never cross namespace boundaries. */
	listCollections: () => Promise<
		Array<{ key: string; record: LocalCollectionRecord }>
	>;
	putCollection: <T = unknown>(
		key: string,
		record: LocalCollectionRecord<T>
	) => Promise<void>;
	deleteCollection: (key: string) => Promise<void>;
	listMutations: () => Promise<LocalMutationRecord[]>;
	getMutation: (
		operationId: string
	) => Promise<LocalMutationRecord | undefined>;
	putMutation: (record: LocalMutationRecord) => Promise<void>;
	deleteMutation: (operationId: string) => Promise<void>;
};

const jsonBytes = (value: unknown) =>
	new TextEncoder().encode(JSON.stringify(value)).byteLength;
const evictionRank: Record<SyncLocalEvictionPriority, number> = {
	disposable: 0,
	normal: 1,
	critical: 2
};

export const runSyncLocalPolicyTransaction = async <R>(options: {
	mode: SyncLocalStoreMode;
	now: number;
	policy: ResolvedSyncLocalDataPolicy;
	protected: boolean;
	raw: SyncLocalTransaction;
	run: (tx: SyncLocalTransaction) => Promise<R>;
}): Promise<R> => {
	const { mode, now, policy, raw } = options;
	const requireProtection = (kind: string, name: string) => {
		if (!options.protected)
			throw new SyncLocalDataPolicyError(
				'PROTECTION_REQUIRED',
				`Sync ${kind} "${name}" requires encrypted persistence, but this runtime has no audited protection provider.`
			);
	};
	const collectionPolicy = (record: LocalCollectionRecord, key: string) =>
		resolveSyncLocalCollectionPolicy(policy, record.collection ?? key);
	const unavailableMemoryOnly = (
		rule: SyncLocalCollectionPolicy | SyncLocalMutationPolicy
	) =>
		rule.protection === 'required' &&
		!options.protected &&
		rule.onProtectionUnavailable === 'memory-only';
	const expired = (record: LocalCollectionRecord, key: string) => {
		const rule = collectionPolicy(record, key);
		return (
			rule.maxAgeMs !== undefined &&
			record.storedAt !== undefined &&
			now - record.storedAt >= rule.maxAgeMs
		);
	};
	const tx: SyncLocalTransaction = {
		...raw,
		resolveMutationPolicy: (name) =>
			resolveSyncLocalMutationPolicy(policy, name),
		getCollection: async <T>(key: string) => {
			const record = await raw.getCollection<T>(key);
			if (record === undefined) return undefined;
			const rule = collectionPolicy(record, key);
			if (
				rule.persistence === 'memory-only' ||
				unavailableMemoryOnly(rule) ||
				expired(record, key)
			) {
				if (mode === 'readwrite') await raw.deleteCollection(key);
				return undefined;
			}
			if (rule.protection === 'required')
				requireProtection('collection', record.collection ?? key);
			return record;
		},
		listCollections: async () => {
			const kept: Array<{ key: string; record: LocalCollectionRecord }> =
				[];
			for (const entry of await raw.listCollections()) {
				const rule = collectionPolicy(entry.record, entry.key);
				if (
					rule.persistence === 'memory-only' ||
					unavailableMemoryOnly(rule) ||
					expired(entry.record, entry.key)
				) {
					if (mode === 'readwrite')
						await raw.deleteCollection(entry.key);
					continue;
				}
				if (rule.protection === 'required')
					requireProtection(
						'collection',
						entry.record.collection ?? entry.key
					);
				kept.push(entry);
			}
			return kept;
		},
		putCollection: async (key, record) => {
			const rule = collectionPolicy(record, key);
			if (
				rule.persistence === 'memory-only' ||
				unavailableMemoryOnly(rule)
			) {
				await raw.deleteCollection(key);
				return;
			}
			if (rule.protection === 'required')
				requireProtection('collection', record.collection ?? key);
			await raw.putCollection(
				key,
				rule.maxAgeMs !== undefined ||
					policy.maxBytesPerNamespace !== undefined
					? { ...record, storedAt: now }
					: record
			);
		},
		getMutation: async (operationId) => {
			const record = await raw.getMutation(operationId);
			if (!record) return undefined;
			const rule = resolveSyncLocalMutationPolicy(policy, record.name);
			if (
				rule.persistence === 'memory-only' ||
				unavailableMemoryOnly(rule)
			) {
				if (mode === 'readwrite') await raw.deleteMutation(operationId);
				return undefined;
			}
			if (rule.protection === 'required')
				requireProtection('mutation', record.name);
			return rule.conflict && record.conflictPolicy === undefined
				? { ...record, conflictPolicy: structuredClone(rule.conflict) }
				: record;
		},
		listMutations: async () => {
			const records: LocalMutationRecord[] = [];
			for (const record of await raw.listMutations()) {
				const rule = resolveSyncLocalMutationPolicy(
					policy,
					record.name
				);
				if (
					rule.persistence === 'memory-only' ||
					unavailableMemoryOnly(rule)
				) {
					if (mode === 'readwrite')
						await raw.deleteMutation(record.operationId);
					continue;
				}
				if (rule.protection === 'required')
					requireProtection('mutation', record.name);
				records.push(
					rule.conflict && record.conflictPolicy === undefined
						? {
								...record,
								conflictPolicy: structuredClone(rule.conflict)
							}
						: record
				);
			}
			return records;
		},
		putMutation: async (record) => {
			const rule = resolveSyncLocalMutationPolicy(policy, record.name);
			if (
				rule.persistence === 'memory-only' ||
				unavailableMemoryOnly(rule)
			) {
				await raw.deleteMutation(record.operationId);
				return;
			}
			if (rule.protection === 'required')
				requireProtection('mutation', record.name);
			await raw.putMutation(
				rule.conflict && record.conflictPolicy === undefined
					? {
							...record,
							conflictPolicy: structuredClone(rule.conflict)
						}
					: record
			);
		}
	};
	const result = await options.run(tx);
	if (mode === 'readwrite' && policy.maxBytesPerNamespace !== undefined) {
		const mutations = await raw.listMutations();
		let collections = await raw.listCollections();
		let bytes =
			mutations.reduce((total, value) => total + jsonBytes(value), 0) +
			collections.reduce(
				(total, entry) => total + jsonBytes(entry.record),
				0
			);
		const candidates = [...collections].sort((left, right) => {
			const leftRule = collectionPolicy(left.record, left.key);
			const rightRule = collectionPolicy(right.record, right.key);
			return (
				evictionRank[leftRule.evictionPriority ?? 'normal'] -
					evictionRank[rightRule.evictionPriority ?? 'normal'] ||
				(left.record.storedAt ?? 0) - (right.record.storedAt ?? 0) ||
				left.key.localeCompare(right.key)
			);
		});
		for (const candidate of candidates) {
			if (bytes <= policy.maxBytesPerNamespace) break;
			await raw.deleteCollection(candidate.key);
			bytes -= jsonBytes(candidate.record);
		}
		if (bytes > policy.maxBytesPerNamespace)
			throw new SyncLocalDataPolicyError(
				'QUOTA_EXCEEDED',
				`Sync pending operations require ${bytes} logical bytes, exceeding the protected ${policy.maxBytesPerNamespace}-byte namespace quota; pending mutations were retained.`
			);
	}
	return result;
};

/**
 * Durable, transactional local state partitioned by authenticated principal.
 * Implementations back this with IndexedDB on web/PWA and SQLite on native.
 */
export type SyncLocalStore = {
	transaction: <R>(
		namespace: string,
		mode: SyncLocalStoreMode,
		run: (tx: SyncLocalTransaction) => Promise<R>
	) => Promise<R>;
	/** Delete one signed-out principal without affecting any other account. */
	deleteNamespace: (namespace: string) => Promise<void>;
	/** Inspect the schema atomically prepared before local reads. */
	getSchemaStatus?: () => Promise<SyncLocalStoreSchemaStatus>;
};

export type IndexedDbSyncLocalStoreOptions = {
	/** Defaults to `absolutejs-sync-local-v1`. */
	databaseName?: string;
	/** Override for tests or non-window runtimes. Defaults to global IndexedDB. */
	indexedDB?: IDBFactory;
	/** Generated logical data-upgrade plan. Defaults to legacy schema 1. */
	storageSchema?: SyncLocalStoreSchemaInput;
	/** Required when any persisted collection or mutation requires protection. */
	protection?: SyncLocalProtectionProvider;
	/** Injectable clock for deterministic retention tests. */
	now?: () => number;
};

export type MemorySyncLocalStoreOptions = {
	storageSchema?: SyncLocalStoreSchemaInput;
	protection?: SyncLocalProtectionProvider;
	now?: () => number;
};

const clone = <T>(value: T): T => structuredClone(value);

type MemoryNamespace = {
	installationId?: string;
	collections: Map<string, LocalCollectionRecord>;
	mutations: Map<string, LocalMutationRecord>;
};

const emptyNamespace = (): MemoryNamespace => ({
	collections: new Map(),
	mutations: new Map()
});

const cloneNamespace = (source: MemoryNamespace): MemoryNamespace => ({
	installationId: source.installationId,
	collections: new Map(
		[...source.collections].map(([key, value]) => [key, clone(value)])
	),
	mutations: new Map(
		[...source.mutations].map(([key, value]) => [key, clone(value)])
	)
});

/**
 * In-memory reference adapter. Useful for SSR, tests, and as the executable
 * conformance model for durable adapters. Transactions are serialized and
 * roll back on throw.
 */
export const createMemorySyncLocalStore = ({
	storageSchema = { version: 1 },
	protection,
	now = Date.now
}: MemorySyncLocalStoreOptions = {}): SyncLocalStore => {
	const localData = resolveSyncLocalDataPolicy(storageSchema);
	let protectorPromise: Promise<SyncLocalRecordProtector> | undefined;
	const prepareProtector = () => (protectorPromise ??= protection?.prepare());
	const targetComponents = normalizeSyncLocalSchemaComponents(storageSchema);
	const resolvedSchema = resolveSyncLocalSchemaComponents(
		Object.fromEntries(
			targetComponents.map((component) => [
				component.id,
				component.version
			])
		),
		storageSchema
	);
	const schemaStatus = createSyncLocalSchemaStatus(
		resolvedSchema.components,
		[],
		isSchemaBundle(storageSchema)
	);
	const namespaces = new Map<string, MemoryNamespace>();
	let tail = Promise.resolve();
	const withLock = async <R>(run: () => Promise<R>): Promise<R> => {
		let release!: () => void;
		const previous = tail;
		tail = new Promise<void>((resolve) => {
			release = resolve;
		});
		await previous;
		try {
			return await run();
		} finally {
			release();
		}
	};

	const transaction: SyncLocalStore['transaction'] = async (
		namespace,
		mode,
		run
	) => {
		if (namespace.length === 0) {
			throw new Error('Sync local-store namespace must not be empty');
		}
		const protector = await prepareProtector();
		return withLock(async () => {
			const current = namespaces.get(namespace) ?? emptyNamespace();
			const working = cloneNamespace(current);
			const writable = () => {
				if (mode !== 'readwrite') {
					throw new Error(
						'Cannot write in a readonly Sync local transaction'
					);
				}
			};
			const tx: SyncLocalTransaction = {
				getInstallationId: async () => working.installationId,
				setInstallationId: async (installationId) => {
					writable();
					if (installationId.length === 0) {
						throw new Error(
							'Sync installation id must not be empty'
						);
					}
					working.installationId = installationId;
				},
				getCollection: async <T>(key: string) => {
					const record = working.collections.get(key);
					return record === undefined
						? undefined
						: clone(record as LocalCollectionRecord<T>);
				},
				listCollections: async () =>
					[...working.collections.entries()].map(([key, record]) => ({
						key,
						record: clone(record)
					})),
				putCollection: async (key, record) => {
					writable();
					working.collections.set(key, clone(record));
				},
				deleteCollection: async (key) => {
					writable();
					working.collections.delete(key);
				},
				listMutations: async () =>
					[...working.mutations.values()]
						.sort((a, b) => a.createdAt - b.createdAt)
						.map(clone),
				getMutation: async (operationId) => {
					const record = working.mutations.get(operationId);
					return record === undefined ? undefined : clone(record);
				},
				putMutation: async (record) => {
					writable();
					working.mutations.set(record.operationId, clone(record));
				},
				deleteMutation: async (operationId) => {
					writable();
					working.mutations.delete(operationId);
				}
			};
			const result = await runSyncLocalPolicyTransaction({
				mode,
				now: now(),
				policy: localData,
				protected: protector !== undefined,
				raw: tx,
				run
			});
			if (mode === 'readwrite') namespaces.set(namespace, working);
			return result;
		});
	};

	return {
		transaction,
		getSchemaStatus: async () => ({ ...schemaStatus }),
		deleteNamespace: async (namespace) => {
			await withLock(async () => {
				namespaces.delete(namespace);
				return undefined;
			});
		}
	};
};

type ProtectedLocalRecord = {
	name: string;
	protector: string;
	value: string;
};
type IndexedCollectionRow = Partial<LocalCollectionRecord> & {
	namespace: string;
	key: string;
	protectedRecord?: ProtectedLocalRecord;
};
type IndexedMutationRow = Partial<LocalMutationRecord> & {
	namespace: string;
	operationId: string;
	createdAt?: number;
	protectedRecord?: ProtectedLocalRecord;
};

const openProtectedRecord = <T>(
	value: ProtectedLocalRecord,
	protector: SyncLocalRecordProtector | undefined,
	context: Omit<SyncLocalRecordProtectionContext, 'name'>
): T => {
	if (protector === undefined || protector.id !== value.protector)
		throw new SyncLocalDataPolicyError(
			'PROTECTION_REQUIRED',
			`Sync data is protected by "${value.protector}", which is unavailable in this runtime.`
		);
	return JSON.parse(
		protector.open(value.value, { ...context, name: value.name })
	) as T;
};

const requestResult = <T>(request: IDBRequest<T>): Promise<T> =>
	new Promise((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});

const transactionComplete = (transaction: IDBTransaction): Promise<void> =>
	new Promise((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onabort = () =>
			reject(
				transaction.error ?? new Error('IndexedDB transaction aborted')
			);
		transaction.onerror = () =>
			reject(
				transaction.error ?? new Error('IndexedDB transaction failed')
			);
	});

const deleteIndexRows = (index: IDBIndex, namespace: string): Promise<void> =>
	new Promise((resolve, reject) => {
		const request = index.openCursor(namespace);
		request.onerror = () => reject(request.error);
		request.onsuccess = () => {
			const cursor = request.result;
			if (cursor === null) {
				resolve();
				return;
			}
			cursor.delete();
			cursor.continue();
		};
	});

const migrateIndexedRows = <T>(
	request: IDBRequest<IDBCursorWithValue | null>,
	migrate: (row: T) => T | null
): Promise<void> =>
	new Promise((resolve, reject) => {
		request.onerror = () => reject(request.error);
		request.onsuccess = () => {
			const cursor = request.result;
			if (cursor === null) {
				resolve();
				return;
			}
			let next: T | null;
			try {
				next = migrate(cursor.value as T);
			} catch (error) {
				reject(error);
				return;
			}
			const write = next === null ? cursor.delete() : cursor.update(next);
			write.onerror = () => reject(write.error);
			write.onsuccess = () => cursor.continue();
		};
	});

/**
 * Browser/PWA implementation of {@link SyncLocalStore}. Confirmed collection
 * state, cursors, installation identity, and the mutation outbox share one
 * IndexedDB transaction, including multi-collection frame commits.
 */
export const createIndexedDbSyncLocalStore = ({
	databaseName = 'absolutejs-sync-local-v1',
	indexedDB: factory = globalThis.indexedDB,
	storageSchema = { version: 1 },
	protection,
	now = Date.now
}: IndexedDbSyncLocalStoreOptions = {}): SyncLocalStore => {
	const localData = resolveSyncLocalDataPolicy(storageSchema);
	let protectorPromise: Promise<SyncLocalRecordProtector> | undefined;
	const prepareProtector = () => (protectorPromise ??= protection?.prepare());
	if (factory === undefined) {
		throw new Error(
			'createIndexedDbSyncLocalStore requires IndexedDB in this runtime'
		);
	}

	let databasePromise: Promise<IDBDatabase> | undefined;
	const database = () => {
		databasePromise ??= new Promise<IDBDatabase>((resolve, reject) => {
			const request = factory.open(databaseName, 2);
			request.onupgradeneeded = () => {
				const db = request.result;
				if (!db.objectStoreNames.contains('metadata'))
					db.createObjectStore('metadata');
				if (!db.objectStoreNames.contains('collections')) {
					const collections = db.createObjectStore('collections', {
						keyPath: ['namespace', 'key']
					});
					collections.createIndex('namespace', 'namespace');
				}
				if (!db.objectStoreNames.contains('mutations')) {
					const mutations = db.createObjectStore('mutations', {
						keyPath: ['namespace', 'operationId']
					});
					mutations.createIndex('namespace', 'namespace');
				}
				if (!db.objectStoreNames.contains('schema'))
					db.createObjectStore('schema');
			};
			request.onsuccess = () => {
				request.result.onversionchange = () => request.result.close();
				resolve(request.result);
			};
			request.onerror = () => reject(request.error);
			request.onblocked = () =>
				reject(
					new Error(`IndexedDB upgrade blocked for "${databaseName}"`)
				);
		});
		return databasePromise;
	};

	let schemaPromise: Promise<SyncLocalStoreSchemaStatus> | undefined;
	const prepareSchema = () => {
		schemaPromise ??= Promise.all([database(), prepareProtector()]).then(
			async ([db, preparedProtector]) => {
				const native = db.transaction(
					['schema', 'collections', 'mutations'],
					'readwrite'
				);
				const completed = transactionComplete(native);
				try {
					const schemaStore = native.objectStore('schema');
					const savedVersion = await requestResult<unknown>(
						schemaStore.get('logicalVersion')
					);
					const savedComponents = await requestResult<unknown>(
						schemaStore.get('componentVersions')
					);
					const storedVersions: Record<string, number> = {};
					if (
						typeof savedComponents === 'object' &&
						savedComponents !== null &&
						!Array.isArray(savedComponents)
					)
						for (const [id, version] of Object.entries(
							savedComponents
						)) {
							if (
								!Number.isSafeInteger(version) ||
								(version as number) < 1
							)
								throw new SyncLocalStoreSchemaError(
									'INVALID_PLAN',
									`Stored Sync component "${id}" has an invalid version`
								);
							storedVersions[id] = version as number;
						}
					if (
						storedVersions['@absolutejs/app'] === undefined &&
						typeof savedVersion === 'number'
					)
						storedVersions['@absolutejs/app'] = savedVersion;
					const resolved = resolveSyncLocalSchemaComponents(
						storedVersions,
						storageSchema
					);
					const steps = resolved.components.flatMap(
						(component) => component.steps
					);
					if (steps.length > 0) {
						await migrateIndexedRows<IndexedCollectionRow>(
							native.objectStore('collections').openCursor(),
							(row) => {
								const {
									namespace,
									key,
									protectedRecord,
									...plain
								} = row;
								const record = protectedRecord
									? openProtectedRecord<LocalCollectionRecord>(
											protectedRecord,
											preparedProtector,
											{ kind: 'collection', namespace }
										)
									: (plain as LocalCollectionRecord);
								const migrated =
									migrateSyncLocalCollectionRecord(
										record,
										{ key, namespace },
										steps
									);
								if (migrated === null) return null;
								return protectedRecord && preparedProtector
									? {
											key,
											namespace,
											protectedRecord: {
												...protectedRecord,
												value: preparedProtector.seal(
													JSON.stringify(migrated),
													{
														kind: 'collection',
														name:
															migrated.collection ??
															key,
														namespace
													}
												)
											}
										}
									: { ...migrated, key, namespace };
							}
						);
						await migrateIndexedRows<IndexedMutationRow>(
							native.objectStore('mutations').openCursor(),
							(row) => {
								const { namespace, protectedRecord, ...plain } =
									row;
								const record = protectedRecord
									? openProtectedRecord<LocalMutationRecord>(
											protectedRecord,
											preparedProtector,
											{ kind: 'mutation', namespace }
										)
									: (plain as LocalMutationRecord);
								const migrated = migrateSyncLocalMutationRecord(
									record,
									{ key: record.operationId, namespace },
									steps
								);
								if (migrated === null) return null;
								return protectedRecord && preparedProtector
									? {
											createdAt: migrated.createdAt,
											namespace,
											operationId: migrated.operationId,
											protectedRecord: {
												...protectedRecord,
												value: preparedProtector.seal(
													JSON.stringify(migrated),
													{
														kind: 'mutation',
														name: migrated.name,
														namespace
													}
												)
											}
										}
									: { ...migrated, namespace };
							}
						);
					}
					const nextVersions = { ...storedVersions };
					for (const component of resolved.components)
						nextVersions[component.id] = component.targetVersion;
					await requestResult(
						schemaStore.put(nextVersions, 'componentVersions')
					);
					const app = resolved.components.find(
						(component) => component.id === '@absolutejs/app'
					);
					if (app)
						await requestResult(
							schemaStore.put(app.targetVersion, 'logicalVersion')
						);
					await completed;
					return createSyncLocalSchemaStatus(
						resolved.components,
						resolved.orphanedComponents,
						isSchemaBundle(storageSchema)
					);
				} catch (error) {
					try {
						native.abort();
					} catch {
						// A failed request may already have aborted the transaction.
					}
					await completed.catch(() => {});
					throw error;
				}
			}
		);
		return schemaPromise;
	};

	const transaction: SyncLocalStore['transaction'] = async (
		namespace,
		mode,
		run
	) => {
		if (namespace.length === 0) {
			throw new Error('Sync local-store namespace must not be empty');
		}
		await prepareSchema();
		const protector = await prepareProtector();
		const db = await database();
		const native = db.transaction(
			['metadata', 'collections', 'mutations'],
			mode
		);
		const completed = transactionComplete(native);
		const writable = () => {
			if (mode !== 'readwrite') {
				throw new Error(
					'Cannot write in a readonly Sync local transaction'
				);
			}
		};
		const metadata = native.objectStore('metadata');
		const collections = native.objectStore('collections');
		const mutations = native.objectStore('mutations');
		const tx: SyncLocalTransaction = {
			getInstallationId: () =>
				requestResult<string | undefined>(metadata.get(namespace)),
			setInstallationId: async (installationId) => {
				writable();
				if (installationId.length === 0) {
					throw new Error('Sync installation id must not be empty');
				}
				await requestResult(metadata.put(installationId, namespace));
			},
			getCollection: async <T>(key: string) => {
				const row = await requestResult<
					IndexedCollectionRow | undefined
				>(collections.get([namespace, key]));
				if (row === undefined) return undefined;
				if (row.protectedRecord)
					return openProtectedRecord<LocalCollectionRecord<T>>(
						row.protectedRecord,
						protector,
						{ kind: 'collection', namespace }
					);
				const {
					namespace: _namespace,
					key: _key,
					protectedRecord: _protected,
					...record
				} = row;
				return record as LocalCollectionRecord<T>;
			},
			listCollections: async () => {
				const rows = await requestResult<IndexedCollectionRow[]>(
					collections.index('namespace').getAll(namespace)
				);
				return rows.map((row) => ({
					key: row.key,
					record: row.protectedRecord
						? openProtectedRecord<LocalCollectionRecord>(
								row.protectedRecord,
								protector,
								{ kind: 'collection', namespace }
							)
						: (({
								namespace: _namespace,
								key: _key,
								protectedRecord: _protected,
								...record
							}) => record as LocalCollectionRecord)(row)
				}));
			},
			putCollection: async (key, record) => {
				writable();
				await requestResult(
					collections.put(
						protector
							? {
									key,
									namespace,
									protectedRecord: {
										name: record.collection ?? key,
										protector: protector.id,
										value: protector.seal(
											JSON.stringify(record),
											{
												kind: 'collection',
												name: record.collection ?? key,
												namespace
											}
										)
									}
								}
							: { ...record, key, namespace }
					)
				);
			},
			deleteCollection: async (key) => {
				writable();
				await requestResult(collections.delete([namespace, key]));
			},
			listMutations: async () => {
				const rows = await requestResult<IndexedMutationRow[]>(
					mutations.index('namespace').getAll(namespace)
				);
				return rows
					.map((row) =>
						row.protectedRecord
							? openProtectedRecord<LocalMutationRecord>(
									row.protectedRecord,
									protector,
									{ kind: 'mutation', namespace }
								)
							: (({
									namespace: _namespace,
									protectedRecord: _protected,
									...record
								}) => record as LocalMutationRecord)(row)
					)
					.sort((a, b) => a.createdAt - b.createdAt);
			},
			getMutation: async (operationId) => {
				const row = await requestResult<IndexedMutationRow | undefined>(
					mutations.get([namespace, operationId])
				);
				if (row === undefined) return undefined;
				if (row.protectedRecord)
					return openProtectedRecord<LocalMutationRecord>(
						row.protectedRecord,
						protector,
						{ kind: 'mutation', namespace }
					);
				const {
					namespace: _namespace,
					protectedRecord: _protected,
					...record
				} = row;
				return record as LocalMutationRecord;
			},
			putMutation: async (record) => {
				writable();
				await requestResult(
					mutations.put(
						protector
							? {
									createdAt: record.createdAt,
									namespace,
									operationId: record.operationId,
									protectedRecord: {
										name: record.name,
										protector: protector.id,
										value: protector.seal(
											JSON.stringify(record),
											{
												kind: 'mutation',
												name: record.name,
												namespace
											}
										)
									}
								}
							: { ...record, namespace }
					)
				);
			},
			deleteMutation: async (operationId) => {
				writable();
				await requestResult(mutations.delete([namespace, operationId]));
			}
		};

		try {
			const result = await runSyncLocalPolicyTransaction({
				mode,
				now: now(),
				policy: localData,
				protected: protector !== undefined,
				raw: tx,
				run
			});
			await completed;
			return result;
		} catch (error) {
			try {
				native.abort();
			} catch {
				// The transaction may already have aborted because a request failed.
			}
			await completed.catch(() => {});
			throw error;
		}
	};

	return {
		transaction,
		getSchemaStatus: async () => ({ ...(await prepareSchema()) }),
		deleteNamespace: async (namespace) => {
			await prepareSchema();
			const db = await database();
			const native = db.transaction(
				['metadata', 'collections', 'mutations'],
				'readwrite'
			);
			const completed = transactionComplete(native);
			await requestResult(
				native.objectStore('metadata').delete(namespace)
			);
			await deleteIndexRows(
				native.objectStore('collections').index('namespace'),
				namespace
			);
			await deleteIndexRows(
				native.objectStore('mutations').index('namespace'),
				namespace
			);
			await completed;
		}
	};
};

const randomId = (): string => {
	const id = globalThis.crypto?.randomUUID?.();
	if (id === undefined) {
		throw new Error(
			'Sync operation ids require crypto.randomUUID or a createId callback'
		);
	}
	return id;
};

/** Get or create the stable installation id used to prefix operation ids. */
export const ensureSyncInstallationId = (
	store: SyncLocalStore,
	namespace: string,
	createId: () => string = randomId
): Promise<string> =>
	store.transaction(namespace, 'readwrite', async (tx) => {
		const existing = await tx.getInstallationId();
		if (existing !== undefined) return existing;
		const installationId = createId();
		await tx.setInstallationId(installationId);
		return installationId;
	});

/** Create a globally unique id grouped under a stable installation identity. */
export const createSyncOperationId = (
	installationId: string,
	createId: () => string = randomId
): string => `${installationId}:${createId()}`;
