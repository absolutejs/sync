import type {
	HeadlessSyncPullResult,
	HeadlessSyncRequest,
	HeadlessSyncResponse
} from '../headlessProtocol';
import type { RowKey } from '../engine/types';
import type { SyncMutationRejection } from '../reconciliation';
import type {
	LocalCollectionRecord,
	LocalMutationRecord,
	SyncLocalStore
} from './localStore';

export type HeadlessSyncCollection<T = unknown> = {
	collection: string;
	params?: unknown;
	/** Stable local-store key used by the foreground collection handle. */
	localKey: string;
	/** Defaults to the row's `id` property. */
	key?: (row: T) => RowKey;
};

export type HeadlessSyncFetchResponse = {
	ok: boolean;
	status: number;
	json: () => Promise<unknown>;
};

export type RunHeadlessSyncOptions = {
	endpoint: string;
	store: SyncLocalStore;
	namespace: string;
	collections?: HeadlessSyncCollection[];
	/** Maximum due outbox records sent in one run. Defaults to 50. */
	maxMutations?: number;
	/** Maximum collection descriptors pulled in one run. Defaults to 50. */
	maxPulls?: number;
	/** Inclusive automatic rejection ceiling. Defaults to 5 attempts. */
	maxAttempts?: number;
	retryBackoff?: (attempt: number) => number;
	/** Supply short-lived authorization headers without coupling Sync to Auth. */
	headers?: () => Record<string, string> | Promise<Record<string, string>>;
	/** Injection seam for constrained native runners and tests. */
	fetch?: (
		url: string,
		init: {
			body: string;
			credentials: 'include';
			headers: Record<string, string>;
			method: 'POST';
			redirect: 'error';
		}
	) => Promise<HeadlessSyncFetchResponse>;
	now?: () => number;
};

export type HeadlessSyncRunResult = {
	acknowledged: number;
	conflictsDiscarded: number;
	conflictsRetried: number;
	deadLettered: number;
	pulled: number;
	retryScheduled: number;
};

const defaultKey = (row: unknown): RowKey => {
	if (typeof row !== 'object' || row === null)
		throw new TypeError('Headless Sync rows require an object key.');
	const key = Reflect.get(row, 'id');
	if (typeof key !== 'string' && typeof key !== 'number')
		throw new TypeError(
			'Headless Sync rows require an id key or an explicit key function.'
		);
	return key;
};

const requireInteger = (value: number | undefined, fallback: number) => {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result < 0)
		throw new TypeError(
			'Headless Sync limits must be non-negative integers.'
		);
	return result;
};

const retryDelay = (attempt: number) =>
	Math.min(30_000, 500 * 2 ** Math.max(0, attempt - 1));

const parseResponse = (value: unknown): HeadlessSyncResponse => {
	if (
		typeof value !== 'object' ||
		value === null ||
		Reflect.get(value, 'version') !== 1 ||
		!Array.isArray(Reflect.get(value, 'mutations')) ||
		!Array.isArray(Reflect.get(value, 'pulls'))
	)
		throw new Error('The Headless Sync response is malformed.');
	return value as HeadlessSyncResponse;
};

const sameBaseline = (
	current: LocalCollectionRecord | undefined,
	baseline: LocalCollectionRecord | undefined
) =>
	current?.cursor === baseline?.cursor &&
	(current?.version ?? 0) === (baseline?.version ?? 0);

const applyPull = (
	current: LocalCollectionRecord | undefined,
	pull: Exclude<HeadlessSyncPullResult, { type: 'error' }>,
	collection: HeadlessSyncCollection,
	key: (row: unknown) => RowKey
): LocalCollectionRecord => {
	const descriptor: Omit<LocalCollectionRecord, 'rows' | 'version'> = {
		...current,
		collection: collection.collection,
		params: collection.params
	};
	if (collection.key === undefined) descriptor.headlessKey = 'id';
	else delete descriptor.headlessKey;
	if (pull.type === 'snapshot')
		return {
			...descriptor,
			rows: pull.rows,
			version: pull.version,
			cursor: pull.cursor
		};
	const rows = new Map<RowKey, unknown>();
	for (const row of current?.rows ?? []) rows.set(key(row), row);
	for (const row of pull.removed) rows.delete(key(row));
	for (const row of pull.changed) rows.set(key(row), row);
	for (const row of pull.added) rows.set(key(row), row);
	return {
		...descriptor,
		rows: [...rows.values()],
		version: pull.version,
		cursor: pull.cursor
	};
};

/**
 * Run one bounded HTTP push/pull cycle against a durable local store. It has no
 * DOM or WebSocket dependency and commits each response only if the foreground
 * process has not advanced that record since the request snapshot.
 */
export const runHeadlessSync = async ({
	endpoint,
	store,
	namespace,
	collections,
	maxMutations: configuredMaxMutations,
	maxPulls: configuredMaxPulls,
	maxAttempts: configuredMaxAttempts,
	retryBackoff = retryDelay,
	headers,
	fetch: fetchOverride,
	now = Date.now
}: RunHeadlessSyncOptions): Promise<HeadlessSyncRunResult> => {
	if (endpoint.length === 0 || namespace.length === 0)
		throw new TypeError(
			'Headless Sync endpoint and namespace are required.'
		);
	const maxMutations = requireInteger(configuredMaxMutations, 50);
	const maxPulls = requireInteger(configuredMaxPulls, 50);
	const maxAttempts = requireInteger(configuredMaxAttempts, 5);
	const startedAt = now();
	const snapshot = await store.transaction(
		namespace,
		'readwrite',
		async (tx) => {
			const due = (await tx.listMutations())
				.filter(
					(record) =>
						record.state !== 'dead-letter' &&
						(record.nextAttemptAt ?? 0) <= startedAt
				)
				.slice(0, maxMutations);
			const mutations: LocalMutationRecord[] = [];
			for (const record of due) {
				const next = { ...record, attempts: record.attempts + 1 };
				delete next.nextAttemptAt;
				await tx.putMutation(next);
				mutations.push(next);
			}
			const discoveredCollections: HeadlessSyncCollection[] =
				collections ??
				(await tx.listCollections())
					.filter(
						({ record }) =>
							record.headlessKey === 'id' &&
							typeof record.collection === 'string' &&
							record.collection.length > 0
					)
					.map(({ key, record }) => ({
						collection: record.collection!,
						localKey: key,
						params: record.params
					}));
			const pulls = await Promise.all(
				discoveredCollections
					.slice(0, maxPulls)
					.map(async (collection) => ({
						collection,
						record: await tx.getCollection(collection.localKey)
					}))
			);
			return { mutations, pulls };
		}
	);

	const request: HeadlessSyncRequest = {
		version: 1,
		mutations: snapshot.mutations.map(({ operationId, name, args }) => ({
			operationId,
			name,
			args
		})),
		pulls: snapshot.pulls.map(({ collection, record }, index) => ({
			id: String(index),
			collection: collection.collection,
			params: collection.params,
			...(record?.cursor !== undefined
				? { since: record.cursor }
				: (record?.version ?? 0) > 0
					? { since: record!.version }
					: {})
		}))
	};
	const fetchImpl =
		fetchOverride ??
		((url, init) =>
			globalThis.fetch(url, init) as Promise<HeadlessSyncFetchResponse>);
	let response: HeadlessSyncResponse;
	try {
		const fetched = await fetchImpl(endpoint, {
			body: JSON.stringify(request),
			credentials: 'include',
			headers: {
				accept: 'application/json',
				'content-type': 'application/json',
				...(await headers?.())
			},
			method: 'POST',
			redirect: 'error'
		});
		if (!fetched.ok)
			throw new Error(
				`Headless Sync failed with HTTP ${fetched.status}.`
			);
		response = parseResponse(await fetched.json());
	} catch (error) {
		await store.transaction(namespace, 'readwrite', async (tx) => {
			for (const sent of snapshot.mutations) {
				const current = await tx.getMutation(sent.operationId);
				if (current?.attempts !== sent.attempts) continue;
				await tx.putMutation({
					...current,
					lastError:
						error instanceof Error ? error.message : String(error),
					nextAttemptAt:
						now() + Math.max(0, retryBackoff(current.attempts))
				});
			}
		});
		throw error;
	}

	const mutationResults = new Map(
		response.mutations.map((result) => [result.operationId, result])
	);
	const pullResults = new Map(
		response.pulls.map((result) => [result.id, result])
	);
	const result: HeadlessSyncRunResult = {
		acknowledged: 0,
		conflictsDiscarded: 0,
		conflictsRetried: 0,
		deadLettered: 0,
		pulled: 0,
		retryScheduled: 0
	};
	await store.transaction(namespace, 'readwrite', async (tx) => {
		for (const sent of snapshot.mutations) {
			const current = await tx.getMutation(sent.operationId);
			if (current?.attempts !== sent.attempts) continue;
			const outcome = mutationResults.get(sent.operationId);
			if (outcome?.status === 'ack') {
				await tx.deleteMutation(sent.operationId);
				result.acknowledged += 1;
				continue;
			}
			const rejection: SyncMutationRejection =
				outcome?.status === 'reject'
					? outcome.rejection
					: {
							kind: 'retryable',
							message: 'Missing mutation response.'
						};
			if (
				rejection.kind === 'conflict' &&
				current.conflictPolicy?.strategy === 'server-wins'
			) {
				await tx.deleteMutation(sent.operationId);
				result.conflictsDiscarded += 1;
				continue;
			}
			if (
				rejection.kind === 'conflict' &&
				current.conflictPolicy?.strategy === 'client-wins' &&
				(current.conflictAttempts ?? 0) <
					(current.conflictPolicy.maxAttempts ?? 1)
			) {
				const retry: LocalMutationRecord = {
					...current,
					conflictAttempts: (current.conflictAttempts ?? 0) + 1,
					lastError: rejection.message,
					rejection,
					state: 'pending'
				};
				delete retry.nextAttemptAt;
				await tx.putMutation(retry);
				result.conflictsRetried += 1;
				continue;
			}
			if (
				rejection.kind !== 'retryable' ||
				current.attempts >= maxAttempts
			) {
				const deadLetter: LocalMutationRecord = {
					...current,
					deadLetteredAt: now(),
					lastError: rejection.message,
					rejection,
					state: 'dead-letter'
				};
				delete deadLetter.nextAttemptAt;
				await tx.putMutation(deadLetter);
				result.deadLettered += 1;
			} else {
				await tx.putMutation({
					...current,
					lastError: rejection.message,
					nextAttemptAt:
						now() +
						Math.max(
							0,
							rejection.retryAfterMs ??
								retryBackoff(current.attempts)
						),
					rejection,
					state: 'pending'
				});
				result.retryScheduled += 1;
			}
		}
		for (let index = 0; index < snapshot.pulls.length; index += 1) {
			const baseline = snapshot.pulls[index]!;
			const pull = pullResults.get(String(index));
			if (pull === undefined || pull.type === 'error') continue;
			const current = await tx.getCollection(
				baseline.collection.localKey
			);
			if (!sameBaseline(current, baseline.record)) continue;
			await tx.putCollection(
				baseline.collection.localKey,
				applyPull(
					current,
					pull,
					baseline.collection,
					baseline.collection.key ?? defaultKey
				)
			);
			result.pulled += 1;
		}
	});
	return result;
};
