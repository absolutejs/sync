import type { LocalMutationRecord, SyncLocalStore } from './localStore';

export type SyncClientConnectionStatus =
	| 'closed'
	| 'connecting'
	| 'offline'
	| 'online';

/** Framework-neutral local-first diagnostics. */
export type SyncClientStatus = {
	connection: SyncClientConnectionStatus;
	pending: number;
	deadLetters: number;
	/** Retained dead letters whose server outcome was a conflict. */
	conflicts: number;
	/** Conflicts resolved automatically during this client lifetime. */
	automaticResolutions: number;
	oldestPendingAt?: number;
	oldestDeadLetterAt?: number;
	lastConflictAt?: number;
	lastSuccessfulPullAt?: number;
	lastSuccessfulPushAt?: number;
	lastError?: string;
};

export type SyncFlushOptions = {
	/** Finite foreground/background budget. Defaults to 10 seconds. */
	timeoutMs?: number;
};

export type SyncFlushResult = {
	deadLetters: number;
	pending: number;
	timedOut: boolean;
};

export type SyncClientRuntimeTransport = {
	durable?: {
		createId?: () => string;
		onError?: (error: unknown) => void;
		maxAttempts?: number;
		retryBackoff?: (attempt: number) => number;
		store: SyncLocalStore;
		namespace: string;
	};
	/** Enroll clients in host lifecycle handling without page-level wiring. */
	registerClient?: (client: SyncRuntimeClient) => void | (() => void);
	socketTicket?: () => Promise<string>;
	/** Host-provided socket implementation, used by native/WebView bridges. */
	webSocketImpl?: typeof WebSocket;
};

export type SyncRuntimeClient = {
	discardDeadLetter: (operationId: string) => Promise<void>;
	flush: (options?: SyncFlushOptions) => Promise<SyncFlushResult>;
	listDeadLetters: () => Promise<LocalMutationRecord[]>;
	rebaseDeadLetter: (operationId: string, args: unknown) => Promise<string>;
	reconnect: () => void;
	retryDeadLetter: (operationId: string) => Promise<void>;
	status: () => SyncClientStatus;
	subscribeStatus: (
		listener: (status: SyncClientStatus) => void
	) => () => void;
};

export type SyncRuntimeDeadLetter = {
	attempts: number;
	code?: string;
	createdAt: number;
	deadLetteredAt?: number;
	kind?: 'conflict' | 'permanent' | 'retryable';
	message?: string;
	name: string;
	operationId: string;
};

/** Aggregate, redacted diagnostics for framework and host devtools. */
export type SyncRuntimeInspection = {
	automaticResolutions: number;
	clients: number;
	conflicts: number;
	deadLetters: SyncRuntimeDeadLetter[];
	lastError?: string;
	lastSuccessfulPullAt?: number;
	lastSuccessfulPushAt?: number;
	oldestDeadLetterAt?: number;
	oldestPendingAt?: number;
	pending: number;
};

type Installation = { transport: SyncClientRuntimeTransport };
type Registry = {
	clients: SyncRuntimeClient[];
	installations: Installation[];
};
const RUNTIME_TRANSPORT = Symbol.for(
	'@absolutejs/sync/client-runtime-transport'
);
const host = globalThis as { [key: symbol]: unknown };
const isRegistry = (value: unknown): value is Registry =>
	typeof value === 'object' &&
	value !== null &&
	Array.isArray(Reflect.get(value, 'installations')) &&
	Array.isArray(Reflect.get(value, 'clients'));
const registry = (() => {
	const existing = host[RUNTIME_TRANSPORT];
	if (isRegistry(existing)) return existing;
	if (
		typeof existing === 'object' &&
		existing !== null &&
		Array.isArray(Reflect.get(existing, 'installations'))
	) {
		Reflect.set(existing, 'clients', []);
		return existing as Registry;
	}
	const created: Registry = { clients: [], installations: [] };
	Object.defineProperty(host, RUNTIME_TRANSPORT, {
		configurable: false,
		enumerable: false,
		value: created,
		writable: false
	});

	return created;
})();

export const getSyncClientRuntimeTransport = () =>
	registry.installations.at(-1)?.transport;

export const installSyncClientRuntimeTransport = (
	transport: SyncClientRuntimeTransport
) => {
	const installation = { transport };
	registry.installations.push(installation);

	return () => {
		const index = registry.installations.indexOf(installation);
		if (index >= 0) registry.installations.splice(index, 1);
	};
};

export const registerSyncRuntimeClient = (client: SyncRuntimeClient) => {
	registry.clients.push(client);
	return () => {
		const index = registry.clients.indexOf(client);
		if (index >= 0) registry.clients.splice(index, 1);
	};
};

const maximum = (values: Array<number | undefined>) => {
	const present = values.filter(
		(value): value is number => value !== undefined
	);
	return present.length === 0 ? undefined : Math.max(...present);
};

const minimum = (values: Array<number | undefined>) => {
	const present = values.filter(
		(value): value is number => value !== undefined
	);
	return present.length === 0 ? undefined : Math.min(...present);
};

export const inspectSyncRuntime = async (): Promise<SyncRuntimeInspection> => {
	const clients = [...registry.clients];
	const statuses = clients.map((client) => client.status());
	const deadLetters = (
		await Promise.all(clients.map((client) => client.listDeadLetters()))
	)
		.flat()
		.map(
			(record): SyncRuntimeDeadLetter => ({
				attempts: record.attempts,
				...(record.rejection?.code
					? { code: record.rejection.code }
					: {}),
				createdAt: record.createdAt,
				...(record.deadLetteredAt === undefined
					? {}
					: { deadLetteredAt: record.deadLetteredAt }),
				...(record.rejection?.kind
					? { kind: record.rejection.kind }
					: {}),
				...(record.rejection?.message
					? { message: record.rejection.message }
					: {}),
				name: record.name,
				operationId: record.operationId
			})
		)
		.sort(
			(left, right) =>
				(left.deadLetteredAt ?? left.createdAt) -
					(right.deadLetteredAt ?? right.createdAt) ||
				left.operationId.localeCompare(right.operationId)
		);
	const lastError = statuses.findLast(
		(status) => status.lastError
	)?.lastError;
	return {
		automaticResolutions: statuses.reduce(
			(total, status) => total + status.automaticResolutions,
			0
		),
		clients: clients.length,
		conflicts: deadLetters.filter((record) => record.kind === 'conflict')
			.length,
		deadLetters,
		...(lastError ? { lastError } : {}),
		...(maximum(statuses.map((status) => status.lastSuccessfulPullAt)) ===
		undefined
			? {}
			: {
					lastSuccessfulPullAt: maximum(
						statuses.map((status) => status.lastSuccessfulPullAt)
					)!
				}),
		...(maximum(statuses.map((status) => status.lastSuccessfulPushAt)) ===
		undefined
			? {}
			: {
					lastSuccessfulPushAt: maximum(
						statuses.map((status) => status.lastSuccessfulPushAt)
					)!
				}),
		...(minimum(statuses.map((status) => status.oldestDeadLetterAt)) ===
		undefined
			? {}
			: {
					oldestDeadLetterAt: minimum(
						statuses.map((status) => status.oldestDeadLetterAt)
					)!
				}),
		...(minimum(statuses.map((status) => status.oldestPendingAt)) ===
		undefined
			? {}
			: {
					oldestPendingAt: minimum(
						statuses.map((status) => status.oldestPendingAt)
					)!
				}),
		pending: statuses.reduce((total, status) => total + status.pending, 0)
	};
};

const clientWithDeadLetter = async (operationId: string) => {
	for (const client of registry.clients)
		if (
			(await client.listDeadLetters()).some(
				(record) => record.operationId === operationId
			)
		)
			return client;
	throw new Error(`Unknown Sync dead letter "${operationId}"`);
};

export const retrySyncRuntimeDeadLetter = async (operationId: string) =>
	(await clientWithDeadLetter(operationId)).retryDeadLetter(operationId);

export const discardSyncRuntimeDeadLetter = async (operationId: string) =>
	(await clientWithDeadLetter(operationId)).discardDeadLetter(operationId);

export const rebaseSyncRuntimeDeadLetter = async (
	operationId: string,
	args: unknown
) =>
	(await clientWithDeadLetter(operationId)).rebaseDeadLetter(
		operationId,
		args
	);
