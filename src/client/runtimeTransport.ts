import type { SyncLocalStore } from './localStore';

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
	oldestPendingAt?: number;
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
	socketTicket: () => Promise<string>;
};

export type SyncRuntimeClient = {
	flush: (options?: SyncFlushOptions) => Promise<SyncFlushResult>;
	reconnect: () => void;
	status: () => SyncClientStatus;
	subscribeStatus: (
		listener: (status: SyncClientStatus) => void
	) => () => void;
};

type Installation = { transport: SyncClientRuntimeTransport };
type Registry = { installations: Installation[] };
const RUNTIME_TRANSPORT = Symbol.for(
	'@absolutejs/sync/client-runtime-transport'
);
const host = globalThis as { [key: symbol]: unknown };
const isRegistry = (value: unknown): value is Registry =>
	typeof value === 'object' &&
	value !== null &&
	Array.isArray(Reflect.get(value, 'installations'));
const registry = (() => {
	const existing = host[RUNTIME_TRANSPORT];
	if (isRegistry(existing)) return existing;
	const created: Registry = { installations: [] };
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
