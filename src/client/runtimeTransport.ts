import type { SyncLocalStore } from './localStore';

export type SyncClientRuntimeTransport = {
	durable?: {
		createId?: () => string;
		onError?: (error: unknown) => void;
		store: SyncLocalStore;
		namespace: string;
	};
	socketTicket: () => Promise<string>;
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
