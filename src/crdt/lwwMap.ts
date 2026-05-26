/**
 * A last-write-wins map (LWW-Map) — a CRDT key→value map. Each key is an
 * independent LWW register: the write (set or delete) with the highest timestamp
 * wins, ties broken by replica id. `delete` is a tombstone so it can lose to a
 * later concurrent `set`. State-based: `merge` is per-key LWW.
 */

export type LwwMapEntry<V> = {
	value?: V;
	deleted: boolean;
	timestamp: number;
	replica: string;
};

export type LwwMapState<V> = Record<string, LwwMapEntry<V>>;

const pick = <V>(a: LwwMapEntry<V>, b: LwwMapEntry<V>): LwwMapEntry<V> => {
	if (b.timestamp > a.timestamp) {
		return b;
	}
	if (b.timestamp < a.timestamp) {
		return a;
	}
	return b.replica > a.replica ? b : a;
};

export const lwwMap = {
	create: <V>(): LwwMapState<V> => ({}),

	set: <V>(
		state: LwwMapState<V>,
		key: string,
		value: V,
		replica: string,
		timestamp = Date.now()
	): LwwMapState<V> => ({
		...state,
		[key]: { value, deleted: false, timestamp, replica }
	}),

	delete: <V>(
		state: LwwMapState<V>,
		key: string,
		replica: string,
		timestamp = Date.now()
	): LwwMapState<V> => ({
		...state,
		[key]: { value: state[key]?.value, deleted: true, timestamp, replica }
	}),

	get: <V>(state: LwwMapState<V>, key: string): V | undefined => {
		const entry = state[key];

		return entry !== undefined && !entry.deleted ? entry.value : undefined;
	},

	has: <V>(state: LwwMapState<V>, key: string): boolean => {
		const entry = state[key];

		return entry !== undefined && !entry.deleted;
	},

	keys: <V>(state: LwwMapState<V>): string[] =>
		Object.keys(state).filter((key) => !state[key]?.deleted),

	entries: <V>(state: LwwMapState<V>): [string, V][] => {
		const out: [string, V][] = [];
		for (const [key, entry] of Object.entries(state)) {
			if (!entry.deleted && entry.value !== undefined) {
				out.push([key, entry.value]);
			}
		}

		return out;
	},

	/** Per-key last-write-wins (commutative/idempotent). */
	merge: <V>(a: LwwMapState<V>, b: LwwMapState<V>): LwwMapState<V> => {
		const out: LwwMapState<V> = { ...a };
		for (const [key, entry] of Object.entries(b)) {
			const existing = out[key];
			out[key] = existing === undefined ? entry : pick(existing, entry);
		}

		return out;
	}
};
