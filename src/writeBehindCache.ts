export type WriteBehindCacheOptions<K, V> = {
	/**
	 * Read a value from the durable store on a cache miss. Called at most once per
	 * key until the entry is evicted.
	 */
	load: (key: K) => Promise<V | undefined> | V | undefined;
	/** Persist a value to the durable store. Runs in the background (write-behind). */
	persist: (key: K, value: V) => Promise<void> | void;
	/** Remove a value from the durable store. */
	remove?: (key: K) => Promise<void> | void;
	/**
	 * Coalesce writes: each key persists at most once per window. A burst of
	 * `set`s collapses into a single durable write. Defaults to 250ms.
	 */
	debounceMs?: number;
	/**
	 * After a key persists, return true to drop it from the in-memory cache so the
	 * cache stays bounded to "hot" entries (e.g. evict terminal sessions). The next
	 * `get` reloads it via `load`. Defaults to never evicting.
	 */
	evict?: (value: V, key: K) => boolean;
	/**
	 * Called when a background persist throws. The cache stays authoritative and the
	 * key re-persists on its next `set`, so a transient durable-store blip does not
	 * drop live state. Defaults to a no-op.
	 */
	onPersistError?: (error: unknown, key: K) => void;
};

export type WriteBehindCache<K, V> = {
	/** Cached value, or load-through from the durable store on a miss. */
	get: (key: K) => Promise<V | undefined>;
	/** Cached value only — synchronous, never touches the durable store. */
	peek: (key: K) => V | undefined;
	has: (key: K) => boolean;
	/** Write to memory immediately and schedule a coalesced durable persist. */
	set: (key: K, value: V) => void;
	/** Drop from cache and the durable store. */
	delete: (key: K) => Promise<void>;
	keys: () => IterableIterator<K>;
	values: () => IterableIterator<V>;
	size: () => number;
	/** Persist every pending key to the durable store now. Call on shutdown. */
	flush: () => Promise<void>;
};

/**
 * Wrap a durable store (Postgres, SQLite, Drizzle, Prisma, file, S3, an HTTP API …)
 * with an in-memory hot cache and write-behind persistence.
 *
 * Reads are served from memory; writes hit memory synchronously and are flushed to
 * the durable store in coalesced background batches. The durable store stays the
 * source of truth for history and cross-instance reads, while a latency-sensitive
 * hot path (a per-frame voice session, presence, cursors, game state) stays fast.
 *
 * This is the "fast authoritative local state, durable persistence synced behind it"
 * split a sync engine like Convex makes — without adopting a whole sync-engine
 * backend. Bring your own store via `load`/`persist`/`remove`.
 */
export const createWriteBehindCache = <K, V>(
	options: WriteBehindCacheOptions<K, V>
): WriteBehindCache<K, V> => {
	const debounceMs = options.debounceMs ?? 250;
	const cache = new Map<K, V>();
	const timers = new Map<K, ReturnType<typeof setTimeout>>();

	const persist = async (key: K) => {
		timers.delete(key);
		const value = cache.get(key);
		if (value === undefined) {
			return;
		}
		try {
			await options.persist(key, value);
			if (options.evict?.(value, key)) {
				cache.delete(key);
			}
		} catch (error) {
			options.onPersistError?.(error, key);
		}
	};

	const schedulePersist = (key: K) => {
		if (timers.has(key)) {
			return;
		}
		timers.set(
			key,
			setTimeout(() => {
				void persist(key);
			}, debounceMs)
		);
	};

	return {
		get: async (key) => {
			const cached = cache.get(key);
			if (cached !== undefined) {
				return cached;
			}
			const loaded = await options.load(key);
			if (loaded !== undefined) {
				cache.set(key, loaded);
			}
			return loaded;
		},
		peek: (key) => cache.get(key),
		has: (key) => cache.has(key),
		set: (key, value) => {
			cache.set(key, value);
			schedulePersist(key);
		},
		delete: async (key) => {
			const timer = timers.get(key);
			if (timer) {
				clearTimeout(timer);
				timers.delete(key);
			}
			cache.delete(key);
			await options.remove?.(key);
		},
		keys: () => cache.keys(),
		values: () => cache.values(),
		size: () => cache.size,
		flush: async () => {
			for (const timer of timers.values()) {
				clearTimeout(timer);
			}
			timers.clear();
			await Promise.all([...cache.keys()].map((key) => persist(key)));
		}
	};
};
