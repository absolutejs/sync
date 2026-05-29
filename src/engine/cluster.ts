import type { RowChange } from './types';

/**
 * Horizontal scale — the seam for running the engine across many server
 * instances. The in-process hub only reaches subscribers on the same instance;
 * a {@link ClusterBus} fans every instance's committed changes out to the
 * others, so a mutation on instance A reaches subscribers on instance B.
 *
 * Client-agnostic like the {@link ChangeSource} seam: you supply `publish` +
 * `subscribe` over your bus of choice (Redis stream, Postgres `LISTEN/NOTIFY`,
 * NATS…), and the engine handles fan-out and loop prevention (each message is
 * tagged with the originating instance, and an instance ignores its own).
 *
 * **1.17.0:** cross-instance resume now works. Each `ClusterMessage` carries
 * the originating instance's `originVersion`; every engine logs peer changes
 * against `(origin, originVersion)` in the same change log it uses for its
 * own commits. Subscriptions return an opaque resume cursor encoding a
 * vector of `(instanceId, version)` pairs; on reconnect to a DIFFERENT
 * instance, the cursor round-trips and the new instance builds a catch-up
 * covering both its own + the peer's changes — no fresh snapshot needed.
 */

/** A committed change as it travels over the bus. */
export type ClusterChange = {
	table: string;
	change: RowChange<unknown>;
};

export type ClusterMessage = {
	/** The instance that produced these changes (so peers ignore their own). */
	origin: string;
	/**
	 * The originating instance's local version at the time it broadcast.
	 * Each change in `changes` is logged on the receiving instance against
	 * `(origin, originVersion)` so a client carrying a cursor that
	 * references `origin` can resume across instances. Added in 1.17.0;
	 * older `ClusterBus` implementations that omit this field default peer
	 * changes to version `0` (any cross-instance resume falls back to a
	 * snapshot — matches pre-1.17 behavior exactly).
	 */
	originVersion?: number;
	changes: ClusterChange[];
};

export type ClusterBus = {
	/** Broadcast this instance's committed changes to the others. */
	publish: (message: ClusterMessage) => void | Promise<void>;
	/**
	 * Receive other instances' changes; return a function that stops listening.
	 * The engine filters out messages from its own `origin`.
	 */
	subscribe: (
		onMessage: (message: ClusterMessage) => void
	) => (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
};

/**
 * An in-process {@link ClusterBus} — for tests, local dev, or several engines in
 * one process. **Not** cross-process: real horizontal scale needs a bus that
 * spans machines (Redis stream, Postgres `LISTEN/NOTIFY`).
 */
export const createInMemoryClusterBus = (): ClusterBus => {
	const listeners = new Set<(message: ClusterMessage) => void>();

	return {
		publish: (message) => {
			for (const listener of listeners) {
				listener(message);
			}
		},
		subscribe: (onMessage) => {
			listeners.add(onMessage);

			return () => {
				listeners.delete(onMessage);
			};
		}
	};
};
