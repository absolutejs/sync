/**
 * Conflict-free replicated data types (CRDTs) for multiplayer/offline editing —
 * pure, dependency-free, and isomorphic (use the same code client and server).
 *
 * These are *state-based* CRDTs (CvRDTs): every `merge` is commutative,
 * associative, and idempotent, so replicas that exchange state in any order
 * converge to the same value. That fits the sync engine without engine changes:
 * store the CRDT state as a row field, have a mutation `merge` the incoming
 * state into the stored one (concurrent writes combine instead of clobbering),
 * and have each client merge the broadcast state into its local edits.
 */

const sumValues = (counts: Record<string, number>) =>
	Object.values(counts).reduce((total, value) => total + value, 0);

const mergeMax = (
	a: Record<string, number>,
	b: Record<string, number>
): Record<string, number> => {
	const merged: Record<string, number> = { ...a };
	for (const [replica, value] of Object.entries(b)) {
		merged[replica] = Math.max(merged[replica] ?? 0, value);
	}
	return merged;
};

/* ─── PN-counter ─── */

/** A counter that survives concurrent increments/decrements across replicas. */
export type CounterState = {
	increments: Record<string, number>;
	decrements: Record<string, number>;
};

export const counter = {
	create: (): CounterState => ({ increments: {}, decrements: {} }),
	/** Current value: total increments minus total decrements. */
	value: (state: CounterState) =>
		sumValues(state.increments) - sumValues(state.decrements),
	increment: (
		state: CounterState,
		replica: string,
		by = 1
	): CounterState => ({
		increments: {
			...state.increments,
			[replica]: (state.increments[replica] ?? 0) + by
		},
		decrements: state.decrements
	}),
	decrement: (
		state: CounterState,
		replica: string,
		by = 1
	): CounterState => ({
		increments: state.increments,
		decrements: {
			...state.decrements,
			[replica]: (state.decrements[replica] ?? 0) + by
		}
	}),
	/** Merge by taking the max count seen per replica (monotonic). */
	merge: (a: CounterState, b: CounterState): CounterState => ({
		increments: mergeMax(a.increments, b.increments),
		decrements: mergeMax(a.decrements, b.decrements)
	})
};

/* ─── LWW register ─── */

/** A single value where the latest write wins (ties broken by replica id). */
export type LwwState<T> = { value: T; timestamp: number; replica: string };

export const lww = {
	create: <T>(
		value: T,
		replica: string,
		timestamp = Date.now()
	): LwwState<T> => ({ value, timestamp, replica }),
	set: <T>(
		value: T,
		replica: string,
		timestamp = Date.now()
	): LwwState<T> => ({
		value,
		timestamp,
		replica
	}),
	/** Keep the entry with the higher timestamp (replica id breaks ties). */
	merge: <T>(a: LwwState<T>, b: LwwState<T>): LwwState<T> => {
		if (b.timestamp > a.timestamp) {
			return b;
		}
		if (b.timestamp < a.timestamp) {
			return a;
		}
		return b.replica > a.replica ? b : a;
	}
};

/* ─── Collaborative text ─── */

/**
 * The contract a collaborative-text CRDT exposes, independent of the algorithm
 * behind it. Implemented first-party by the RGA below ({@link createTextCrdt})
 * and by third-party backends in the `sync-adapters` repo (e.g.
 * `@absolutejs/sync-yjs`). `State` is whatever that backend persists and
 * broadcasts — JSON ({@link TextState}) for the RGA, a base64 update for Yjs.
 */
export type CrdtText<State> = {
	/** The current visible text. */
	text: () => string;
	/** Reconcile the local text to `next` (the backend computes the edit). */
	setText: (next: string) => void;
	/** Merge another replica's state in (e.g. a broadcast from the server). */
	merge: (state: State) => void;
	/** The serializable state to persist/broadcast. */
	state: () => State;
};

/**
 * The minimal server-side surface the engine needs to auto-merge a CRDT field on
 * write (see `engine.registerCrdt`): combine two states and produce an empty one.
 * Both the first-party {@link rgaText} and `@absolutejs/sync-yjs`'s `yjsText`
 * satisfy it, as does any {@link TextCrdtAdapter}.
 */
export type CrdtMergeable<State> = {
	empty: () => State;
	merge: (a: State, b: State) => State;
};

/**
 * A pluggable collaborative-text backend. `create` mints a live doc for a
 * replica; `merge` combines two persisted states server-side (no live instance
 * needed — for the merge-on-write mutation); `empty`/`textOf` are conveniences.
 * Swap the first-party {@link rgaText} for an adapter to get a different engine
 * (e.g. Yjs) behind the exact same call sites.
 */
export type TextCrdtAdapter<State> = CrdtMergeable<State> & {
	create: (replica: string, initial?: State) => CrdtText<State>;
	textOf: (state: State) => string;
};

/* ─── Collaborative text (RGA) — the first-party backend ─── */

/** One inserted character in the replicated sequence (kept as a tombstone if deleted). */
export type TextElement = {
	id: string;
	replica: string;
	clock: number;
	/** Id of the element this was inserted after (`null` = start of document). */
	after: string | null;
	value: string;
	deleted: boolean;
};

/** Serializable state of a {@link TextCrdt} — safe to store as a row field. */
export type TextState = { elements: TextElement[] };

// Sibling order (same `after`): higher clock first, then higher replica id.
const compare = (a: TextElement, b: TextElement) => {
	if (a.clock !== b.clock) {
		return b.clock - a.clock;
	}
	if (a.replica === b.replica) {
		return 0;
	}
	return a.replica > b.replica ? -1 : 1;
};

/** Flatten the sequence into document order (an iterative RGA pre-order walk). */
const linearize = (elements: TextElement[]): TextElement[] => {
	const children = new Map<string | null, TextElement[]>();
	for (const element of elements) {
		const list = children.get(element.after);
		if (list === undefined) {
			children.set(element.after, [element]);
		} else {
			list.push(element);
		}
	}
	for (const list of children.values()) {
		list.sort(compare);
	}
	const ordered: TextElement[] = [];
	const stack = [...(children.get(null) ?? [])].reverse();
	while (stack.length > 0) {
		const element = stack.pop()!;
		ordered.push(element);
		const kids = children.get(element.id);
		if (kids !== undefined) {
			for (let index = kids.length - 1; index >= 0; index -= 1) {
				stack.push(kids[index]!);
			}
		}
	}
	return ordered;
};

/** The visible string of a text-CRDT state. Pure — use it server-side too. */
export const textOf = (state: TextState): string =>
	linearize(state.elements)
		.filter((element) => !element.deleted)
		.map((element) => element.value)
		.join('');

/** Merge two text-CRDT states (commutative/idempotent). Pure — for server mutations. */
export const mergeTextState = (a: TextState, b: TextState): TextState => {
	const byId = new Map<string, TextElement>();
	for (const element of [...a.elements, ...b.elements]) {
		const existing = byId.get(element.id);
		byId.set(
			element.id,
			existing === undefined
				? element
				: { ...existing, deleted: existing.deleted || element.deleted }
		);
	}
	return { elements: [...byId.values()] };
};

/** The RGA text CRDT — {@link CrdtText} plus direct positional edits. */
export type TextCrdt = CrdtText<TextState> & {
	/** Insert `value` at visible `index`. */
	insert: (index: number, value: string) => void;
	/** Tombstone `count` visible characters from `index`. */
	delete: (index: number, count: number) => void;
};

/**
 * A collaborative text buffer backed by an RGA sequence CRDT. Concurrent inserts
 * and deletes from different replicas merge without conflict and converge. Drive
 * it from an input via {@link TextCrdt.setText}; persist/broadcast
 * {@link TextCrdt.state}; apply remote state via {@link TextCrdt.merge}.
 */
export const createTextCrdt = (
	replica: string,
	initial?: TextState
): TextCrdt => {
	const elements = new Map<string, TextElement>();
	let clock = 0;
	if (initial !== undefined) {
		for (const element of initial.elements) {
			elements.set(element.id, element);
			clock = Math.max(clock, element.clock);
		}
	}

	const visible = () =>
		linearize([...elements.values()]).filter((element) => !element.deleted);

	const insert = (index: number, value: string) => {
		const seen = visible();
		let after = index <= 0 ? null : (seen[index - 1]?.id ?? null);
		for (const char of [...value]) {
			clock += 1;
			const element: TextElement = {
				id: `${replica}:${clock}`,
				replica,
				clock,
				after,
				value: char,
				deleted: false
			};
			elements.set(element.id, element);
			after = element.id;
		}
	};

	const remove = (index: number, count: number) => {
		const seen = visible();
		for (let offset = 0; offset < count; offset += 1) {
			const target = seen[index + offset];
			if (target !== undefined) {
				elements.set(target.id, { ...target, deleted: true });
			}
		}
	};

	return {
		text: () => textOf({ elements: [...elements.values()] }),
		insert,
		delete: remove,
		merge: (state) => {
			for (const element of state.elements) {
				const existing = elements.get(element.id);
				elements.set(
					element.id,
					existing === undefined
						? element
						: {
								...existing,
								deleted: existing.deleted || element.deleted
							}
				);
				clock = Math.max(clock, element.clock);
			}
		},
		// Reconcile to `next` by editing only the changed middle: keep the common
		// prefix/suffix, delete the old middle, insert the new — so two clients
		// typing in different places merge instead of overwriting.
		setText: (next) => {
			const current = textOf({ elements: [...elements.values()] });
			if (current === next) {
				return;
			}
			let prefix = 0;
			const maxPrefix = Math.min(current.length, next.length);
			while (prefix < maxPrefix && current[prefix] === next[prefix]) {
				prefix += 1;
			}
			let suffix = 0;
			while (
				suffix < maxPrefix - prefix &&
				current[current.length - 1 - suffix] ===
					next[next.length - 1 - suffix]
			) {
				suffix += 1;
			}
			const removed = current.length - prefix - suffix;
			if (removed > 0) {
				remove(prefix, removed);
			}
			const inserted = next.slice(prefix, next.length - suffix);
			if (inserted.length > 0) {
				insert(prefix, inserted);
			}
		},
		state: () => ({ elements: [...elements.values()] })
	};
};

/**
 * The first-party collaborative-text backend (the RGA above) packaged as a
 * {@link TextCrdtAdapter}. Zero dependencies. Use it directly, or swap in an
 * adapter from `sync-adapters` (e.g. `@absolutejs/sync-yjs`) for the same shape.
 */
export const rgaText: TextCrdtAdapter<TextState> = {
	create: createTextCrdt,
	empty: () => ({ elements: [] }),
	merge: mergeTextState,
	textOf
};
