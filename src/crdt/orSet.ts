/**
 * An observed-remove set (OR-Set) — a CRDT set where concurrent add/remove of an
 * element resolves **add-wins**: each `add` tags the element with a unique tag,
 * and `remove` only retracts the tags it has observed, so a concurrent add (a new
 * tag) survives. State-based: `merge` is union of tags minus removed tags.
 */

const newTag = (): string => globalThis.crypto.randomUUID();
const defaultEquals = <T>(a: T, b: T): boolean => Object.is(a, b);

export type OrSetState<T> = {
	/** Each add: the value plus the unique tag that observed it. */
	adds: { value: T; tag: string }[];
	/** Tags retracted by `remove`. */
	removed: string[];
};

export const orSet = {
	create: <T>(): OrSetState<T> => ({ adds: [], removed: [] }),

	add: <T>(
		state: OrSetState<T>,
		value: T,
		tag = newTag()
	): OrSetState<T> => ({
		adds: [...state.adds, { value, tag }],
		removed: state.removed
	}),

	/** Retract every tag currently observed for `value` (add-wins on re-add). */
	remove: <T>(
		state: OrSetState<T>,
		value: T,
		equals: (a: T, b: T) => boolean = defaultEquals
	): OrSetState<T> => {
		const tags = state.adds
			.filter((entry) => equals(entry.value, value))
			.map((entry) => entry.tag);

		return {
			adds: state.adds,
			removed: [...new Set([...state.removed, ...tags])]
		};
	},

	has: <T>(
		state: OrSetState<T>,
		value: T,
		equals: (a: T, b: T) => boolean = defaultEquals
	): boolean => {
		const removed = new Set(state.removed);

		return state.adds.some(
			(entry) => equals(entry.value, value) && !removed.has(entry.tag)
		);
	},

	/** The live, de-duplicated members. */
	values: <T>(
		state: OrSetState<T>,
		equals: (a: T, b: T) => boolean = defaultEquals
	): T[] => {
		const removed = new Set(state.removed);
		const out: T[] = [];
		for (const entry of state.adds) {
			if (
				!removed.has(entry.tag) &&
				!out.some((value) => equals(value, entry.value))
			) {
				out.push(entry.value);
			}
		}

		return out;
	},

	/** Union the observed tags and the removed tags (commutative/idempotent). */
	merge: <T>(a: OrSetState<T>, b: OrSetState<T>): OrSetState<T> => {
		const byTag = new Map<string, { value: T; tag: string }>();
		for (const entry of [...a.adds, ...b.adds]) {
			byTag.set(entry.tag, entry);
		}

		return {
			adds: [...byTag.values()],
			removed: [...new Set([...a.removed, ...b.removed])]
		};
	}
};
