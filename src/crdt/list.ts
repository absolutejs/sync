/**
 * An ordered list CRDT — the same RGA sequence type as the collaborative text,
 * but over arbitrary items instead of characters. Concurrent inserts and deletes
 * at any position merge without conflict and converge. State-based, with delta
 * support (`takeDelta`) so a client uploads only its new ops.
 */

export type ListElement<T> = {
	id: string;
	replica: string;
	clock: number;
	after: string | null;
	value: T;
	deleted: boolean;
};

export type ListState<T> = { elements: ListElement<T>[] };

const order = <T>(a: ListElement<T>, b: ListElement<T>) => {
	if (a.clock !== b.clock) {
		return b.clock - a.clock;
	}
	if (a.replica === b.replica) {
		return 0;
	}
	return a.replica > b.replica ? -1 : 1;
};

const linearize = <T>(elements: ListElement<T>[]): ListElement<T>[] => {
	const present = new Set(elements.map((element) => element.id));
	const children = new Map<string | null, ListElement<T>[]>();
	for (const element of elements) {
		const anchor =
			element.after !== null && !present.has(element.after)
				? null
				: element.after;
		const list = children.get(anchor);
		if (list === undefined) {
			children.set(anchor, [element]);
		} else {
			list.push(element);
		}
	}
	for (const list of children.values()) {
		list.sort(order);
	}
	const ordered: ListElement<T>[] = [];
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

/** The visible items of a list-CRDT state. Pure. */
export const listOf = <T>(state: ListState<T>): T[] =>
	linearize(state.elements)
		.filter((element) => !element.deleted)
		.map((element) => element.value);

/** Merge two list-CRDT states (commutative/idempotent). Pure. */
export const mergeListState = <T>(
	a: ListState<T>,
	b: ListState<T>
): ListState<T> => {
	const byId = new Map<string, ListElement<T>>();
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

export type ListCrdt<T> = {
	/** The current visible items. */
	list: () => T[];
	/** Insert `items` at visible `index`. */
	insert: (index: number, items: T[]) => void;
	/** Tombstone `count` visible items from `index`. */
	delete: (index: number, count: number) => void;
	/** Merge another replica's state in. */
	merge: (state: ListState<T>) => void;
	/** The full serializable state (for hydration). */
	state: () => ListState<T>;
	/** The locally-authored ops since the last call, then clears the buffer. */
	takeDelta: () => ListState<T>;
};

/** Create a live ordered-list CRDT for `replica`. */
export const createList = <T>(
	replica: string,
	initial?: ListState<T>
): ListCrdt<T> => {
	const elements = new Map<string, ListElement<T>>();
	const pending = new Map<string, ListElement<T>>();
	let clock = 0;
	if (initial !== undefined) {
		for (const element of initial.elements) {
			elements.set(element.id, element);
			clock = Math.max(clock, element.clock);
		}
	}

	const visible = () =>
		linearize([...elements.values()]).filter((element) => !element.deleted);

	return {
		list: () => listOf({ elements: [...elements.values()] }),
		insert: (index, items) => {
			const seen = visible();
			let after = index <= 0 ? null : (seen[index - 1]?.id ?? null);
			for (const value of items) {
				clock += 1;
				const element: ListElement<T> = {
					id: `${replica}:${clock}`,
					replica,
					clock,
					after,
					value,
					deleted: false
				};
				elements.set(element.id, element);
				pending.set(element.id, element);
				after = element.id;
			}
		},
		delete: (index, count) => {
			const seen = visible();
			for (let offset = 0; offset < count; offset += 1) {
				const target = seen[index + offset];
				if (target !== undefined) {
					const tombstoned = { ...target, deleted: true };
					elements.set(target.id, tombstoned);
					pending.set(target.id, tombstoned);
				}
			}
		},
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
		state: () => ({ elements: [...elements.values()] }),
		takeDelta: () => {
			const delta = { elements: [...pending.values()] };
			pending.clear();

			return delta;
		}
	};
};
