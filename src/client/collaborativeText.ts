import { createSyncCollection } from './syncCollection';
import type { SyncCollectionStatus } from './syncCollection';
import { createTextCrdt } from '../crdt';
import type { CrdtText, TextState } from '../crdt';
import type { RowKey } from '../engine/types';

/**
 * Options for a live collaborative-text binding. It subscribes to a sync
 * collection, tracks one row's CRDT field, and merges remote edits into a local
 * replica — so the visible text reflects everyone's edits and converges. Edits
 * are sent through the engine's auto-registered `"<collection>:merge"` mutation
 * (override with `mutation`), which merges instead of overwriting.
 */
export type CollaborativeTextOptions<State = TextState> = {
	/** The sync WebSocket URL (same one your collections use). */
	url: string;
	/** Collection (and table) name holding the document rows. */
	collection: string;
	/** Which row to edit (its key value). */
	id: RowKey;
	/** The row field holding the CRDT state. */
	field: string;
	/** The row's key field name (defaults to `"id"`). */
	keyField?: string;
	/** Mutation to send edits through (defaults to `"<collection>:merge"`). */
	mutation?: string;
	/** This client's replica id (defaults to a random UUID). */
	replica?: string;
	/**
	 * CRDT backend factory (defaults to the first-party RGA `createTextCrdt`).
	 * Pass e.g. `(replica) => createYjsText(replica)` from `@absolutejs/sync-yjs`
	 * — it must match the backend the server registered for this field.
	 */
	create?: (replica: string) => CrdtText<State>;
};

export type CollaborativeTextState = {
	/** The current merged, visible text. */
	text: string;
	/** The underlying collection's connection status. */
	status: SyncCollectionStatus;
};

export type CollaborativeText = {
	get: () => CollaborativeTextState;
	subscribe: (run: (state: CollaborativeTextState) => void) => () => void;
	/** Reconcile the local text to `next` and broadcast the merged state. */
	setText: (next: string) => void;
	close: () => void;
};

/**
 * Framework-agnostic controller behind the `useCollaborativeText` bindings. Opens
 * a {@link createSyncCollection}, so create it on the client only (the framework
 * wrappers do this in an effect / on mount).
 */
export const createCollaborativeText = <State = TextState>(
	options: CollaborativeTextOptions<State>
): CollaborativeText => {
	const keyField = options.keyField ?? 'id';
	const mutation = options.mutation ?? `${options.collection}:merge`;
	const replica = options.replica ?? globalThis.crypto.randomUUID();
	const make =
		options.create ??
		((id: string) => createTextCrdt(id) as unknown as CrdtText<State>);
	const crdt = make(replica);

	let current: CollaborativeTextState = { status: 'connecting', text: '' };
	const subscribers = new Set<(state: CollaborativeTextState) => void>();
	const emit = () => {
		for (const run of subscribers) {
			run(current);
		}
	};

	const collection = createSyncCollection<Record<string, unknown>>({
		collection: options.collection,
		key: (row) => row[keyField] as RowKey,
		url: options.url
	});

	const apply = (state: {
		data: Record<string, unknown>[];
		status: SyncCollectionStatus;
	}) => {
		let { text } = current;
		const row = state.data.find(
			(candidate) => candidate[keyField] === options.id
		);
		const fieldState = row?.[options.field];
		if (fieldState !== undefined) {
			// Idempotent for our own echoes; folds in other replicas' edits.
			crdt.merge(fieldState as State);
			text = crdt.text();
		}
		current = { status: state.status, text };
		emit();
	};
	apply(collection.get());
	const unsubscribe = collection.subscribe(apply);

	return {
		get: () => current,
		subscribe(run) {
			subscribers.add(run);
			run(current);

			return () => {
				subscribers.delete(run);
			};
		},
		setText(next) {
			crdt.setText(next);
			current = { status: current.status, text: next };
			emit();
			void collection.mutate({
				args: { [keyField]: options.id, [options.field]: crdt.state() },
				name: mutation
			});
		},
		close() {
			unsubscribe();
			collection.close();
			subscribers.clear();
		}
	};
};
