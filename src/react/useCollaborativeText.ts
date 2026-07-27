import { useCallback, useEffect, useRef, useState } from 'react';
import { createCollaborativeText } from '../client/collaborativeText';
import type {
	CollaborativeText,
	CollaborativeTextOptions,
	CollaborativeTextState
} from '../client/collaborativeText';
import type { TextState } from '../crdt';

/**
 * React binding for a CRDT collaborative-text field. Subscribes to the row's
 * field, merges remote edits into a local replica, and returns the merged
 * `text`, a `setText` that broadcasts the merge, and the connection `status`.
 * Concurrent edits from other clients converge with no clobbering and no custom
 * server mutation (the engine's `registerCrdt` auto-registers the merge).
 *
 * SSR-safe: the socket opens in an effect, and re-opens if the target row/field
 * changes; it closes on unmount.
 */
export const useCollaborativeText = <State = TextState>(
	options: CollaborativeTextOptions<State>
) => {
	const [state, setState] = useState<CollaborativeTextState>({
		ready: false,
		status: 'connecting',
		text: ''
	});
	const controllerRef = useRef<CollaborativeText | null>(null);

	useEffect(() => {
		const controller = createCollaborativeText<State>(options);
		controllerRef.current = controller;
		const unsubscribe = controller.subscribe(setState);

		return () => {
			unsubscribe();
			controller.close();
			controllerRef.current = null;
		};
		// Re-open only when the subscription identity changes.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		options.url,
		options.collection,
		options.id,
		options.field,
		options.params
	]);

	const setText = useCallback(
		(next: string) => controllerRef.current?.setText(next),
		[]
	);
	const anchorAt = useCallback(
		(index: number) => controllerRef.current?.anchorAt(index) ?? null,
		[]
	);
	const indexOfAnchor = useCallback(
		(anchor: string | null) =>
			controllerRef.current?.indexOfAnchor(anchor) ?? 0,
		[]
	);

	return {
		anchorAt,
		indexOfAnchor,
		ready: state.ready,
		setText,
		status: state.status,
		text: state.text
	};
};
