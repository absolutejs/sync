import { createCollaborativeText } from '../client/collaborativeText';
import type {
	CollaborativeText,
	CollaborativeTextOptions,
	CollaborativeTextState
} from '../client/collaborativeText';
import type { TextState } from '../crdt';

/**
 * Svelte binding for a CRDT collaborative-text field. A readable store — `$store`
 * gives the current `{ text, status }`, maintained from the merge of every
 * client's edits — with `setText` (broadcasts the merge) and `destroy` attached.
 * Concurrent edits converge with no clobbering and no custom server mutation.
 *
 * SSR-safe: the socket opens lazily on the first browser subscription.
 */
export const createCollaborativeTextStore = <State = TextState>(
	options: CollaborativeTextOptions<State>
) => {
	let controller: CollaborativeText | null = null;
	let current: CollaborativeTextState = { status: 'connecting', text: '' };
	const subscribers = new Set<(state: CollaborativeTextState) => void>();

	const ensureConnected = () => {
		if (controller !== null || typeof window === 'undefined') {
			return;
		}
		controller = createCollaborativeText<State>(options);
		controller.subscribe((state) => {
			current = state;
			subscribers.forEach((run) => run(current));
		});
	};

	return {
		subscribe(run: (state: CollaborativeTextState) => void) {
			subscribers.add(run);
			ensureConnected();
			run(current);

			return () => {
				subscribers.delete(run);
			};
		},
		setText: (next: string) => controller?.setText(next),
		destroy() {
			controller?.close();
			controller = null;
			subscribers.clear();
		}
	};
};
