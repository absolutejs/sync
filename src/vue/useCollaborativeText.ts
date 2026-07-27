import { onMounted, onUnmounted, ref } from 'vue';
import { createCollaborativeText } from '../client/collaborativeText';
import type {
	CollaborativeText,
	CollaborativeTextOptions
} from '../client/collaborativeText';
import type { SyncCollectionStatus } from '../client/syncCollection';
import type { TextState } from '../crdt';

/**
 * Vue composable for a CRDT collaborative-text field. Returns reactive `text` and
 * `status` refs (maintained from the merge of every client's edits) plus a
 * `setText` that broadcasts the merge. Concurrent edits converge with no
 * clobbering and no custom server mutation.
 *
 * SSR-safe: the socket opens in `onMounted` and closes in `onUnmounted` (or via
 * the returned `destroy`).
 */
export const useCollaborativeText = <State = TextState>(
	options: CollaborativeTextOptions<State>
) => {
	const text = ref('');
	const status = ref<SyncCollectionStatus>('connecting');
	const ready = ref(false);

	let controller: CollaborativeText | null = null;
	let unsubscribe: (() => void) | null = null;

	onMounted(() => {
		controller = createCollaborativeText<State>(options);
		unsubscribe = controller.subscribe((state) => {
			text.value = state.text;
			status.value = state.status;
			ready.value = state.ready;
		});
	});

	const destroy = () => {
		unsubscribe?.();
		controller?.close();
		unsubscribe = null;
		controller = null;
	};

	onUnmounted(destroy);

	const setText = (next: string) => controller?.setText(next);
	const anchorAt = (index: number) => controller?.anchorAt(index) ?? null;
	const indexOfAnchor = (anchor: string | null) =>
		controller?.indexOfAnchor(anchor) ?? 0;

	return { anchorAt, destroy, indexOfAnchor, ready, setText, status, text };
};
