import { extractKeyFromWhere, keyTopic, tableTopic } from './topics';
import type { PrismaWhere } from './topics';

export type DeriveReadTopicsOptions = {
	/**
	 * Primary-key field name to narrow on. Defaults to `id` (Prisma's
	 * convention); set it for models keyed by another field.
	 */
	keyField?: string;
};

export type DerivedReadTopics = {
	/** Topics this read depends on — subscribe to all of them. */
	topics: string[];
	/**
	 * `true` when derivation narrowed to a specific row (`model:key`); `false`
	 * when it fell back to the whole-model topic.
	 */
	rowLevel: boolean;
};

/**
 * Derive the reactive topics a read of `model` (optionally filtered by `where`)
 * depends on. A recognised key-field equality narrows to a single `model:key`
 * topic; everything else subscribes to the whole-model topic.
 *
 * @example
 * deriveReadTopics('user');                       // { topics: ['user'], rowLevel: false }
 * deriveReadTopics('user', { id: 5 });            // { topics: ['user:5'], rowLevel: true }
 * deriveReadTopics('user', { id: { gt: 5 } });    // { topics: ['user'], rowLevel: false }
 */
export const deriveReadTopics = (
	model: string,
	where?: PrismaWhere,
	options: DeriveReadTopicsOptions = {}
): DerivedReadTopics => {
	const keyField = options.keyField ?? 'id';
	const key =
		where === undefined ? undefined : extractKeyFromWhere(where, keyField);

	if (key === undefined) {
		return { topics: [tableTopic(model)], rowLevel: false };
	}
	return { topics: [keyTopic(model, key)], rowLevel: true };
};
