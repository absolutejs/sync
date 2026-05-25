/**
 * Drizzle adapter for @absolutejs/sync (Tier 2 — ORM auto-reactivity).
 *
 * Derives reactive-hub topics from a Drizzle schema so you stop hand-naming
 * them: a read subscribes to the topics it depends on ({@link deriveReadTopics})
 * and a mutation publishes the topics it invalidates ({@link publishChange} and
 * friends). Both speak the same {@link tableTopic}/{@link keyTopic} vocabulary,
 * so reads and writes always line up.
 *
 * Granularity is deliberately coarse and DB-agnostic: table-level by default,
 * narrowing to a single row when a filter is a simple primary-key equality.
 */

export { keyTopic, tableTopic } from './topics';

export { deriveReadTopics } from './read';
export type { DeriveReadTopicsOptions, DerivedReadTopics } from './read';

export { publishChange, publishRows, publishWhere } from './write';
export type {
	ChangeOp,
	ChangePayload,
	PublishChangeOptions,
	PublishRowsOptions,
	PublishWhereOptions
} from './write';
