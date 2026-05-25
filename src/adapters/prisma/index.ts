/**
 * Prisma adapter for @absolutejs/sync (Tier 2 — ORM auto-reactivity).
 *
 * The Prisma counterpart of the Drizzle adapter: derive the topics a query
 * depends on ({@link deriveReadTopics}) and publish the topics a mutation
 * invalidates ({@link publishChange} and friends). It identifies a model by name
 * and reads Prisma's plain `where` objects and result records, so it needs no
 * `@prisma/client` import and works on every database Prisma supports.
 *
 * Use the SAME model identifier on the read and write sides — the topic is the
 * model name verbatim (e.g. `'user'` -> topic `user`, row topic `user:5`).
 * Granularity is table-level by default, narrowing to a single row when a filter
 * is a simple key-field equality.
 */

export { keyTopic, tableTopic } from './topics';
export type { PrismaWhere, RowKey } from './topics';

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

export { matchesWhere, UnsupportedFilterError } from './predicate';
export { prismaCollection } from './collection';
export type { PrismaCollectionOptions } from './collection';
