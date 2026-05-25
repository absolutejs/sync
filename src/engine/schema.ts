/**
 * Declarative, dependency-free row schemas keyed by table. The engine validates
 * a mutation's writes against the schema before they're persisted (a bad write
 * is rejected, like a permission deny), and lazily **migrates** rows on read —
 * so changing a shape needs no up-front database migration. Field validators are
 * plain `(value) => boolean` functions; the `field` kit covers the common cases.
 */

/** Validates one field's value. Optionality is encoded by the validator (see `field.optional`). */
export type FieldValidator = (value: unknown) => boolean;

export type TableSchema<Row = unknown> = {
	/** Per-field validators. On insert every field is checked; on update, only
	 * the fields present in the payload. Extra fields not listed here are allowed. */
	fields: Record<string, FieldValidator>;
	/** Schema version, for documentation/migration bookkeeping. Default 1. */
	version?: number;
	/** Upcast a stored/raw row to the current shape — applied lazily on reads. */
	migrate?: (row: Row) => Row;
};

/** A `table` → {@link TableSchema} map. */
export type SchemaDefinition = Record<string, TableSchema<any>>;

/**
 * Define table schemas. Identity at runtime (for type inference). Pass to
 * `createSyncEngine({ schemas })` or register with `engine.registerSchema`.
 */
export const defineSchema = <S extends SchemaDefinition>(schemas: S): S =>
	schemas;

const isFiniteNumber = (value: unknown): value is number =>
	typeof value === 'number' && Number.isFinite(value);

/** A small validator kit. Compose with `field.optional` / `field.array` / `field.enum`. */
export const field = {
	string: ((value) => typeof value === 'string') as FieldValidator,
	number: isFiniteNumber as FieldValidator,
	boolean: ((value) => typeof value === 'boolean') as FieldValidator,
	/** Any defined value. */
	any: (() => true) as FieldValidator,
	/** Allow `undefined` (the field may be omitted), else delegate to `inner`. */
	optional:
		(inner: FieldValidator): FieldValidator =>
		(value) =>
			value === undefined || inner(value),
	/** An array whose every element satisfies `inner`. */
	array:
		(inner: FieldValidator): FieldValidator =>
		(value) =>
			Array.isArray(value) && value.every(inner),
	/** One of the given literal values. */
	enum:
		(...values: unknown[]): FieldValidator =>
		(value) =>
			values.includes(value)
};
