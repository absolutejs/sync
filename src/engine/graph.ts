import type { AggregateGroup } from './aggregate';
import type { CollectionContext } from './collection';
import {
	aggregateOp,
	filterOp,
	joinNode,
	mapOp,
	materialize,
	orderByOp
} from './dataflow';
import type { Change, JoinNode, Operator } from './dataflow';
import type { RowChange, RowKey, ViewDiff } from './types';

/**
 * Declarative incremental queries — the front door to the operator graph. Build a
 * pipeline with {@link query} (`source → filter → map → join → groupBy`); the
 * engine instantiates it per subscription, hydrates each source, and routes each
 * table's changes through the wired operators, emitting result diffs.
 */

/** A table this query reads. */
export type GraphSource<Row, P = void, Ctx = CollectionContext> = {
	table: string;
	hydrate: (params: P, ctx: Ctx) => Promise<Iterable<Row>> | Iterable<Row>;
	key: (row: Row) => RowKey;
	/** Scope incremental changes (a row that fails it leaves). */
	match?: (row: Row, params: P, ctx: Ctx) => boolean;
};

export type JoinOptions<Left, Right, Out> = {
	/** Join value on the left (current) stream. */
	on: (left: Left) => RowKey;
	/** Join value on the right source. */
	rightOn: (right: Right) => RowKey;
	/** Combine a matched pair. */
	select: (left: Left, right: Right) => Out;
	/** Output row identity (unique per pair). */
	key: (out: Out) => RowKey;
};

export type GroupByOptions<Row> = {
	/** Input row identity (to track contributions). */
	key: (row: Row) => RowKey;
	groupBy?: (row: Row) => RowKey;
	value?: (row: Row) => number;
};

export type OrderByQueryOptions<Row> = {
	/** Row identity. */
	key: (row: Row) => RowKey;
	/** Sort comparator (ascending). */
	compare: (a: Row, b: Row) => number;
	/** Keep at most this many rows (top-N). */
	limit?: number;
	/** Skip this many from the front (pagination). */
	offset?: number;
};

/** A live, instantiated graph for one subscription. */
export type GraphInstance<Out> = {
	tables: string[];
	hydrate: () => Promise<Out[]>;
	applyChange: (table: string, change: RowChange<unknown>) => ViewDiff<Out>;
};

// `any` throughout the internals: a graph chains heterogeneously-typed stages;
// the public Query/GraphSource surface stays fully typed.
type AnyStep =
	| { kind: 'filter'; predicate: (row: any, p: any, ctx: any) => boolean }
	| {
			kind: 'map';
			transform: (row: any) => any;
			rekey?: (row: any) => RowKey;
	  }
	| ({ kind: 'join'; right: GraphSource<any, any, any> } & JoinOptions<
			any,
			any,
			any
	  >)
	| ({ kind: 'aggregate' } & GroupByOptions<any>)
	| ({ kind: 'orderBy' } & OrderByQueryOptions<any>);

type AnySource = GraphSource<any, any, any>;

export type Query<Row, P = void, Ctx = CollectionContext> = {
	filter: (
		predicate: (row: Row, params: P, ctx: Ctx) => boolean
	) => Query<Row, P, Ctx>;
	map: <Out>(
		transform: (row: Row) => Out,
		rekey?: (row: Out) => RowKey
	) => Query<Out, P, Ctx>;
	join: <Right, Out>(
		right: GraphSource<Right, P, Ctx>,
		options: JoinOptions<Row, Right, Out>
	) => Query<Out, P, Ctx>;
	groupBy: (options: GroupByOptions<Row>) => Query<AggregateGroup, P, Ctx>;
	orderBy: (options: OrderByQueryOptions<Row>) => Query<Row, P, Ctx>;
	/** Source tables this query reads. */
	tables: () => string[];
	/** Instantiate the graph for one subscription's params/ctx. */
	instantiate: (params: P, ctx: Ctx) => GraphInstance<Row>;
};

type Stage =
	| { kind: 'op'; op: Operator<any, any> }
	| { kind: 'join'; node: JoinNode<any, any, any>; right: AnySource };

const instantiate = (
	source: AnySource,
	steps: AnyStep[],
	params: any,
	ctx: any
): GraphInstance<any> => {
	const stages: Stage[] = [];
	let currentKey: (row: any) => RowKey = source.key;

	for (const step of steps) {
		if (step.kind === 'filter') {
			const predicate = step.predicate;
			stages.push({
				kind: 'op',
				op: filterOp((row) => predicate(row, params, ctx))
			});
		} else if (step.kind === 'map') {
			stages.push({
				kind: 'op',
				op: mapOp(step.transform, step.rekey)
			});
			if (step.rekey) {
				currentKey = step.rekey;
			}
		} else if (step.kind === 'join') {
			stages.push({
				kind: 'join',
				node: joinNode({
					leftKey: currentKey,
					rightKey: step.right.key,
					leftOn: step.on,
					rightOn: step.rightOn,
					select: step.select,
					key: step.key
				}),
				right: step.right
			});
			currentKey = step.key;
		} else if (step.kind === 'aggregate') {
			stages.push({
				kind: 'op',
				op: aggregateOp({
					key: step.key,
					groupBy: step.groupBy,
					value: step.value
				})
			});
			currentKey = (group: AggregateGroup) => group.group;
		} else {
			stages.push({
				kind: 'op',
				op: orderByOp({
					key: step.key,
					compare: step.compare,
					limit: step.limit,
					offset: step.offset
				})
			});
			// orderBy preserves rows (and their identity), just windows them.
			currentKey = step.key;
		}
	}

	const sink = materialize<any>(currentKey);

	const propagate = (
		changes: Change<any>[],
		fromStage: number,
		side: 'left' | 'right'
	): Change<any>[] => {
		let cs = changes;
		for (let i = fromStage; i < stages.length; i += 1) {
			const stage = stages[i]!;
			if (stage.kind === 'join') {
				cs =
					i === fromStage && side === 'right'
						? stage.node.pushRight(cs)
						: stage.node.pushLeft(cs);
			} else {
				cs = stage.op.push(cs);
			}
		}
		return cs;
	};

	// table -> the entry points its changes feed (root left + each join's right).
	type Entry = {
		stageIndex: number;
		side: 'left' | 'right';
		key: (row: any) => RowKey;
		match?: (row: any, p: any, ctx: any) => boolean;
	};
	const entries = new Map<string, Entry[]>();
	const addEntry = (table: string, entry: Entry) => {
		const list = entries.get(table);
		if (list === undefined) {
			entries.set(table, [entry]);
		} else {
			list.push(entry);
		}
	};
	addEntry(source.table, {
		stageIndex: 0,
		side: 'left',
		key: source.key,
		match: source.match
	});
	stages.forEach((stage, index) => {
		if (stage.kind === 'join') {
			addEntry(stage.right.table, {
				stageIndex: index,
				side: 'right',
				key: stage.right.key,
				match: stage.right.match
			});
		}
	});

	const toChange = (
		entry: Entry,
		change: RowChange<unknown>
	): Change<any> => {
		const key = entry.key(change.row);
		if (
			change.op === 'delete' ||
			(entry.match !== undefined && !entry.match(change.row, params, ctx))
		) {
			return { op: 'delete', key, row: change.row };
		}
		return { op: 'upsert', key, row: change.row };
	};

	return {
		tables: [
			source.table,
			...steps
				.filter(
					(step): step is AnyStep & { kind: 'join' } =>
						step.kind === 'join'
				)
				.map((step) => step.right.table)
		],
		hydrate: async () => {
			// Prime each join's right index first, then push the root through.
			for (let i = 0; i < stages.length; i += 1) {
				const stage = stages[i]!;
				if (stage.kind === 'join') {
					const rightRows = [
						...(await stage.right.hydrate(params, ctx))
					];
					propagate(
						rightRows.map((row) => ({
							op: 'upsert' as const,
							key: stage.right.key(row),
							row
						})),
						i,
						'right'
					);
				}
			}
			const rootRows = [...(await source.hydrate(params, ctx))];
			const out = propagate(
				rootRows.map((row) => ({
					op: 'upsert' as const,
					key: source.key(row),
					row
				})),
				0,
				'left'
			);
			sink.apply(out);
			return sink.rows();
		},
		applyChange: (table, change) => {
			const list = entries.get(table);
			if (list === undefined) {
				return { added: [], removed: [], changed: [] };
			}
			const out: Change<any>[] = [];
			for (const entry of list) {
				out.push(
					...propagate(
						[toChange(entry, change)],
						entry.stageIndex,
						entry.side
					)
				);
			}
			return sink.apply(out);
		}
	};
};

const makeQuery = <Row, P, Ctx>(
	source: AnySource,
	steps: AnyStep[]
): Query<Row, P, Ctx> => ({
	filter: (predicate) =>
		makeQuery(source, [...steps, { kind: 'filter', predicate }]),
	map: (transform, rekey) =>
		makeQuery(source, [...steps, { kind: 'map', transform, rekey }]),
	join: (right, options) =>
		makeQuery(source, [...steps, { kind: 'join', right, ...options }]),
	groupBy: (options) =>
		makeQuery(source, [...steps, { kind: 'aggregate', ...options }]),
	orderBy: (options) =>
		makeQuery(source, [...steps, { kind: 'orderBy', ...options }]),
	tables: () => [
		source.table,
		...steps
			.filter(
				(step): step is AnyStep & { kind: 'join' } =>
					step.kind === 'join'
			)
			.map((step) => step.right.table)
	],
	instantiate: (params, ctx) =>
		instantiate(source, steps, params, ctx) as GraphInstance<Row>
});

/** Start a query from a source table. */
export const query = <Row, P = void, Ctx = CollectionContext>(
	source: GraphSource<Row, P, Ctx>
): Query<Row, P, Ctx> => makeQuery(source, []);

/** A collection backed by an incremental operator graph (see {@link query}). */
export type GraphCollectionDefinition<
	Out,
	P = void,
	Ctx = CollectionContext
> = {
	name: string;
	kind: 'graph';
	query: Query<Out, P, Ctx>;
	authorize?: (params: P, ctx: Ctx) => boolean | Promise<boolean>;
	/** Output row identity (used by the engine/transport to key result rows). */
	key: (out: Out) => RowKey;
};

/** Define a collection whose result is maintained by an operator graph. */
export const defineGraphCollection = <Out, P = void, Ctx = CollectionContext>(
	definition: Omit<GraphCollectionDefinition<Out, P, Ctx>, 'kind'>
): GraphCollectionDefinition<Out, P, Ctx> => ({ ...definition, kind: 'graph' });
