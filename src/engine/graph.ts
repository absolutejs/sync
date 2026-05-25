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
	/**
	 * Provide to make this a LEFT join: output for a left row with no matching
	 * right (e.g. a user with zero orders). Omit for an inner join.
	 */
	selectUnmatched?: (left: Left) => Out;
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
type AnySource = GraphSource<any, any, any>;

type Plan = { source: AnySource; steps: AnyStep[] };

type AnyStep =
	| { kind: 'filter'; predicate: (row: any, p: any, ctx: any) => boolean }
	| {
			kind: 'map';
			transform: (row: any) => any;
			rekey?: (row: any) => RowKey;
	  }
	// A join's right input is itself a (sub)query plan — a base table is just a
	// plan with no steps, so a join can combine two derived streams.
	| ({ kind: 'join'; rightPlan: Plan } & JoinOptions<any, any, any>)
	| ({ kind: 'aggregate' } & GroupByOptions<any>)
	| ({ kind: 'orderBy' } & OrderByQueryOptions<any>);

/** Plan behind each Query, so a Query can be passed as a join's right input. */
const PLANS = new WeakMap<object, Plan>();

const planTables = (plan: Plan): string[] => {
	const tables = [plan.source.table];
	for (const step of plan.steps) {
		if (step.kind === 'join') {
			tables.push(...planTables(step.rightPlan));
		}
	}
	return [...new Set(tables)];
};

export type Query<Row, P = void, Ctx = CollectionContext> = {
	filter: (
		predicate: (row: Row, params: P, ctx: Ctx) => boolean
	) => Query<Row, P, Ctx>;
	map: <Out>(
		transform: (row: Row) => Out,
		rekey?: (row: Out) => RowKey
	) => Query<Out, P, Ctx>;
	join: <Right, Out>(
		right: GraphSource<Right, P, Ctx> | Query<Right, P, Ctx>,
		options: JoinOptions<Row, Right, Out>
	) => Query<Out, P, Ctx>;
	/**
	 * LEFT join: like {@link join} but keeps left rows with no match, emitting
	 * `selectUnmatched(left)` for them (required, so the intent is explicit).
	 */
	leftJoin: <Right, Out>(
		right: GraphSource<Right, P, Ctx> | Query<Right, P, Ctx>,
		options: JoinOptions<Row, Right, Out> & {
			selectUnmatched: (left: Row) => Out;
		}
	) => Query<Out, P, Ctx>;
	groupBy: (options: GroupByOptions<Row>) => Query<AggregateGroup, P, Ctx>;
	orderBy: (options: OrderByQueryOptions<Row>) => Query<Row, P, Ctx>;
	/** Source tables this query reads. */
	tables: () => string[];
	/** Instantiate the graph for one subscription's params/ctx. */
	instantiate: (params: P, ctx: Ctx) => GraphInstance<Row>;
};

/** A graph that emits a change stream (no materialization) — recursive: a join's
 * right is itself a StreamGraph, so subqueries nest. */
type StreamGraph = {
	tables: string[];
	outKey: (row: any) => RowKey;
	hydrateStream: () => Promise<Change<any>[]>;
	applyStream: (table: string, change: RowChange<unknown>) => Change<any>[];
};

type Stage =
	| { kind: 'op'; op: Operator<any, any> }
	| { kind: 'join'; node: JoinNode<any, any, any>; right: StreamGraph };

/** How a table's change enters the graph (root's left, or a join's right input). */
type Entry = {
	stageIndex: number;
	side: 'left' | 'right';
	produce: (change: RowChange<unknown>) => Change<any>[];
};

const instantiateStream = (
	source: AnySource,
	steps: AnyStep[],
	params: any,
	ctx: any
): StreamGraph => {
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
			stages.push({ kind: 'op', op: mapOp(step.transform, step.rekey) });
			if (step.rekey) {
				currentKey = step.rekey;
			}
		} else if (step.kind === 'join') {
			const right = instantiateStream(
				step.rightPlan.source,
				step.rightPlan.steps,
				params,
				ctx
			);
			stages.push({
				kind: 'join',
				node: joinNode({
					leftKey: currentKey,
					rightKey: right.outKey,
					leftOn: step.on,
					rightOn: step.rightOn,
					select: step.select,
					selectUnmatched: step.selectUnmatched,
					key: step.key
				}),
				right
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

	const sourceChange = (change: RowChange<unknown>): Change<any> => {
		const key = source.key(change.row);
		if (
			change.op === 'delete' ||
			(source.match !== undefined &&
				!source.match(change.row, params, ctx))
		) {
			return { op: 'delete', key, row: change.row };
		}
		return { op: 'upsert', key, row: change.row };
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
	// The root source feeds the left of stage 0.
	addEntry(source.table, {
		stageIndex: 0,
		side: 'left',
		produce: (change) => [sourceChange(change)]
	});
	// Each join's right subgraph feeds that join's right; route every sub-table.
	stages.forEach((stage, index) => {
		if (stage.kind === 'join') {
			for (const table of stage.right.tables) {
				addEntry(table, {
					stageIndex: index,
					side: 'right',
					produce: (change) =>
						(stage as { right: StreamGraph }).right.applyStream(
							table,
							change
						)
				});
			}
		}
	});

	return {
		tables: planTables({ source, steps }),
		outKey: currentKey,
		hydrateStream: async () => {
			// Prime each join's right (recursively hydrating its subgraph) first.
			for (let i = 0; i < stages.length; i += 1) {
				const stage = stages[i]!;
				if (stage.kind === 'join') {
					propagate(await stage.right.hydrateStream(), i, 'right');
				}
			}
			const rootRows = [...(await source.hydrate(params, ctx))];
			return propagate(
				rootRows.map((row) => ({
					op: 'upsert' as const,
					key: source.key(row),
					row
				})),
				0,
				'left'
			);
		},
		applyStream: (table, change) => {
			const list = entries.get(table);
			if (list === undefined) {
				return [];
			}
			const out: Change<any>[] = [];
			for (const entry of list) {
				out.push(
					...propagate(
						entry.produce(change),
						entry.stageIndex,
						entry.side
					)
				);
			}
			return out;
		}
	};
};

const instantiate = (
	source: AnySource,
	steps: AnyStep[],
	params: any,
	ctx: any
): GraphInstance<any> => {
	const graph = instantiateStream(source, steps, params, ctx);
	const sink = materialize<any>(graph.outKey);
	return {
		tables: graph.tables,
		hydrate: async () => {
			sink.apply(await graph.hydrateStream());
			return sink.rows();
		},
		applyChange: (table, change) =>
			sink.apply(graph.applyStream(table, change))
	};
};

const makeQuery = <Row, P, Ctx>(
	source: AnySource,
	steps: AnyStep[]
): Query<Row, P, Ctx> => {
	// `right` is a base source or a sub-Query; normalize to a plan, then append a
	// join step (inner or left — `selectUnmatched` in `options` decides).
	const addJoin = <Right, Out>(
		right: GraphSource<Right, P, Ctx> | Query<Right, P, Ctx>,
		options: JoinOptions<Row, Right, Out>
	): Query<Out, P, Ctx> => {
		const rightPlan = PLANS.get(right as object) ?? {
			source: right as AnySource,
			steps: []
		};
		return makeQuery(source, [
			...steps,
			{ kind: 'join', rightPlan, ...options }
		]);
	};
	const queryInstance: Query<Row, P, Ctx> = {
		filter: (predicate) =>
			makeQuery(source, [...steps, { kind: 'filter', predicate }]),
		map: (transform, rekey) =>
			makeQuery(source, [...steps, { kind: 'map', transform, rekey }]),
		join: (right, options) => addJoin(right, options),
		leftJoin: (right, options) => addJoin(right, options),
		groupBy: (options) =>
			makeQuery(source, [...steps, { kind: 'aggregate', ...options }]),
		orderBy: (options) =>
			makeQuery(source, [...steps, { kind: 'orderBy', ...options }]),
		tables: () => planTables({ source, steps }),
		instantiate: (params, ctx) =>
			instantiate(source, steps, params, ctx) as GraphInstance<Row>
	};
	PLANS.set(queryInstance, { source, steps });
	return queryInstance;
};

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
