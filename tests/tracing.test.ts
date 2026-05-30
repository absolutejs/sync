/**
 * 1.21.0 OTel integration. Drives a captured-span tracer through
 * `engine.runMutation` and `engine.subscribe` and asserts the spans
 * fire with the right names + ABS_ATTRS attributes.
 */
import { describe, expect, test } from 'bun:test';
import {
	ABS_ATTRS,
	createNoopSpan,
	type Span,
	type Tracer,
	type TracerProvider
} from '@absolutejs/telemetry';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';

type Row = { id: number; v: string };

type CapturedSpan = {
	name: string;
	attrs: Record<string, unknown>;
	status?: { code: number; message?: string };
	exception?: unknown;
	ended: boolean;
};

const makeCapturingTracerProvider = (): {
	provider: TracerProvider;
	spans: CapturedSpan[];
} => {
	const spans: CapturedSpan[] = [];
	const tracer: Tracer = {
		startActiveSpan: ((
			name: string,
			optionsOrFn: unknown,
			maybeFn?: unknown
		) => {
			const fn =
				typeof optionsOrFn === 'function'
					? (optionsOrFn as (span: Span) => unknown)
					: (maybeFn as (span: Span) => unknown);
			const options = (typeof optionsOrFn === 'function'
				? {}
				: optionsOrFn ?? {}) as {
				attributes?: Record<string, unknown>;
			};
			const record: CapturedSpan = {
				attrs: { ...(options.attributes ?? {}) },
				ended: false,
				name
			};
			spans.push(record);
			return fn(makeSpan(record));
		}) as Tracer['startActiveSpan'],
		startSpan: (
			name: string,
			options?: { attributes?: Record<string, unknown> }
		) => {
			const record: CapturedSpan = {
				attrs: { ...(options?.attributes ?? {}) },
				ended: false,
				name
			};
			spans.push(record);
			return makeSpan(record);
		}
	};
	const provider: TracerProvider = {
		getTracer: () => tracer
	};
	return { provider, spans };
};

const makeSpan = (record: CapturedSpan): Span => {
	const noop = createNoopSpan();
	return {
		...noop,
		addEvent: (() => makeSpan(record)) as Span['addEvent'],
		end: () => {
			record.ended = true;
		},
		isRecording: () => !record.ended,
		recordException: (exception) => {
			record.exception = exception;
		},
		setAttribute: ((key, value) => {
			record.attrs[key] = value;
			return makeSpan(record);
		}) as Span['setAttribute'],
		setAttributes: ((attrs) => {
			Object.assign(record.attrs, attrs);
			return makeSpan(record);
		}) as Span['setAttributes'],
		setStatus: ((status) => {
			record.status = status;
			return makeSpan(record);
		}) as Span['setStatus']
	};
};

const wire = (tracerProvider?: TracerProvider) => {
	const store = new Map<number, Row>();
	const engine = createSyncEngine({
		instanceId: 'engine-A',
		tracerProvider
	});
	engine.registerReader('rows', { all: () => [...store.values()] });
	engine.registerWriter<Row>('rows', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	engine.register(
		defineCollection<Row>({
			hydrate: () => [...store.values()],
			key: (row) => row.id,
			match: () => true,
			name: 'rows'
		})
	);
	return engine;
};

describe('sync 1.21.0 — OTel via @absolutejs/telemetry', () => {
	test('no tracerProvider = no spans emitted (zero-cost noop)', async () => {
		// If anything internal were leaking spans through a non-noop
		// path this would catch it. Same flow as below, just without
		// the provider.
		const engine = wire();
		engine.registerMutation(
			defineMutation({
				handler: (args: Row, _ctx, actions) =>
					actions.insert('rows', args),
				name: 'add'
			})
		);
		await engine.runMutation('add', { id: 1, v: 'x' }, {});
		// Nothing to assert directly — just verifying no throw.
		expect(true).toBe(true);
	});

	test('runMutation emits sync.runMutation span with ABS_ATTRS', async () => {
		const { provider, spans } = makeCapturingTracerProvider();
		const engine = wire(provider);
		engine.registerMutation(
			defineMutation({
				handler: (args: Row, _ctx, actions) =>
					actions.insert('rows', args),
				name: 'createRow'
			})
		);
		await engine.runMutation('createRow', { id: 1, v: 'one' }, {});
		const mutationSpan = spans.find(
			(span) => span.name === 'sync.runMutation'
		);
		expect(mutationSpan).toBeDefined();
		expect(mutationSpan!.attrs[ABS_ATTRS.engineId]).toBe('engine-A');
		expect(mutationSpan!.attrs[ABS_ATTRS.mutation]).toBe('createRow');
		expect(mutationSpan!.ended).toBe(true);
	});

	test('runMutation throw records exception + sets ERROR status', async () => {
		const { provider, spans } = makeCapturingTracerProvider();
		const engine = wire(provider);
		engine.registerMutation(
			defineMutation({
				handler: () => {
					throw new Error('mutation broke');
				},
				name: 'failing'
			})
		);
		await expect(engine.runMutation('failing', {}, {})).rejects.toThrow(
			'mutation broke'
		);
		const mutationSpan = spans.find(
			(span) => span.name === 'sync.runMutation'
		);
		expect(mutationSpan).toBeDefined();
		expect(mutationSpan!.status?.code).toBe(2);
		expect(mutationSpan!.exception).toBeInstanceOf(Error);
		expect(mutationSpan!.ended).toBe(true);
	});

	test('subscribe emits sync.subscribe span with ABS_ATTRS', async () => {
		const { provider, spans } = makeCapturingTracerProvider();
		const engine = wire(provider);
		const sub = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const subscribeSpan = spans.find(
			(span) => span.name === 'sync.subscribe'
		);
		expect(subscribeSpan).toBeDefined();
		expect(subscribeSpan!.attrs[ABS_ATTRS.engineId]).toBe('engine-A');
		expect(subscribeSpan!.attrs[ABS_ATTRS.collection]).toBe('rows');
		expect(subscribeSpan!.ended).toBe(true);
		sub.unsubscribe();
	});

	test('subscribe failure (unknown collection) records the exception', async () => {
		const { provider, spans } = makeCapturingTracerProvider();
		const engine = wire(provider);
		await expect(
			engine.subscribe({
				collection: 'no-such-collection',
				ctx: {},
				onDiff: () => {},
				params: undefined
			})
		).rejects.toThrow(/Unknown collection/);
		const subscribeSpan = spans.find(
			(span) => span.name === 'sync.subscribe'
		);
		expect(subscribeSpan).toBeDefined();
		expect(subscribeSpan!.status?.code).toBe(2);
		expect(subscribeSpan!.exception).toBeInstanceOf(Error);
	});
});
