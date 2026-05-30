import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import {
	createSyncEngine,
	SubscriptionLimitError
} from '../src/engine/syncEngine';

type Row = { id: number; v: string };
type Ctx = { tenantId?: string; system?: boolean };

const wireEngine = (
	max?: number,
	key?: (ctx: unknown, args: { collection: string }) => string | undefined
) => {
	const store = new Map<number, Row>();
	const engine = createSyncEngine(
		max !== undefined && key !== undefined
			? {
					subscriptionLimit: { key, max }
				}
			: {}
	);
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
			authorize: (_params, ctx: Ctx) =>
				ctx?.tenantId !== 'deny',
			hydrate: () => [...store.values()],
			key: (row) => row.id,
			match: () => true,
			name: 'rows'
		})
	);
	return { engine, store };
};

const ctxKey = (ctx: unknown) => (ctx as Ctx)?.tenantId;

describe('subscriptionLimit — 1.20.1', () => {
	test('caps active subscriptions per tenant key', async () => {
		const { engine } = wireEngine(2, ctxKey);
		const subA1 = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined
		});
		const subA2 = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined
		});
		await expect(
			engine.subscribe<Row>({
				collection: 'rows',
				ctx: { tenantId: 'A' },
				onDiff: () => {},
				params: undefined
			})
		).rejects.toThrow(SubscriptionLimitError);

		expect(engine.metrics().subscriptions.byTenant).toEqual({ A: 2 });
		subA1.unsubscribe();
		subA2.unsubscribe();
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
	});

	test('different tenant keys count separately', async () => {
		const { engine } = wireEngine(1, ctxKey);
		const subA = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined
		});
		const subB = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'B' },
			onDiff: () => {},
			params: undefined
		});
		expect(engine.metrics().subscriptions.byTenant).toEqual({ A: 1, B: 1 });
		await expect(
			engine.subscribe<Row>({
				collection: 'rows',
				ctx: { tenantId: 'A' },
				onDiff: () => {},
				params: undefined
			})
		).rejects.toThrow(SubscriptionLimitError);
		subA.unsubscribe();
		subB.unsubscribe();
	});

	test('undefined key skips the cap for that call', async () => {
		const { engine } = wireEngine(1, (ctx) => {
			const t = ctx as Ctx;
			return t?.system ? undefined : t?.tenantId;
		});
		// Five "system" subs — none counted.
		const subs = await Promise.all(
			[1, 2, 3, 4, 5].map(() =>
				engine.subscribe<Row>({
					collection: 'rows',
					ctx: { system: true },
					onDiff: () => {},
					params: undefined
				})
			)
		);
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
		// Tenant-key path STILL gets bounded.
		const tenantSub = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'X' },
			onDiff: () => {},
			params: undefined
		});
		await expect(
			engine.subscribe<Row>({
				collection: 'rows',
				ctx: { tenantId: 'X' },
				onDiff: () => {},
				params: undefined
			})
		).rejects.toThrow(SubscriptionLimitError);
		for (const sub of subs) sub.unsubscribe();
		tenantSub.unsubscribe();
	});

	test('no setting → byTenant stays empty (back-compat)', async () => {
		const { engine } = wireEngine();
		await Promise.all(
			[1, 2, 3, 4, 5].map(() =>
				engine.subscribe<Row>({
					collection: 'rows',
					ctx: { tenantId: 'A' },
					onDiff: () => {},
					params: undefined
				})
			)
		);
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
	});

	test('failed authorize releases the slot — no leak', async () => {
		const { engine } = wireEngine(1, ctxKey);
		await expect(
			engine.subscribe<Row>({
				collection: 'rows',
				ctx: { tenantId: 'deny' },
				onDiff: () => {},
				params: undefined
			})
		).rejects.toThrow(/Not authorized/);
		// 'deny' tenant should NOT show up — the slot was released.
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
		// And a fresh subscribe under the same tenant should succeed.
		const sub = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'deny-but-allowed' },
			onDiff: () => {},
			params: undefined
		});
		sub.unsubscribe();
	});

	test('SubscriptionLimitError carries diagnostic fields', async () => {
		const { engine } = wireEngine(1, ctxKey);
		const subA = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined
		});
		try {
			await engine.subscribe<Row>({
				collection: 'rows',
				ctx: { tenantId: 'A' },
				onDiff: () => {},
				params: undefined
			});
			expect.unreachable();
		} catch (error) {
			expect(error).toBeInstanceOf(SubscriptionLimitError);
			const typed = error as SubscriptionLimitError;
			expect(typed.tenantKey).toBe('A');
			expect(typed.limit).toBe(1);
			expect(typed.active).toBe(1);
		}
		subA.unsubscribe();
	});

	test('aborted signal releases the slot (idempotent unsubscribe)', async () => {
		const { engine } = wireEngine(1, ctxKey);
		const controller = new AbortController();
		const sub = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined,
			signal: controller.signal
		});
		expect(engine.metrics().subscriptions.byTenant).toEqual({ A: 1 });
		// Abort fires the wrapped unsubscribe.
		controller.abort();
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
		// Calling unsubscribe again is a no-op (idempotent).
		sub.unsubscribe();
		expect(engine.metrics().subscriptions.byTenant).toEqual({});
		// A second tenant-A subscribe should now succeed.
		const sub2 = await engine.subscribe<Row>({
			collection: 'rows',
			ctx: { tenantId: 'A' },
			onDiff: () => {},
			params: undefined
		});
		sub2.unsubscribe();
	});
});
