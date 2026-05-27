/**
 * `@absolutejs/sync/mcp` smoke tests. We don't spin up an actual MCP
 * transport here — that's a manual integration target. Instead we
 * exercise the server builder + the underlying tool callbacks by
 * driving the `McpServer.callTool` path the SDK exposes.
 */

import { describe, expect, test } from 'bun:test';
import { defineCollection } from '../src/engine/collection';
import { defineMutation } from '../src/engine/mutation';
import { createSyncEngine } from '../src/engine/syncEngine';
import { createSyncMcpServer } from '../src/mcp/server';

type Item = { id: number; n: number };

const itemsCollection = (store: Map<number, Item>) =>
	defineCollection<Item>({
		hydrate: () => [...store.values()],
		key: (row) => row.id,
		match: () => true,
		name: 'items'
	});

const makeEngine = () => {
	const store = new Map<number, Item>();
	const engine = createSyncEngine();
	engine.registerReader('items', { all: async () => [...store.values()] });
	engine.registerWriter<Item>('items', {
		delete: async (row) => {
			store.delete((row as Item).id);
		},
		insert: async (data) => {
			const row = data as Item;
			store.set(row.id, row);
			return row;
		},
		update: async (data) => {
			const row = data as Item;
			store.set(row.id, row);
			return row;
		}
	});
	engine.register(itemsCollection(store));
	engine.registerMutation(
		defineMutation({
			handler: async (_args, _ctx, actions) =>
				actions.insert<Item>('items', { id: 1, n: 42 }),
			name: 'addOne'
		})
	);
	return { engine, store };
};

const parseTextResult = (result: unknown): unknown => {
	const r = result as {
		content: Array<{ text?: string; type: string }>;
	};
	const first = r.content?.[0];
	if (first?.type !== 'text' || typeof first.text !== 'string') {
		throw new Error('not a text result');
	}
	try {
		return JSON.parse(first.text);
	} catch {
		return first.text;
	}
};

const isErrorResult = (result: unknown): boolean => {
	const r = result as { isError?: boolean };
	return r.isError === true;
};

describe('createSyncMcpServer', () => {
	test('exposes list_collections / list_mutations / inspect_engine', async () => {
		const { engine } = makeEngine();
		const mcp = await createSyncMcpServer({
			engine,
			name: 'test',
			version: '0.0.1'
		});
		// The McpServer SDK exposes a `_registeredTools` map (internal but
		// reliable for smoke tests). Confirm the four tools we expect are
		// registered.
		const reg = (
			mcp.server as unknown as {
				_registeredTools: Record<string, unknown>;
			}
		)._registeredTools;
		expect(Object.keys(reg).sort()).toEqual([
			'get_snapshot',
			'inspect_engine',
			'list_collections',
			'list_mutations',
			'run_mutation'
		]);
	});

	test('list_collections returns engine.inspect().collections', async () => {
		const { engine } = makeEngine();
		const mcp = await createSyncMcpServer({ engine });
		const tool = (
			mcp.server as unknown as {
				_registeredTools: Record<
					string,
					{
						handler: (
							args: unknown,
							extra: unknown
						) => Promise<unknown>;
					}
				>;
			}
		)._registeredTools.list_collections;
		const result = await tool!.handler({}, {});
		const parsed = parseTextResult(result) as Array<{ name: string }>;
		expect(parsed.length).toBe(1);
		expect(parsed[0]!.name).toBe('items');
	});

	test('run_mutation invokes the engine and returns the handler result', async () => {
		const { engine, store } = makeEngine();
		const mcp = await createSyncMcpServer({ engine });
		const tool = (
			mcp.server as unknown as {
				_registeredTools: Record<
					string,
					{
						handler: (
							args: unknown,
							extra: unknown
						) => Promise<unknown>;
					}
				>;
			}
		)._registeredTools.run_mutation;
		const result = await tool!.handler(
			{ args: {}, mutation: 'addOne' },
			{}
		);
		const parsed = parseTextResult(result);
		expect(parsed).toEqual({ id: 1, n: 42 });
		// And the store actually got the row.
		expect(store.get(1)).toEqual({ id: 1, n: 42 });
	});

	test('run_mutation surfaces unknown-mutation errors as isError', async () => {
		const { engine } = makeEngine();
		const mcp = await createSyncMcpServer({ engine });
		const tool = (
			mcp.server as unknown as {
				_registeredTools: Record<
					string,
					{
						handler: (
							args: unknown,
							extra: unknown
						) => Promise<unknown>;
					}
				>;
			}
		)._registeredTools.run_mutation;
		const result = await tool!.handler(
			{ args: {}, mutation: 'doesNotExist' },
			{}
		);
		expect(isErrorResult(result)).toBe(true);
	});

	test('get_snapshot uses defaultCtx when no ctx override is supplied', async () => {
		const { engine, store } = makeEngine();
		store.set(7, { id: 7, n: 100 });
		const mcp = await createSyncMcpServer({
			defaultCtx: { tenantId: 'demo' },
			engine
		});
		const tool = (
			mcp.server as unknown as {
				_registeredTools: Record<
					string,
					{
						handler: (
							args: unknown,
							extra: unknown
						) => Promise<unknown>;
					}
				>;
			}
		)._registeredTools.get_snapshot;
		const result = await tool!.handler(
			{ collection: 'items' },
			{}
		);
		const parsed = parseTextResult(result) as Array<{ id: number }>;
		expect(parsed.length).toBe(1);
		expect(parsed[0]!.id).toBe(7);
	});
});
