/**
 * `@absolutejs/sync/mcp` — a Model Context Protocol server that surfaces
 * a {@link SyncEngine}'s public read + mutate surface to MCP-aware
 * clients (Claude Code, Cursor, custom agents).
 *
 * The agent gets four tools out of the box:
 *
 *   - `list_collections` — registered collection names + kinds + tables.
 *   - `list_mutations` — registered mutation names.
 *   - `inspect_engine` — full {@link EngineInspection} snapshot
 *     (collections, mutations, schedules, readers, writers, recent
 *     changes).
 *   - `get_snapshot` — `{ collection, params?, ctx? }` → current rows.
 *   - `run_mutation` — `{ mutation, args, ctx? }` → result.
 *
 * Tools that take a `ctx` accept the agent's per-call override; if
 * omitted, the server's `defaultCtx` is used. That's how multi-tenant
 * gating works: the platform spawns one MCP server per tenant with
 * `defaultCtx: { tenantId }`, and the agent can't reach across.
 *
 * ## Why
 *
 * Val.town's MCP server ([val.town/mcp](https://blog.val.town/mcp))
 * exposes its entire UI through 36+ tools — that's the 2026 entry
 * point for "let me run code on a platform." Cloudflare's MCP server
 * does the same for the Cloudflare API, fitting "the entire surface
 * through just two tools in under 1,000 tokens" via Code Mode
 * ([blog.cloudflare.com/dynamic-workers](https://blog.cloudflare.com/dynamic-workers/)).
 *
 * Sync's MCP server is the same play for the sync engine itself: an
 * agent can wire up a real-time leaderboard in three messages, then
 * `run_mutation` to drive it without ever touching source files.
 *
 * ## Usage (stdio)
 *
 * ```ts
 * // mcp-stdio-server.ts — point your MCP client (claude-code, etc.)
 * // at this file's path.
 * import { createSyncEngine, ... } from '@absolutejs/sync/engine';
 * import { createSyncMcpServer, serveStdio } from '@absolutejs/sync/mcp';
 *
 * const engine = createSyncEngine();
 * // ... register your collections / mutations ...
 *
 * const server = createSyncMcpServer({
 *   engine,
 *   defaultCtx: { tenantId: 'demo' },
 * });
 * await serveStdio(server);
 * ```
 *
 * Run it as the MCP server: in your client's MCP config, add an entry
 * like:
 *
 * ```json
 * {
 *   "mcpServers": {
 *     "my-sync": {
 *       "command": "bun",
 *       "args": ["./mcp-stdio-server.ts"]
 *     }
 *   }
 * }
 * ```
 *
 * ## Optional peer dep
 *
 * `@modelcontextprotocol/sdk` is an optional peer — install only if
 * you use the MCP surface. The dynamic `import()` below makes this
 * subpath load-free when not used.
 */

import { z } from 'zod';
import type { SyncEngine } from '../engine/syncEngine';

/** Per-call context default + override behaviour. */
export type SyncMcpServerOptions = {
	/** The engine to expose. */
	engine: SyncEngine;
	/**
	 * Default `ctx` for `get_snapshot` and `run_mutation`. Tools accept
	 * an optional `ctx` override on each call; if absent, this default
	 * is used. Set per-tenant when running one server per tenant.
	 */
	defaultCtx?: unknown;
	/**
	 * Display name advertised to the MCP client. Defaults to
	 * `'sync-engine'`. Useful when one client connects to multiple
	 * sync MCP servers (e.g. `'sync-prod'` / `'sync-staging'`).
	 */
	name?: string;
	/** Semver string surfaced to the MCP client. Defaults to `'0.1.0'`. */
	version?: string;
};

type LoadedMcpSdk = {
	McpServer: typeof import('@modelcontextprotocol/sdk/server/mcp.js').McpServer;
	StdioServerTransport: typeof import('@modelcontextprotocol/sdk/server/stdio.js').StdioServerTransport;
};

let cachedSdk: LoadedMcpSdk | undefined;
const loadMcpSdk = async (): Promise<LoadedMcpSdk> => {
	if (cachedSdk !== undefined) return cachedSdk;
	try {
		const [mcpMod, stdioMod] = await Promise.all([
			import('@modelcontextprotocol/sdk/server/mcp.js'),
			import('@modelcontextprotocol/sdk/server/stdio.js')
		]);
		cachedSdk = {
			McpServer: mcpMod.McpServer,
			StdioServerTransport: stdioMod.StdioServerTransport
		};
		return cachedSdk;
	} catch (error) {
		throw new Error(
			'@absolutejs/sync/mcp requires the optional peer "@modelcontextprotocol/sdk". ' +
				'Install it with: bun add @modelcontextprotocol/sdk',
			{ cause: error }
		);
	}
};

const asTextResult = (value: unknown) => ({
	content: [{ text: JSON.stringify(value, null, 2), type: 'text' as const }]
});

const asError = (message: string) => ({
	content: [{ text: message, type: 'text' as const }],
	isError: true
});

/** A precompiled MCP server with the sync tools already registered. */
export type SyncMcpServer = {
	/** The underlying SDK server — register additional tools on it if you want. */
	server: import('@modelcontextprotocol/sdk/server/mcp.js').McpServer;
	/** Wire to a transport (stdio, sse, custom). The SDK handles the rest. */
	connect: (
		transport: import('@modelcontextprotocol/sdk/shared/transport.js').Transport
	) => Promise<void>;
};

export const createSyncMcpServer = async (
	options: SyncMcpServerOptions
): Promise<SyncMcpServer> => {
	const { McpServer } = await loadMcpSdk();
	const { engine, defaultCtx } = options;
	const server = new McpServer({
		name: options.name ?? 'sync-engine',
		version: options.version ?? '0.1.0'
	});

	const resolveCtx = (callerCtx: unknown): unknown =>
		callerCtx === undefined ? defaultCtx : callerCtx;

	server.registerTool(
		'list_collections',
		{
			description:
				'List every collection registered on this sync engine, with kind, ' +
				'backing tables, and live subscription count.',
			inputSchema: {}
		},
		() => asTextResult(engine.inspect().collections)
	);

	server.registerTool(
		'list_mutations',
		{
			description:
				'List every mutation registered on this sync engine by name.',
			inputSchema: {}
		},
		() => asTextResult(engine.inspect().mutations)
	);

	server.registerTool(
		'inspect_engine',
		{
			description:
				'Full engine inspection snapshot: collections (+kind, tables, ' +
				'subscription counts), mutations, schedules, readers, writers, ' +
				'change-log version, and recent changes. Use this when you need ' +
				'the whole picture in one call.',
			inputSchema: {}
		},
		() => asTextResult(engine.inspect())
	);

	// inputSchema for the read+mutate tools is a ZodRawShape — the
	// MCP SDK's preferred form (it generates JSON Schema for the
	// model from this). `unknown` accepts any JSON value the agent
	// hands us; the engine does its own validation downstream.
	server.registerTool(
		'get_snapshot',
		{
			description:
				'One-shot read of a collection: returns the current rows the ' +
				'authenticated context can see. The collection name must be one ' +
				'returned by list_collections. Params and ctx are JSON values; ' +
				'ctx defaults to the server-configured defaultCtx when omitted.',
			inputSchema: {
				collection: z
					.string()
					.describe('Collection name as registered on the engine.'),
				ctx: z
					.unknown()
					.optional()
					.describe(
						'Optional per-call ctx override. Defaults to the server defaultCtx.'
					),
				params: z
					.unknown()
					.optional()
					.describe(
						'Optional collection params (matches the collection definition).'
					)
			}
		},
		async (args) => {
			try {
				const rows = await engine.hydrate(
					args.collection,
					args.params,
					resolveCtx(args.ctx)
				);
				return asTextResult(rows);
			} catch (err) {
				return asError(
					err instanceof Error
						? `${err.name}: ${err.message}`
						: String(err)
				);
			}
		}
	);

	server.registerTool(
		'run_mutation',
		{
			description:
				'Run a registered mutation by name. Args is the JSON payload the ' +
				'handler receives. Ctx defaults to the server defaultCtx. Returns ' +
				"the handler's result, or an error object if authorization / " +
				'validation fails.',
			inputSchema: {
				args: z
					.unknown()
					.describe('Arguments object passed to the handler.'),
				ctx: z
					.unknown()
					.optional()
					.describe(
						'Optional per-call ctx override. Defaults to the server defaultCtx.'
					),
				mutation: z
					.string()
					.describe('Mutation name as registered on the engine.')
			}
		},
		async (args) => {
			try {
				const result = await engine.runMutation(
					args.mutation,
					args.args,
					resolveCtx(args.ctx)
				);
				return asTextResult(result);
			} catch (err) {
				return asError(
					err instanceof Error
						? `${err.name}: ${err.message}`
						: String(err)
				);
			}
		}
	);

	return {
		connect: (transport) => server.connect(transport),
		server
	};
};

/**
 * Convenience: wire a {@link SyncMcpServer} to stdio. This is the
 * normal path for "I want claude-code to talk to my sync engine."
 *
 * Blocks until the transport closes. The server stays alive across
 * many tool calls.
 */
export const serveStdio = async (
	syncMcp: SyncMcpServer | Promise<SyncMcpServer>
): Promise<void> => {
	const { StdioServerTransport } = await loadMcpSdk();
	const resolved = await syncMcp;
	const transport = new StdioServerTransport();
	await resolved.connect(transport);
};
