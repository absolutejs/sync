/**
 * `@absolutejs/sync/mcp` — MCP server bindings for a {@link SyncEngine}.
 * Surfaces the engine's read + mutate APIs as MCP tools that Claude
 * Code / Cursor / any MCP client can call directly.
 *
 * The MCP SDK (`@modelcontextprotocol/sdk`) is an OPTIONAL peer; this
 * subpath module loads it lazily so apps that don't use MCP pay
 * nothing.
 */

export {
	createSyncMcpServer,
	serveStdio,
	type SyncMcpServer,
	type SyncMcpServerOptions
} from './server';
