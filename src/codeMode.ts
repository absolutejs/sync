/**
 * `@absolutejs/sync/code-mode` — wraps engine mutations as Code Mode host
 * tools.
 *
 * The Code Mode pattern (Cloudflare Dynamic Workers / Anthropic
 * programmatic tool calling, both ~2026) replaces "N tool calls per
 * turn" with "1 tool call whose body is JS that chains the underlying
 * fns." Sync's contribution: the engine's mutation surface is the
 * underlying fns. The model writes
 *
 * ```js
 * const c = await runMutation('comments:create', { body: '@bob …', resourceId });
 * await runMutation('comments:toggleReaction', { commentId: c.id, emoji: '👍' });
 * return c.id;
 * ```
 *
 * and three would-be tool turns collapse into one — only the final
 * `return` enters the conversation context.
 *
 * The factory here returns a **host-tool map** that's shape-compatible
 * with `@absolutejs/ai`'s `codeModeTool({ tools })` option. We don't
 * import `@absolutejs/ai` because the consumer is responsible for
 * wiring the two; sync stays decoupled from the AI SDK.
 *
 * ## v0.1 semantics (read carefully)
 *
 * - Each `runMutation` call runs in its own DB transaction (the
 *   engine's per-call retry/transaction wrapper). If mutation 3/5
 *   fails, mutations 1–2 are already committed; the model receives
 *   the error and decides whether to compensate (e.g. by calling a
 *   delete mutation).
 * - Cross-mutation atomicity (all-or-nothing across N runMutations) is
 *   NOT provided here — it would need a new engine batch primitive.
 *   That's a deliberate v0.2 followup; this v0.1 ships the integration
 *   without lying about transactional semantics.
 * - The host-side `ctx` factory runs ONCE per `run_mutations` tool
 *   call; the same ctx is threaded through every mutation in that
 *   script. This matches the "one model turn ≈ one user request"
 *   shape, where the user identity doesn't change mid-script.
 *
 * @example
 * ```ts
 * import { engineMutationsAsHostTools } from '@absolutejs/sync/code-mode';
 * import { codeModeTool } from '@absolutejs/ai/tools';
 *
 * const sessionCtx = () => ({ userId: currentSessionUserId() });
 * const hostTools = engineMutationsAsHostTools({
 *   ctx: sessionCtx,
 *   engine,
 *   mutations: [
 *     {
 *       description: 'Post a comment on a resource.',
 *       name: 'comments:create',
 *       tsSignature:
 *         '(args: { resourceId: string; body: string }) => ' +
 *         'Promise<{ id: string; authorId: string; body: string }>',
 *     },
 *     {
 *       description: 'Toggle a reaction emoji on a comment.',
 *       name: 'comments:toggleReaction',
 *       tsSignature:
 *         '(args: { commentId: string; emoji: string }) => ' +
 *         'Promise<{ added: boolean }>',
 *     },
 *   ],
 * });
 *
 * const aiTool = codeModeTool({ tools: hostTools });
 * ```
 */

import type { SyncEngine } from './engine/syncEngine';

/** One engine mutation surfaced to the model as a host function. */
export type MutationToolDescriptor = {
	/** Engine-registered mutation name (e.g. `'comments:create'`). */
	name: string;
	/**
	 * One-line description shown to the model in the Code Mode prompt.
	 * Should describe *what the mutation does*, not just its inputs.
	 */
	description: string;
	/**
	 * TypeScript signature shown to the model. Default
	 * `'(args: any) => Promise<any>'` — provide a real signature so the
	 * model knows the input shape.
	 */
	tsSignature?: string;
	/**
	 * Override the host-fn name surfaced to the model. Default: the
	 * mutation `name` with `:` replaced by `_` (engine names are not
	 * valid JS identifiers). For `'comments:create'` the model sees
	 * `comments_create` by default.
	 */
	hostFnName?: string;
};

/** Options for {@link engineMutationsAsHostTools}. */
export type EngineMutationsAsHostToolsOptions<Ctx> = {
	/** The engine whose mutations should be exposed. */
	engine: SyncEngine;
	/**
	 * Resolve the per-call ctx — invoked ONCE at the start of each
	 * Code Mode `run_mutations` call and threaded through every
	 * mutation in that script. Typically returns the current
	 * session/user identity. May be sync or async.
	 */
	ctx: () => Ctx | Promise<Ctx>;
	/** The mutation set to expose. */
	mutations: MutationToolDescriptor[];
};

/**
 * Shape of one entry in the host-tool map. Mirrors
 * `@absolutejs/ai/tools`'s `CodeModeHostTool` exactly — no import
 * needed; the AI SDK consumes any record with this shape.
 */
export type CodeModeHostTool = {
	description: string;
	tsSignature: string;
	handler: (...args: unknown[]) => unknown;
};

/** Map of model-visible host-fn name → host tool. */
export type CodeModeHostToolMap = Record<string, CodeModeHostTool>;

const DEFAULT_TS_SIGNATURE = '(args: any) => Promise<any>';

const defaultHostFnName = (mutationName: string): string =>
	mutationName.replace(/[^A-Za-z0-9_]/g, '_');

/**
 * Build a host-tool map that exposes the engine's mutation surface to a
 * Code Mode tool. Pass the result to `codeModeTool({ tools })` from
 * `@absolutejs/ai/tools`.
 *
 * The function throws synchronously at build time if any descriptor
 * names a mutation that isn't registered on the engine, so a typo'd
 * allowlist surfaces at boot, not at the first model call.
 */
export const engineMutationsAsHostTools = <Ctx>(
	options: EngineMutationsAsHostToolsOptions<Ctx>
): CodeModeHostToolMap => {
	const { engine, ctx, mutations } = options;
	const registered = new Set(engine.inspect().mutations);
	const map: CodeModeHostToolMap = {};
	const seenHostFnNames = new Set<string>();

	for (const descriptor of mutations) {
		if (!registered.has(descriptor.name)) {
			throw new Error(
				`engineMutationsAsHostTools: mutation "${descriptor.name}" is not registered on the engine. ` +
					`Register it first, or remove it from the mutations list.`
			);
		}
		const hostFnName =
			descriptor.hostFnName ?? defaultHostFnName(descriptor.name);
		if (seenHostFnNames.has(hostFnName)) {
			throw new Error(
				`engineMutationsAsHostTools: duplicate host-fn name "${hostFnName}". ` +
					`Two mutations map to the same identifier — set hostFnName explicitly on one of them.`
			);
		}
		seenHostFnNames.add(hostFnName);

		map[hostFnName] = {
			description: descriptor.description,
			handler: async (...args: unknown[]): Promise<unknown> => {
				// Code Mode passes positional args from the model's JS call.
				// Engine mutations take a single `args` value — first
				// positional arg is the mutation payload.
				const payload = args[0];
				const resolvedCtx = await ctx();
				return engine.runMutation(
					descriptor.name,
					payload,
					resolvedCtx
				);
			},
			tsSignature: descriptor.tsSignature ?? DEFAULT_TS_SIGNATURE
		};
	}

	return map;
};
