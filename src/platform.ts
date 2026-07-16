import { Type, type Static } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import { Elysia } from 'elysia';
import {
	createSyncEngine,
	type SyncEngine,
	type SyncEngineOptions
} from './engine/syncEngine';
import { syncSocket, type SyncSocketOptions } from './engine/socket';
import { sync, type SyncPluginOptions } from './plugin';
import { createReactiveHub, type ReactiveHub } from './reactiveHub';

export const PLATFORM_SYNC_ENVIRONMENT_KEY = 'ABSOLUTE_SYNC_RUNTIME';
export const PLATFORM_SYNC_HEALTH_PATH = '/.well-known/absolute/sync';

export const PlatformSyncConfigurationSchema = Type.Object(
	{
		changeLogRetainMs: Type.Integer({
			maximum: 604_800_000,
			minimum: 1_000
		}),
		changeLogSize: Type.Integer({ maximum: 10_000, minimum: 1 }),
		closeOnSlow: Type.Boolean(),
		heartbeatMs: Type.Integer({ maximum: 120_000, minimum: 1_000 }),
		instanceId: Type.String({
			maxLength: 128,
			minLength: 1,
			pattern: '^[A-Za-z0-9_.:-]+$'
		}),
		maxBufferedBytes: Type.Integer({
			maximum: 16_777_216,
			minimum: 65_536
		}),
		mutationConcurrency: Type.Integer({ maximum: 64, minimum: 1 }),
		mutationQueueLimit: Type.Integer({ maximum: 10_000, minimum: 0 }),
		pushPath: Type.String({
			maxLength: 128,
			minLength: 1,
			pattern: '^/[^?#]*$'
		}),
		socketPath: Type.String({
			maxLength: 128,
			minLength: 1,
			pattern: '^/[^?#]*$'
		}),
		tier: Type.Union([
			Type.Literal('push'),
			Type.Literal('engine'),
			Type.Literal('both')
		]),
		version: Type.Literal(1)
	},
	{ additionalProperties: false }
);

export type PlatformSyncConfiguration = Static<
	typeof PlatformSyncConfigurationSchema
>;

export class PlatformSyncConfigurationError extends Error {}

export const readPlatformSyncConfiguration = (
	environment: Record<string, string | undefined> = process.env
): PlatformSyncConfiguration | null => {
	const serialized = environment[PLATFORM_SYNC_ENVIRONMENT_KEY]?.trim();
	if (!serialized) return null;
	let parsed: unknown;
	try {
		parsed = JSON.parse(serialized);
	} catch {
		throw new PlatformSyncConfigurationError(
			'Platform Sync configuration is not valid JSON'
		);
	}
	if (!Value.Check(PlatformSyncConfigurationSchema, parsed))
		throw new PlatformSyncConfigurationError(
			'Platform Sync configuration is invalid'
		);

	return parsed;
};

export type PlatformSyncRuntimeOptions = {
	configuration?: PlatformSyncConfiguration;
	engineOptions?: SyncEngineOptions;
	hub?: ReactiveHub;
	onSlow?: SyncSocketOptions['onSlow'];
	resolveContext?: SyncSocketOptions['resolveContext'];
	resolveTopics?: SyncPluginOptions['resolveTopics'];
};

export const createPlatformSyncRuntime = (
	options: PlatformSyncRuntimeOptions = {}
) => {
	const configuration =
		options.configuration ?? readPlatformSyncConfiguration();
	if (!configuration)
		throw new PlatformSyncConfigurationError(
			'Platform Sync is not configured'
		);
	const pushEnabled = configuration.tier !== 'engine';
	const engineEnabled = configuration.tier !== 'push';
	const hub = pushEnabled ? (options.hub ?? createReactiveHub()) : undefined;
	const engine: SyncEngine | undefined = engineEnabled
		? createSyncEngine({
				...options.engineOptions,
				changeLogRetainMs: configuration.changeLogRetainMs,
				changeLogSize: configuration.changeLogSize,
				instanceId: configuration.instanceId,
				mutationConcurrency: configuration.mutationConcurrency,
				mutationQueueLimit: configuration.mutationQueueLimit
			})
		: undefined;
	const app = new Elysia({ name: '@absolutejs/sync/platform' })
		.use(
			hub
				? sync({
						heartbeatMs: configuration.heartbeatMs,
						hub,
						path: configuration.pushPath,
						...(options.resolveTopics
							? { resolveTopics: options.resolveTopics }
							: {})
					})
				: new Elysia({ name: '@absolutejs/sync/platform/no-push' })
		)
		.use(
			engine
				? syncSocket({
						closeOnSlow: configuration.closeOnSlow,
						engine,
						maxBufferedBytes: configuration.maxBufferedBytes,
						path: configuration.socketPath,
						...(options.onSlow ? { onSlow: options.onSlow } : {}),
						...(options.resolveContext
							? { resolveContext: options.resolveContext }
							: {})
					})
				: new Elysia({ name: '@absolutejs/sync/platform/no-engine' })
		)
		.get(PLATFORM_SYNC_HEALTH_PATH, () => ({
			contract: 1 as const,
			instanceId: configuration.instanceId,
			ready: true as const,
			tier: configuration.tier
		}));

	return {
		app,
		configuration,
		engine,
		hub,
		metrics: () => engine?.metrics() ?? null
	};
};
