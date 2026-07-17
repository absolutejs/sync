import { Type, type Static } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import { Elysia } from 'elysia';
import {
	createSyncEngine,
	type SyncEngine,
	type SyncEngineOptions
} from './engine/syncEngine';
import type { ClusterBus } from './engine/cluster';
import type { SyncPack } from './engine/pack';
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
	clusterBus?: ClusterBus;
	configuration?: PlatformSyncConfiguration;
	engineOptions?: SyncEngineOptions;
	hub?: ReactiveHub;
	onSlow?: SyncSocketOptions['onSlow'];
	packs?: SyncPack[];
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
	if (!engine && (options.clusterBus || options.packs?.length))
		throw new PlatformSyncConfigurationError(
			'Platform Sync packs and cluster buses require the engine or both tier'
		);
	for (const pack of options.packs ?? []) engine?.registerPack(pack);
	let clusterState: 'disabled' | 'connecting' | 'connected' | 'error' =
		options.clusterBus ? 'connecting' : 'disabled';
	let clusterError: unknown;
	let disconnectCluster: (() => Promise<void>) | undefined;
	const ready = options.clusterBus
		? engine!
				.connectCluster(options.clusterBus)
				.then((disconnect) => {
					disconnectCluster = disconnect;
					clusterState = 'connected';
				})
				.catch((error: unknown) => {
					clusterError = error;
					clusterState = 'error';
					throw error;
				})
		: Promise.resolve();
	void ready.catch(() => undefined);
	let disposed = false;
	const dispose = async () => {
		if (disposed) return;
		disposed = true;
		try {
			await ready;
		} finally {
			await disconnectCluster?.();
			disconnectCluster = undefined;
		}
	};
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
		.get(PLATFORM_SYNC_HEALTH_PATH, ({ set }) => {
			const isReady =
				clusterState !== 'connecting' && clusterState !== 'error';
			if (!isReady) set.status = 503;
			return {
				cluster: {
					configured: clusterState !== 'disabled',
					connected: clusterState === 'connected',
					...(clusterError instanceof Error
						? { error: clusterError.message }
						: {})
				},
				contract: 1 as const,
				instanceId: configuration.instanceId,
				packs: engine?.inspect().packs ?? [],
				ready: isReady,
				tier: configuration.tier
			};
		});

	return {
		app,
		configuration,
		dispose,
		engine,
		hub,
		metrics: () => engine?.metrics() ?? null,
		ready
	};
};
