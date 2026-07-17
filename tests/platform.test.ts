import { describe, expect, test } from 'bun:test';
import {
	createPlatformSyncRuntime,
	PLATFORM_SYNC_ENVIRONMENT_KEY,
	PLATFORM_SYNC_HEALTH_PATH,
	PlatformSyncConfigurationError,
	PlatformSyncHealthSchema,
	readPlatformSyncConfiguration,
	type PlatformSyncConfiguration
} from '../src/platform';
import { Value } from '@sinclair/typebox/value';
import type { ClusterBus } from '../src/engine/cluster';
import { defineSyncPack } from '../src/engine/pack';

const configuration: PlatformSyncConfiguration = {
	changeLogRetainMs: 60_000,
	changeLogSize: 1_024,
	closeOnSlow: true,
	heartbeatMs: 25_000,
	instanceId: 'release-1',
	maxBufferedBytes: 1_048_576,
	mutationConcurrency: 8,
	mutationQueueLimit: 100,
	pushPath: '/sync',
	socketPath: '/sync/ws',
	tier: 'both',
	version: 1
};

describe('platform Sync runtime', () => {
	test('reads one bounded environment contract', () => {
		expect(
			readPlatformSyncConfiguration({
				[PLATFORM_SYNC_ENVIRONMENT_KEY]: JSON.stringify(configuration)
			})
		).toEqual(configuration);
		expect(() =>
			readPlatformSyncConfiguration({
				[PLATFORM_SYNC_ENVIRONMENT_KEY]: JSON.stringify({
					...configuration,
					mutationConcurrency: 0
				})
			})
		).toThrow(PlatformSyncConfigurationError);
	});

	test('assembles declared tiers and exposes fixed readiness', async () => {
		const runtime = createPlatformSyncRuntime({ configuration });
		expect(runtime.engine).toBeDefined();
		expect(runtime.hub).toBeDefined();
		const response = await runtime.app.handle(
			new Request(`http://localhost${PLATFORM_SYNC_HEALTH_PATH}`)
		);
		expect(response.status).toBe(200);
		const health = await response.json();
		expect(Value.Check(PlatformSyncHealthSchema, health)).toBe(true);
		expect(health).toEqual({
			cluster: { configured: false, connected: false },
			contract: 1,
			instanceId: 'release-1',
			packs: [],
			ready: true,
			tier: 'both'
		});
	});

	test('owns pack registration, cluster readiness, and teardown', async () => {
		let disconnected = 0;
		const gate = Promise.withResolvers<void>();
		const clusterBus: ClusterBus = {
			publish: () => undefined,
			subscribe: async () => {
				await gate.promise;
				return async () => {
					disconnected += 1;
				};
			}
		};
		const pack = defineSyncPack({
			name: '@absolutejs/sync-pack-test',
			ownsTables: [],
			version: '1.0.0'
		});
		const runtime = createPlatformSyncRuntime({
			clusterBus,
			configuration,
			packs: [pack]
		});
		const pending = await runtime.app.handle(
			new Request(`http://localhost${PLATFORM_SYNC_HEALTH_PATH}`)
		);
		expect(pending.status).toBe(503);
		expect(await pending.json()).toMatchObject({
			cluster: { configured: true, connected: false },
			packs: [{ name: '@absolutejs/sync-pack-test', version: '1.0.0' }],
			ready: false
		});

		gate.resolve();
		await runtime.ready;
		const connected = await runtime.app.handle(
			new Request(`http://localhost${PLATFORM_SYNC_HEALTH_PATH}`)
		);
		expect(connected.status).toBe(200);
		expect(await connected.json()).toMatchObject({
			cluster: { configured: true, connected: true },
			ready: true
		});

		await runtime.dispose();
		await runtime.dispose();
		expect(disconnected).toBe(1);
	});

	test('rejects engine capabilities on the push-only tier', () => {
		expect(() =>
			createPlatformSyncRuntime({
				configuration: { ...configuration, tier: 'push' },
				packs: [
					defineSyncPack({
						name: 'invalid',
						ownsTables: [],
						version: '1.0.0'
					})
				]
			})
		).toThrow(PlatformSyncConfigurationError);
	});
});
