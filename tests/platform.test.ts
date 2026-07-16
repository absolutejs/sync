import { describe, expect, test } from 'bun:test';
import {
	createPlatformSyncRuntime,
	PLATFORM_SYNC_ENVIRONMENT_KEY,
	PLATFORM_SYNC_HEALTH_PATH,
	PlatformSyncConfigurationError,
	readPlatformSyncConfiguration,
	type PlatformSyncConfiguration
} from '../src/platform';

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
		expect(await response.json()).toEqual({
			contract: 1,
			instanceId: 'release-1',
			ready: true,
			tier: 'both'
		});
	});
});
