import type { Change } from '@absolutejs/changelog';

export const change: Change = {
	detail: "Elysia 2.0.0-rc.0 renamed every lifecycle hook to its `on` form and removed `setup`/`cleanup`. sync's peer ranges (`elysia ^2.0.0-beta.6`, `@elysia/cron ^2.0.0-beta.1`) accepted it untested. sync now builds and tests against Elysia 2.0.0-rc.0 and requires `^2.0.0-rc.0` for both; the plugins whose types carry Elysia's are listed below. No sync API changed shape.",
	kind: 'breaking',
	migration: {
		instruction:
			'Upgrade `elysia` and every `@elysia/*` package (including `@elysia/cron`) to 2.0.0-rc.0 or later 2.x, then rename lifecycle hooks in your app: request → onRequest, beforeHandle → onBeforeHandle, afterHandle → onAfterHandle, afterResponse → onAfterResponse, error → onError, parse → onParse, transform → onTransform; setup/cleanup → onStart/onStop.',
		manual: true
	},
	summary: 'Requires Elysia 2.0.0-rc.0 and @elysia/cron 2.0.0-rc.0',
	symbols: [
		'createPlatformSyncRuntime',
		'scheduled',
		'syncCdc',
		'syncDevtools',
		'syncSocket'
	]
};
