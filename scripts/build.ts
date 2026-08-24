import { $ } from 'bun';
import { rm } from 'node:fs/promises';

const DIST = 'dist';

await rm(DIST, { force: true, recursive: true });

// Server / Node-target entries (engine + adapters). Peer deps stay external.
const serverBuild = await Bun.build({
	entrypoints: [
		'src/index.ts',
		'src/manifest.ts',
		'src/platform.ts',
		'src/writeBehindCache.ts',
		'src/scheduled.ts',
		'src/testing.ts',
		'src/codeMode.ts',
		'src/engine/index.ts',
		'src/mcp/index.ts',
		'src/adapters/drizzle/index.ts',
		'src/adapters/prisma/index.ts',
		'src/adapters/postgres/index.ts',
		'src/adapters/mysql/index.ts',
		'src/adapters/sqlite/index.ts'
	],
	external: [
		'elysia',
		'@elysiajs/cron',
		'@sinclair/typebox',
		'drizzle-orm',
		'@absolutejs/isolated-jsc',
		'@modelcontextprotocol/sdk'
	],
	outdir: DIST,
	root: 'src',
	sourcemap: 'linked',
	target: 'bun'
});

if (!serverBuild.success) {
	for (const log of serverBuild.logs) {
		console.error(log);
	}
	process.exit(1);
}

// Browser-target entries (client store + framework primitives). Frameworks stay
// external so each app brings its own copy.
const browserBuild = await Bun.build({
	entrypoints: [
		'src/client/index.ts',
		'src/client/runtimeTransport.ts',
		'src/crdt/index.ts',
		'src/adapters/tanstack-db/index.ts',
		'src/react/index.ts',
		'src/vue/index.ts',
		'src/svelte/index.ts',
		'src/angular/index.ts'
	],
	external: ['react', 'vue', 'svelte', '@angular/core', '@tanstack/db'],
	outdir: DIST,
	root: 'src',
	sourcemap: 'linked',
	target: 'browser'
});

if (!browserBuild.success) {
	for (const log of browserBuild.logs) {
		console.error(log);
	}
	process.exit(1);
}

await $`tsc --emitDeclarationOnly --project tsconfig.build.json`;
await $`absolute-manifest emit`;
