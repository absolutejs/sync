import type { Change } from '@absolutejs/changelog';

export const change: Change = {
	detail: "The optional `@absolutejs/isolated-jsc` peer range is now `>=0.12.2 <1.0.0`, so a new isolated-jsc minor no longer needs a sync release before apps can install it. Sync uses only isolated-jsc's stable sandbox APIs (`resolveIsolatePolicy`, the isolated runner), and its sandbox tests run against each isolated-jsc release it is bumped to; this one is tested with 0.15.0.",
	kind: 'changed',
	summary: 'Sandboxed handlers accept any isolated-jsc 0.x from 0.12.2'
};
