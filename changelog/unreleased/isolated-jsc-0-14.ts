import type { Change } from '@absolutejs/changelog';

export const change: Change = {
	detail: "The optional `@absolutejs/isolated-jsc` peer range now allows 0.14. With 0.14, the `cpuMs` that sandboxed-handler metrics report is the isolate thread's CPU time instead of the wall-clock duration, so a handler waiting on a host call is no longer charged for the wait.",
	kind: 'changed',
	summary: 'Sandboxed handlers accept isolated-jsc 0.14'
};
