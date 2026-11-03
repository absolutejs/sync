import type { Change } from '@absolutejs/changelog';

export const change: Change = {
	detail: 'Sandboxed mutation handlers take `requireLockdown`, passed through to isolated-jsc 0.13+. On the Worker backend, stock Bun lets a handler reach the `Bun` global through indirect eval and load any module with `import()`; AbsoluteJS patched Bun 1.4.4+absolute.2 closes both with `lockdown()`. With `requireLockdown: true`, the handler refuses to run on a Worker isolate that is not locked down and throws `LockdownUnavailableError` instead. Default `false`, so nothing changes unless you set it. The `@absolutejs/isolated-jsc` peer range now allows 0.13; older versions ignore the option.',
	kind: 'added',
	summary: 'Sandboxed handlers can require a locked-down isolate',
	symbols: ['SandboxConfig']
};
