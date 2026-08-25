import type { SyncMutationRejection } from './reconciliation';

/** Versioned, finite HTTP protocol for service workers and native background work. */
export type HeadlessSyncRequest = {
	version: 1;
	mutations?: HeadlessSyncMutation[];
	pulls?: HeadlessSyncPull[];
};

export type HeadlessSyncMutation = {
	operationId: string;
	name: string;
	args?: unknown;
};

export type HeadlessSyncPull = {
	id: string;
	collection: string;
	params?: unknown;
	since?: number | string;
};

export type HeadlessSyncMutationResult =
	| { operationId: string; status: 'ack'; result?: unknown }
	| {
			operationId: string;
			status: 'reject';
			rejection: SyncMutationRejection;
	  };

export type HeadlessSyncPullResult<T = unknown> =
	| {
			id: string;
			type: 'snapshot';
			rows: T[];
			version: number;
			cursor: string;
	  }
	| {
			id: string;
			type: 'diff';
			added: T[];
			removed: T[];
			changed: T[];
			version: number;
			cursor: string;
	  }
	| { id: string; type: 'error'; message: string };

export type HeadlessSyncResponse = {
	version: 1;
	mutations: HeadlessSyncMutationResult[];
	pulls: HeadlessSyncPullResult[];
};
