/** Why a server refused a mutation. */
export type SyncMutationRejectionKind = 'conflict' | 'permanent' | 'retryable';

/** Serializable rejection metadata carried over the Sync protocol. */
export type SyncMutationRejection = {
	kind: SyncMutationRejectionKind;
	message: string;
	/** Stable application or adapter code suitable for programmatic handling. */
	code?: string;
	/** Explicitly public, serializable conflict/remediation context. */
	details?: unknown;
	/** Server hint for transient failures. Clients still enforce their own ceiling. */
	retryAfterMs?: number;
};

export type SyncMutationRejectionErrorOptions = {
	code?: string;
	details?: unknown;
	retryAfterMs?: number;
	cause?: unknown;
};

/**
 * Throw from a mutation to deliberately classify its client-facing outcome.
 * Details are sent to the client, so callers must never put secrets in them.
 */
export class SyncMutationRejectionError extends Error {
	readonly code: string | undefined;
	readonly details: unknown;
	readonly kind: SyncMutationRejectionKind;
	readonly retryAfterMs: number | undefined;

	constructor(
		kind: SyncMutationRejectionKind,
		message: string,
		options: SyncMutationRejectionErrorOptions = {}
	) {
		super(message, { cause: options.cause });
		this.name = 'SyncMutationRejectionError';
		this.kind = kind;
		this.code = options.code;
		this.details = options.details;
		this.retryAfterMs = options.retryAfterMs;
	}
}

/** Client-side error returned to the caller when an operation is rejected. */
export class SyncMutationRejectedError extends Error {
	readonly operationId: string | undefined;
	readonly rejection: SyncMutationRejection;

	constructor(rejection: SyncMutationRejection, operationId?: string) {
		super(rejection.message);
		this.name = 'SyncMutationRejectedError';
		this.operationId = operationId;
		this.rejection = rejection;
	}
}

/** Convert an arbitrary mutation failure into safe wire metadata. */
export const toSyncMutationRejection = (
	error: unknown
): SyncMutationRejection => {
	if (error instanceof SyncMutationRejectionError) {
		return {
			kind: error.kind,
			message: error.message,
			...(error.code === undefined ? {} : { code: error.code }),
			...(error.details === undefined ? {} : { details: error.details }),
			...(error.retryAfterMs === undefined
				? {}
				: { retryAfterMs: Math.max(0, error.retryAfterMs) })
		};
	}
	return {
		kind: 'permanent',
		message: error instanceof Error ? error.message : String(error)
	};
};
