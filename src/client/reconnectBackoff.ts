/**
 * Reconnect delay state shared by every WebSocket client.
 *
 * Opening a socket is not proof that the connection is healthy: a server may
 * accept it, then close it while hydrating or writing the first snapshot. Only
 * a valid server frame proves useful progress and may reset the backoff.
 */
export const createReconnectBackoff = (
	initialMs: number,
	maximumMs: number
) => {
	let attempt = 0;

	return {
		markHealthy() {
			attempt = 0;
		},
		nextDelay() {
			const delay = Math.min(initialMs * 2 ** attempt, maximumMs);
			attempt += 1;

			return delay;
		}
	};
};

export const hasReconnectHealthyFrameType = (
	frame: unknown,
	types: readonly string[]
) =>
	typeof frame === 'object' &&
	frame !== null &&
	'type' in frame &&
	typeof frame.type === 'string' &&
	types.includes(frame.type);
