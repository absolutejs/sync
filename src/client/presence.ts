import type { PresenceMember } from '../engine/presence';

export type { PresenceMember } from '../engine/presence';

export type PresenceClientOptions<S> = {
	/** WebSocket URL of the {@link syncSocket} endpoint (e.g. `ws://host/sync/ws`). */
	url: string;
	/** Presence room to join (e.g. a document id or channel). */
	room: string;
	/** This member's initial state (e.g. `{ name, typing: false }`). */
	state: S;
	/** Stable id for this member; defaults to a random one per client. */
	memberId?: string;
	/** WebSocket implementation; defaults to the global one. */
	webSocketImpl?: typeof WebSocket;
	/** Initial reconnect backoff (ms); doubles per attempt. Defaults to 500. */
	reconnectMs?: number;
	/** Max reconnect backoff (ms). Defaults to 10000. */
	maxReconnectMs?: number;
};

export type PresenceClient<S> = {
	/** This member's id. */
	id: string;
	/** Current members in the room (including this one). */
	get: () => PresenceMember<S>[];
	/** Subscribe to member changes; returns an unsubscribe. */
	subscribe: (listener: (members: PresenceMember<S>[]) => void) => () => void;
	/** Update this member's own state (e.g. set `typing: true`). */
	set: (state: S) => void;
	/** Leave the room and close the socket. */
	close: () => void;
};

/**
 * Browser client for {@link createPresenceHub} presence: join a room, see who's
 * present (and their state — typing, cursor…), and publish your own. Opens its
 * own small socket to the sync endpoint and re-joins on reconnect.
 * Framework-agnostic (`get` + `subscribe`, ready for `useSyncExternalStore`).
 */
export const createPresence = <S>(
	options: PresenceClientOptions<S>
): PresenceClient<S> => {
	const reconnectMs = options.reconnectMs ?? 500;
	const maxReconnectMs = options.maxReconnectMs ?? 10_000;
	const Impl = options.webSocketImpl ?? globalThis.WebSocket;
	if (!Impl) {
		throw new Error(
			'createPresence requires WebSocket. Run in a browser or pass webSocketImpl.'
		);
	}
	const id =
		options.memberId ??
		globalThis.crypto?.randomUUID?.() ??
		`m${Math.random()}`;

	const members = new Map<string, S>();
	let state = options.state;
	let snapshot: PresenceMember<S>[] = [];
	const listeners = new Set<(members: PresenceMember<S>[]) => void>();

	const emit = () => {
		snapshot = [...members].map(([memberId, memberState]) => ({
			id: memberId,
			state: memberState
		}));
		for (const listener of listeners) {
			listener(snapshot);
		}
	};

	let socket: WebSocket | undefined;
	let connected = false;
	let closed = false;
	let attempt = 0;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

	const send = (frame: unknown) => {
		if (connected) {
			socket?.send(JSON.stringify(frame));
		}
	};

	const connect = () => {
		if (closed) {
			return;
		}
		const ws = new Impl(options.url);
		socket = ws;
		ws.onopen = () => {
			attempt = 0;
			connected = true;
			ws.send(
				JSON.stringify({
					type: 'presence-join',
					room: options.room,
					memberId: id,
					state
				})
			);
		};
		ws.onmessage = (event) => {
			let frame: {
				type?: string;
				room?: string;
				joined?: PresenceMember<S>[];
				updated?: PresenceMember<S>[];
				left?: string[];
			};
			try {
				frame = JSON.parse(event.data as string);
			} catch {
				return;
			}
			if (frame.type !== 'presence' || frame.room !== options.room) {
				return;
			}
			for (const member of frame.joined ?? []) {
				members.set(member.id, member.state);
			}
			for (const member of frame.updated ?? []) {
				members.set(member.id, member.state);
			}
			for (const memberId of frame.left ?? []) {
				members.delete(memberId);
			}
			emit();
		};
		ws.onclose = () => {
			connected = false;
			if (closed || reconnectMs <= 0) {
				return;
			}
			const delay = Math.min(reconnectMs * 2 ** attempt, maxReconnectMs);
			attempt += 1;
			reconnectTimer = setTimeout(connect, delay);
		};
	};

	connect();

	return {
		id,
		get: () => snapshot,
		subscribe: (listener) => {
			listeners.add(listener);
			listener(snapshot);

			return () => {
				listeners.delete(listener);
			};
		},
		set: (next) => {
			state = next;
			send({ type: 'presence-set', room: options.room, state: next });
		},
		close: () => {
			closed = true;
			if (reconnectTimer !== undefined) {
				clearTimeout(reconnectTimer);
			}
			send({ type: 'presence-leave', room: options.room });
			socket?.close();
			members.clear();
		}
	};
};
