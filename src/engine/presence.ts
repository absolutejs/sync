/**
 * Presence — ephemeral, room-scoped state shared over the live socket (who's
 * online, who's typing, cursor positions). Unlike collections it is **not**
 * persisted: it lives only while a member is joined, and a member's state is
 * removed (and peers notified) the moment it leaves or its connection drops.
 *
 * A `room` is any string (a document id, a channel). Each `member` is one
 * participant (typically one connection) with a `state` it owns and updates;
 * everyone in the room sees the member set and its changes.
 */

export type PresenceMember<S = unknown> = { id: string; state: S };

/** What changed in a room: members that joined, updated state, or left. */
export type PresenceDiff<S = unknown> = {
	joined: PresenceMember<S>[];
	updated: PresenceMember<S>[];
	left: string[];
};

export type PresenceHandle<S> = {
	/** The room's members at join time (including this one). */
	members: PresenceMember<S>[];
	/** Replace this member's state and notify the rest of the room. */
	set: (state: S) => void;
	/** Leave the room (remove this member; notify peers). */
	leave: () => void;
};

export type PresenceHub = {
	/**
	 * Join `room` as `memberId` with `state`; `onDiff` receives every later change
	 * to the room (not this member's own join). Returns the current members and
	 * handles to update/leave.
	 */
	join: <S>(
		room: string,
		memberId: string,
		state: S,
		onDiff: (diff: PresenceDiff<S>) => void
	) => PresenceHandle<S>;
	/** Snapshot a room's members without joining. */
	members: <S = unknown>(room: string) => PresenceMember<S>[];
	/** Number of members in a room (0 if none). */
	count: (room: string) => number;
};

type RoomMember = {
	state: unknown;
	onDiff: (diff: PresenceDiff<unknown>) => void;
};

/**
 * Create an in-process presence hub. Transport-agnostic (no socket import): the
 * sync connection wires client `presence-*` frames to it and tears down a
 * connection's memberships on close.
 */
export const createPresenceHub = (): PresenceHub => {
	const rooms = new Map<string, Map<string, RoomMember>>();

	const roomMembers = (room: string): PresenceMember<unknown>[] => {
		const members = rooms.get(room);
		if (members === undefined) {
			return [];
		}
		return [...members].map(([id, member]) => ({
			id,
			state: member.state
		}));
	};

	/** Notify everyone in `room` except the actor that caused the change. */
	const notify = (
		room: string,
		diff: PresenceDiff<unknown>,
		exceptId: string
	) => {
		const members = rooms.get(room);
		if (members === undefined) {
			return;
		}
		for (const [id, member] of members) {
			if (id !== exceptId) {
				member.onDiff(diff);
			}
		}
	};

	return {
		join: (room, memberId, state, onDiff) => {
			let members = rooms.get(room);
			if (members === undefined) {
				members = new Map();
				rooms.set(room, members);
			}
			members.set(memberId, {
				state,
				onDiff: onDiff as (diff: PresenceDiff<unknown>) => void
			});
			// Peers learn this member joined; the joiner gets the snapshot instead.
			notify(
				room,
				{ joined: [{ id: memberId, state }], updated: [], left: [] },
				memberId
			);
			const snapshot = roomMembers(room) as PresenceMember<
				typeof state
			>[];

			return {
				members: snapshot,
				set: (next) => {
					const current = rooms.get(room)?.get(memberId);
					if (current === undefined) {
						return;
					}
					current.state = next;
					notify(
						room,
						{
							joined: [],
							updated: [{ id: memberId, state: next }],
							left: []
						},
						memberId
					);
				},
				leave: () => {
					const roomNow = rooms.get(room);
					if (roomNow?.delete(memberId) !== true) {
						return;
					}
					notify(
						room,
						{ joined: [], updated: [], left: [memberId] },
						memberId
					);
					if (roomNow.size === 0) {
						rooms.delete(room);
					}
				}
			};
		},
		members: (room) => roomMembers(room) as PresenceMember<never>[],
		count: (room) => rooms.get(room)?.size ?? 0
	};
};
