import { describe, expect, test } from 'bun:test';
import { createSyncConnection } from '../src/engine/connection';
import type { ServerFrame } from '../src/engine/connection';
import { createPresenceHub } from '../src/engine/presence';
import type { PresenceDiff } from '../src/engine/presence';
import { createSyncEngine } from '../src/engine/syncEngine';

type Member = { name: string; typing?: boolean };

describe('createPresenceHub', () => {
	test('a joiner sees everyone; peers are told it joined', () => {
		const hub = createPresenceHub();
		const aDiffs: PresenceDiff<Member>[] = [];
		const a = hub.join<Member>('doc', 'a', { name: 'Ada' }, (diff) =>
			aDiffs.push(diff)
		);
		expect(a.members).toEqual([{ id: 'a', state: { name: 'Ada' } }]);

		const b = hub.join<Member>('doc', 'b', { name: 'Bob' }, () => {});
		expect(b.members.map((member) => member.id).sort()).toEqual(['a', 'b']);
		// 'a' learns 'b' joined (but never got an echo of its own join).
		expect(aDiffs).toEqual([
			{
				joined: [{ id: 'b', state: { name: 'Bob' } }],
				updated: [],
				left: []
			}
		]);
	});

	test('set notifies peers; leave removes and notifies', () => {
		const hub = createPresenceHub();
		const aDiffs: PresenceDiff<Member>[] = [];
		hub.join<Member>('doc', 'a', { name: 'Ada' }, (diff) =>
			aDiffs.push(diff)
		);
		const b = hub.join<Member>('doc', 'b', { name: 'Bob' }, () => {});

		b.set({ name: 'Bob', typing: true });
		expect(aDiffs.at(-1)).toEqual({
			joined: [],
			updated: [{ id: 'b', state: { name: 'Bob', typing: true } }],
			left: []
		});

		b.leave();
		expect(aDiffs.at(-1)).toEqual({ joined: [], updated: [], left: ['b'] });
		expect(hub.count('doc')).toBe(1);
	});

	test('rooms are isolated', () => {
		const hub = createPresenceHub();
		const aDiffs: PresenceDiff<Member>[] = [];
		hub.join<Member>('doc1', 'a', { name: 'Ada' }, (diff) =>
			aDiffs.push(diff)
		);
		hub.join<Member>('doc2', 'b', { name: 'Bob' }, () => {});

		expect(aDiffs).toHaveLength(0); // a (doc1) never hears about b (doc2)
		expect(hub.count('doc1')).toBe(1);
		expect(hub.count('doc2')).toBe(1);
	});
});

const presenceFrames = (frames: ServerFrame[]) =>
	frames.filter(
		(frame): frame is Extract<ServerFrame, { type: 'presence' }> =>
			frame.type === 'presence'
	);

describe('presence over a sync connection', () => {
	const setup = () => {
		const engine = createSyncEngine();
		const presence = createPresenceHub();
		const aFrames: ServerFrame[] = [];
		const bFrames: ServerFrame[] = [];
		const connA = createSyncConnection({
			engine,
			ctx: {},
			presence,
			send: (frame) => aFrames.push(frame)
		});
		const connB = createSyncConnection({
			engine,
			ctx: {},
			presence,
			send: (frame) => bFrames.push(frame)
		});

		return { aFrames, bFrames, connA, connB };
	};

	test('join sends a snapshot, and peers get join/set/leave diffs', async () => {
		const { aFrames, bFrames, connA, connB } = setup();

		await connA.handle({
			type: 'presence-join',
			room: 'doc',
			memberId: 'a',
			state: { name: 'Ada' }
		});
		// A's initial snapshot.
		expect(presenceFrames(aFrames).at(-1)).toMatchObject({
			room: 'doc',
			joined: [{ id: 'a', state: { name: 'Ada' } }]
		});

		aFrames.length = 0;
		await connB.handle({
			type: 'presence-join',
			room: 'doc',
			memberId: 'b',
			state: { name: 'Bob' }
		});
		// A is told B joined; B's snapshot has both.
		expect(presenceFrames(aFrames).at(-1)).toMatchObject({
			joined: [{ id: 'b', state: { name: 'Bob' } }]
		});
		expect(
			presenceFrames(bFrames)
				.at(-1)!
				.joined.map((member) => member.id)
				.sort()
		).toEqual(['a', 'b']);

		// B updates (typing) → A sees it.
		aFrames.length = 0;
		await connB.handle({
			type: 'presence-set',
			room: 'doc',
			state: { name: 'Bob', typing: true }
		});
		expect(presenceFrames(aFrames).at(-1)).toMatchObject({
			updated: [{ id: 'b', state: { name: 'Bob', typing: true } }]
		});

		// B's socket closes → A sees B leave (auto-cleanup).
		aFrames.length = 0;
		connB.close();
		expect(presenceFrames(aFrames).at(-1)).toMatchObject({ left: ['b'] });
	});

	test('presence frames error when the socket has no presence hub', async () => {
		const engine = createSyncEngine();
		const frames: ServerFrame[] = [];
		const conn = createSyncConnection({
			engine,
			ctx: {},
			send: (frame) => frames.push(frame)
		});
		await conn.handle({
			type: 'presence-join',
			room: 'doc',
			memberId: 'a',
			state: {}
		});
		expect(frames.at(-1)).toMatchObject({
			type: 'error',
			message: 'Presence is not enabled'
		});
	});
});
