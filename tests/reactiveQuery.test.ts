import { describe, expect, test } from 'bun:test';
import { defineMutation } from '../src/engine/mutation';
import { defineReactiveQuery } from '../src/engine/reactive';
import { createSyncEngine } from '../src/engine/syncEngine';
import type { ViewDiff } from '../src/engine/types';

type Message = { id: number; room: string; text: string };
type User = { id: number; name: string };

const makeStore = () => ({
	messages: new Map<number, Message>(),
	users: new Map<number, User>()
});

const collect = <T>() => {
	const diffs: ViewDiff<T>[] = [];
	return {
		diffs,
		onDiff: (diff: ViewDiff<T>) => {
			diffs.push(diff);
		}
	};
};

describe('reactive queries (read-set tracking)', () => {
	test('re-runs when a table it read changes, and diffs the result', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerReactive(
			defineReactiveQuery<Message, { room: string }>({
				name: 'roomMessages',
				key: (message) => message.id,
				// A plain query: read the table, filter in JS. No `match`.
				run: async ({ db, params }) => {
					const all = await db.all<Message>('messages');
					return all.filter(
						(message) => message.room === params.room
					);
				}
			})
		);

		store.messages.set(1, { id: 1, room: 'a', text: 'hi' });
		store.messages.set(2, { id: 2, room: 'b', text: 'other' });

		const { diffs, onDiff } = collect<Message>();
		const sub = await engine.subscribe<Message, { room: string }>({
			collection: 'roomMessages',
			params: { room: 'a' },
			ctx: {},
			onDiff
		});
		expect(sub.initial).toEqual([{ id: 1, room: 'a', text: 'hi' }]);

		// A new message in room 'a' lands in the store, then the change fires.
		store.messages.set(3, { id: 3, room: 'a', text: 'yo' });
		await engine.applyChange('messages', {
			op: 'insert',
			row: { id: 3, room: 'a', text: 'yo' }
		});
		expect(diffs.at(-1)?.added).toEqual([{ id: 3, room: 'a', text: 'yo' }]);

		// A message in another room changes the table but not this query's result.
		const before = diffs.length;
		store.messages.set(4, { id: 4, room: 'b', text: 'nope' });
		await engine.applyChange('messages', {
			op: 'insert',
			row: { id: 4, room: 'b', text: 'nope' }
		});
		expect(diffs.length).toBe(before); // re-ran, but the diff was empty

		// Editing an existing message re-runs and reports a change.
		store.messages.set(1, { id: 1, room: 'a', text: 'edited' });
		await engine.applyChange('messages', {
			op: 'update',
			row: { id: 1, room: 'a', text: 'edited' }
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 1, room: 'a', text: 'edited' }
		]);
	});

	test('does not re-run on a change to a table it never read', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerReactive(
			defineReactiveQuery<Message>({
				name: 'allMessages',
				key: (message) => message.id,
				run: ({ db }) => db.all<Message>('messages')
			})
		);
		const { diffs, onDiff } = collect<Message>();
		await engine.subscribe<Message>({
			collection: 'allMessages',
			params: undefined,
			ctx: {},
			onDiff
		});

		await engine.applyChange('users', {
			op: 'insert',
			row: { id: 1, name: 'Ada' }
		});
		expect(diffs).toHaveLength(0);
	});

	test('tracks every table read, so a change to either re-runs (a join)', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerReader('users', {
			all: () => [...store.users.values()],
			get: (key) => store.users.get(Number(key))
		});
		type Enriched = { id: number; text: string; author: string };
		engine.registerReactive(
			defineReactiveQuery<Enriched>({
				name: 'messagesWithAuthor',
				key: (row) => row.id,
				run: async ({ db }) => {
					const messages = await db.all<Message & { userId: number }>(
						'messages'
					);
					return Promise.all(
						messages.map(async (message) => {
							const author = await db.get<User>(
								'users',
								message.userId
							);
							return {
								id: message.id,
								text: message.text,
								author: author?.name ?? '?'
							};
						})
					);
				}
			})
		);

		store.users.set(1, { id: 1, name: 'Ada' });
		store.messages.set(1, {
			id: 1,
			room: 'a',
			text: 'hi',
			userId: 1
		} as Message & { userId: number });

		const { diffs, onDiff } = collect<Enriched>();
		const sub = await engine.subscribe<Enriched>({
			collection: 'messagesWithAuthor',
			params: undefined,
			ctx: {},
			onDiff
		});
		expect(sub.initial).toEqual([{ id: 1, text: 'hi', author: 'Ada' }]);

		// Renaming the USER re-runs even though only the users table changed.
		store.users.set(1, { id: 1, name: 'Ada Lovelace' });
		await engine.applyChange('users', {
			op: 'update',
			row: { id: 1, name: 'Ada Lovelace' }
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 1, text: 'hi', author: 'Ada Lovelace' }
		]);
	});

	test('the full loop: write through a mutation, no match, no manual emit', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerWriter('messages', {
			insert: (data: Message) => {
				store.messages.set(data.id, data);

				return data;
			},
			update: (data: Message) => {
				store.messages.set(data.id, data);

				return data;
			},
			delete: (row: { id: number }) => {
				store.messages.delete(row.id);
			}
		});
		engine.registerReactive(
			defineReactiveQuery<Message, { room: string }>({
				name: 'roomMessages',
				key: (message) => message.id,
				run: async ({ db, params }) => {
					const all = await db.all<Message>('messages');
					return all.filter(
						(message) => message.room === params.room
					);
				}
			})
		);
		engine.registerMutation(
			defineMutation({
				name: 'post',
				handler: (args: Message, _ctx, actions) =>
					actions.insert('messages', args)
			})
		);

		const { diffs, onDiff } = collect<Message>();
		await engine.subscribe<Message, { room: string }>({
			collection: 'roomMessages',
			params: { room: 'a' },
			ctx: {},
			onDiff
		});

		await engine.runMutation('post', { id: 1, room: 'a', text: 'hi' }, {});

		// The write persisted and the reactive query went live — with no `match`
		// and no `actions.change`.
		expect(store.messages.get(1)).toEqual({ id: 1, room: 'a', text: 'hi' });
		expect(diffs.at(-1)?.added).toEqual([{ id: 1, room: 'a', text: 'hi' }]);
	});

	test('key-level precision: a get-only query re-runs only for the row it read', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('users', {
			all: () => [...store.users.values()],
			get: (key) => store.users.get(Number(key)),
			key: (row) => (row as User).id
		});
		engine.registerReactive(
			defineReactiveQuery<User, { id: number }>({
				name: 'oneUser',
				key: (user) => user.id,
				run: async ({ db, params }) => {
					const found = await db.get<User>('users', params.id);
					return found ? [found] : [];
				}
			})
		);

		store.users.set(5, { id: 5, name: 'Ada' });
		store.users.set(9, { id: 9, name: 'Bob' });

		const { diffs, onDiff } = collect<User>();
		await engine.subscribe<User, { id: number }>({
			collection: 'oneUser',
			params: { id: 5 },
			ctx: {},
			onDiff
		});

		// A change to a DIFFERENT row (user 9) must NOT re-run this query.
		store.users.set(9, { id: 9, name: 'Bobby' });
		await engine.applyChange('users', {
			op: 'update',
			row: { id: 9, name: 'Bobby' }
		});
		expect(diffs).toHaveLength(0);

		// A change to the row it read (user 5) re-runs.
		store.users.set(5, { id: 5, name: 'Ada Lovelace' });
		await engine.applyChange('users', {
			op: 'update',
			row: { id: 5, name: 'Ada Lovelace' }
		});
		expect(diffs.at(-1)?.changed).toEqual([
			{ id: 5, name: 'Ada Lovelace' }
		]);
	});

	test('range precision: db.where re-runs only for rows in (or leaving) its range', async () => {
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()],
			key: (row) => (row as Message).id
		});
		engine.registerReactive(
			defineReactiveQuery<Message, { room: string }>({
				name: 'roomMessages',
				key: (message) => message.id,
				run: ({ db, params }) =>
					db.where<Message>(
						'messages',
						(message) => message.room === params.room
					)
			})
		);

		store.messages.set(1, { id: 1, room: 'a', text: 'hi' });
		const { diffs, onDiff } = collect<Message>();
		await engine.subscribe<Message, { room: string }>({
			collection: 'roomMessages',
			params: { room: 'a' },
			ctx: {},
			onDiff
		});

		// A message in room 'b' is outside the range → no re-run at all.
		store.messages.set(2, { id: 2, room: 'b', text: 'other' });
		await engine.applyChange('messages', {
			op: 'insert',
			row: { id: 2, room: 'b', text: 'other' }
		});
		expect(diffs).toHaveLength(0);

		// A message entering room 'a' → re-runs, added.
		store.messages.set(3, { id: 3, room: 'a', text: 'yo' });
		await engine.applyChange('messages', {
			op: 'insert',
			row: { id: 3, room: 'a', text: 'yo' }
		});
		expect(diffs.at(-1)?.added).toEqual([{ id: 3, room: 'a', text: 'yo' }]);

		// Message 1 LEAVES room 'a' (now room 'b'): it was a range member, so the
		// query still re-runs even though the new row no longer matches → removed.
		store.messages.set(1, { id: 1, room: 'b', text: 'hi' });
		await engine.applyChange('messages', {
			op: 'update',
			row: { id: 1, room: 'b', text: 'hi' }
		});
		expect(diffs.at(-1)?.removed.map((row) => row.id)).toEqual([1]);
	});

	test('reading a table with no registered reader throws', async () => {
		const engine = createSyncEngine();
		engine.registerReactive(
			defineReactiveQuery<Message>({
				name: 'broken',
				key: (message) => message.id,
				run: ({ db }) => db.all<Message>('messages')
			})
		);
		await expect(
			engine.subscribe<Message>({
				collection: 'broken',
				params: undefined,
				ctx: {},
				onDiff: () => {}
			})
		).rejects.toThrow('No reader registered');
	});
});

describe('reactive queries — fan-out dedup', () => {
	test('one rerun per (collection, params, ctx) per change batch, regardless of subscriber count', async () => {
		// 50 subscribers with equivalent (collection, params, ctx). One mutation
		// should cause exactly ONE rerun of the query body, not 50 — that's the
		// per-query-diff-sharing fix in syncEngine.reactivePairs.
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerWriter<Message>('messages', {
			delete: () => {},
			insert: (row) => {
				store.messages.set(row.id, row);

				return row;
			},
			update: (row) => {
				store.messages.set(row.id, row);

				return row;
			}
		});

		let rerunCount = 0;
		engine.registerReactive(
			defineReactiveQuery<Message>({
				key: (message) => message.id,
				name: 'allMessages',
				run: ({ db }) => {
					rerunCount += 1;

					return db.all<Message>('messages');
				}
			})
		);
		engine.registerMutation(
			defineMutation({
				handler: (args: Message, _ctx, actions) =>
					actions.insert<Message>('messages', args),
				name: 'addMessage'
			})
		);

		// Subscribe 50 equivalent listeners. Each subscribe does its own initial
		// run, so reset the counter once they're all settled.
		const subs: Array<{ unsubscribe: () => void }> = [];
		for (let index = 0; index < 50; index += 1) {
			subs.push(
				await engine.subscribe<Message>({
					collection: 'allMessages',
					ctx: {},
					onDiff: () => {},
					params: undefined
				})
			);
		}
		rerunCount = 0;

		// One mutation → all 50 subs need a re-run, but the engine should
		// dedupe them all into ONE call to the query body.
		await engine.runMutation(
			'addMessage',
			{ id: 1, room: 'r', text: 'hi' },
			{}
		);
		expect(rerunCount).toBe(1);

		for (const sub of subs) sub.unsubscribe();
	});

	test('different ctxs still produce independent reruns (correctness)', async () => {
		// Two subs with different `ctx` references must NOT share a rerun —
		// the query body could legitimately produce different results per ctx.
		const store = makeStore();
		const engine = createSyncEngine();
		engine.registerReader('messages', {
			all: () => [...store.messages.values()]
		});
		engine.registerWriter<Message>('messages', {
			delete: () => {},
			insert: (row) => {
				store.messages.set(row.id, row);

				return row;
			},
			update: (row) => {
				store.messages.set(row.id, row);

				return row;
			}
		});

		const rerunCalls: unknown[] = [];
		engine.registerReactive(
			defineReactiveQuery<Message, void, { user: string }>({
				key: (message) => message.id,
				name: 'allMessages',
				run: ({ ctx, db }) => {
					rerunCalls.push(ctx);

					return db.all<Message>('messages');
				}
			})
		);
		engine.registerMutation(
			defineMutation({
				handler: (args: Message, _ctx, actions) =>
					actions.insert<Message>('messages', args),
				name: 'addMessage'
			})
		);

		await engine.subscribe<Message>({
			collection: 'allMessages',
			ctx: { user: 'alice' },
			onDiff: () => {},
			params: undefined
		});
		await engine.subscribe<Message>({
			collection: 'allMessages',
			ctx: { user: 'bob' },
			onDiff: () => {},
			params: undefined
		});
		rerunCalls.length = 0;

		await engine.runMutation(
			'addMessage',
			{ id: 1, room: 'r', text: 'hi' },
			{}
		);
		// Two distinct ctxs → two reruns (one per ctx), each with the right ctx.
		expect(rerunCalls).toHaveLength(2);
		expect(rerunCalls).toContainEqual({ user: 'alice' });
		expect(rerunCalls).toContainEqual({ user: 'bob' });
	});
});
