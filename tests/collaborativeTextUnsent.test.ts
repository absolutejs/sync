import { afterEach, describe, expect, test } from 'bun:test';
import { createCollaborativeText } from '../src/client/collaborativeText';
import { createTextCrdt } from '../src/crdt';

class FakeWebSocket {
	static instances: FakeWebSocket[] = [];

	sent: string[] = [];
	onopen: ((event: unknown) => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onclose: ((event: unknown) => void) | null = null;

	constructor() {
		FakeWebSocket.instances.push(this);
	}

	send(data: string) {
		this.sent.push(data);
	}
	close() {}
	emit(frame: unknown) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}
	mutations() {
		return this.sent
			.map((raw) => JSON.parse(raw))
			.filter((frame) => frame.type === 'mutate');
	}
}

const socket = () => {
	const found = FakeWebSocket.instances.at(-1);
	if (!found) throw new Error('expected a FakeWebSocket');

	return found;
};

const open = () => {
	const text = createCollaborativeText({
		collection: 'docs',
		field: 'body',
		id: 'doc-1',
		url: 'ws://localhost/sync/ws',
		webSocketImpl: FakeWebSocket as unknown as typeof WebSocket
	});
	socket().onopen?.({});

	return text;
};

const hydrate = () =>
	socket().emit({
		id: 's',
		rows: [{ body: createTextCrdt('server').state(), id: 'doc-1' }],
		type: 'snapshot'
	});

afterEach(() => {
	FakeWebSocket.instances = [];
});

describe('collaborative text unsent edits', () => {
	test('an edit is unsent until the server acknowledges it', async () => {
		const text = open();
		hydrate();
		text.setText('hello');
		expect(text.get().unsent).toBe(1);
		const [mutation] = socket().mutations();
		socket().emit({ mutationId: mutation.mutationId, type: 'ack' });
		await Bun.sleep(0);
		expect(text.get().unsent).toBe(0);
		expect(text.get().refused).toBeNull();
		text.close();
	});

	test('text held before hydration counts once, however often it changes', async () => {
		const text = open();
		text.setText('a');
		text.setText('ab');
		expect(text.get().unsent).toBe(1);
		hydrate();
		expect(text.get().unsent).toBe(1);
		const mutations = socket().mutations();
		expect(mutations).toHaveLength(1);
		socket().emit({ mutationId: mutations[0].mutationId, type: 'ack' });
		await Bun.sleep(0);
		expect(text.get().unsent).toBe(0);
		text.close();
	});

	test('a refused edit says why until a later edit is confirmed', async () => {
		const text = open();
		hydrate();
		text.setText('one');
		const [first] = socket().mutations();
		socket().emit({
			message: 'read only',
			mutationId: first.mutationId,
			type: 'reject'
		});
		await Bun.sleep(0);
		expect(text.get()).toMatchObject({ refused: 'read only', unsent: 0 });
		text.setText('two');
		const second = socket().mutations().at(-1);
		socket().emit({ mutationId: second.mutationId, type: 'ack' });
		await Bun.sleep(0);
		expect(text.get()).toMatchObject({ refused: null, unsent: 0 });
		text.close();
	});
});
