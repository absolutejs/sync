import { describe, expect, test } from 'bun:test';
import {
	counter,
	createTextCrdt,
	lww,
	mergeTextState,
	rgaText,
	textOf,
	type TextState
} from '../src/crdt';

describe('PN-counter', () => {
	test('tracks increments and decrements', () => {
		let state = counter.create();
		state = counter.increment(state, 'a', 3);
		state = counter.decrement(state, 'a', 1);
		expect(counter.value(state)).toBe(2);
	});

	test('merges concurrent edits from two replicas', () => {
		let a = counter.create();
		let b = counter.create();
		a = counter.increment(a, 'a', 5);
		b = counter.increment(b, 'b', 2);
		b = counter.decrement(b, 'b', 1);

		// Merge in either order — same value, and it is idempotent.
		const ab = counter.merge(a, b);
		const ba = counter.merge(b, a);
		expect(counter.value(ab)).toBe(6);
		expect(counter.value(ba)).toBe(6);
		expect(counter.value(counter.merge(ab, ab))).toBe(6);
	});
});

describe('LWW register', () => {
	test('the later write wins', () => {
		const a = lww.create('first', 'a', 1);
		const b = lww.set('second', 'b', 2);
		expect(lww.merge(a, b).value).toBe('second');
		expect(lww.merge(b, a).value).toBe('second');
	});

	test('replica id breaks timestamp ties deterministically', () => {
		const a = lww.create('from-a', 'a', 5);
		const b = lww.create('from-b', 'b', 5);
		expect(lww.merge(a, b).value).toBe('from-b');
		expect(lww.merge(b, a).value).toBe('from-b');
	});
});

describe('collaborative text (RGA)', () => {
	test('local insert and delete read back as plain text', () => {
		const doc = createTextCrdt('a');
		doc.insert(0, 'hello world');
		doc.delete(5, 6); // drop " world"
		expect(doc.text()).toBe('hello');
	});

	test('concurrent inserts from two replicas both survive and converge', () => {
		const a = createTextCrdt('a');
		const b = createTextCrdt('b');
		a.insert(0, 'hello');
		b.merge(a.state()); // b starts from the same base

		// Both type at the same spot before syncing.
		a.insert(5, ' from A');
		b.insert(5, ' from B');

		// Exchange state in opposite orders.
		const aFinal = createTextCrdt('a', a.state());
		aFinal.merge(b.state());
		const bFinal = createTextCrdt('b', b.state());
		bFinal.merge(a.state());

		// Neither edit is lost and both replicas agree on the result.
		expect(aFinal.text()).toBe(bFinal.text());
		expect(aFinal.text()).toContain('from A');
		expect(aFinal.text()).toContain('from B');
	});

	test('a delete on one replica survives a merge from the other', () => {
		const a = createTextCrdt('a');
		a.insert(0, 'abcdef');
		const b = createTextCrdt('b', a.state());

		a.delete(0, 3); // a removes "abc"
		b.insert(6, 'ghi'); // b appends concurrently

		a.merge(b.state());
		b.merge(a.state());
		expect(a.text()).toBe(b.text());
		expect(a.text()).toBe('defghi');
	});

	test('merge is commutative, associative, and idempotent over state', () => {
		const a = createTextCrdt('a');
		a.insert(0, 'one');
		const b = createTextCrdt('b');
		b.insert(0, 'two');
		const c = createTextCrdt('c');
		c.insert(0, 'three');

		const left = mergeTextState(
			mergeTextState(a.state(), b.state()),
			c.state()
		);
		const right = mergeTextState(
			a.state(),
			mergeTextState(b.state(), c.state())
		);
		expect(textOf(left)).toBe(textOf(right));
		// Idempotent: re-merging a state changes nothing.
		expect(textOf(mergeTextState(left, a.state()))).toBe(textOf(left));
	});

	test('setText reconciles via a minimal diff that merges with remote edits', () => {
		// One replica edits the start, the other the end, each via setText only.
		const a = createTextCrdt('a');
		a.insert(0, 'the quick fox');
		const shared: TextState = a.state();

		const b = createTextCrdt('b', shared);
		a.setText('THE quick fox'); // a edits the head
		b.setText('the quick fox!'); // b edits the tail

		a.merge(b.state());
		b.merge(a.state());
		expect(a.text()).toBe(b.text());
		expect(a.text()).toBe('THE quick fox!');
	});
});

describe('rgaText adapter', () => {
	test('conforms to the TextCrdtAdapter contract', () => {
		expect(rgaText.textOf(rgaText.empty())).toBe('');
		const doc = rgaText.create('a');
		doc.setText('hi');
		const merged = rgaText.merge(rgaText.empty(), doc.state());
		expect(rgaText.textOf(merged)).toBe('hi');
	});
});
