import { describe, expect, test } from 'bun:test';
import { createList, lwwMap, orSet } from '../src/crdt';

describe('OR-Set', () => {
	test('add, has, values, remove', () => {
		let set = orSet.create<string>();
		set = orSet.add(set, 'a', 't1');
		set = orSet.add(set, 'b', 't2');
		expect(orSet.has(set, 'a')).toBe(true);
		expect([...orSet.values(set)].sort()).toEqual(['a', 'b']);
		set = orSet.remove(set, 'a');
		expect(orSet.has(set, 'a')).toBe(false);
		expect(orSet.values(set)).toEqual(['b']);
	});

	test('concurrent add/remove resolves add-wins', () => {
		let a = orSet.create<string>();
		a = orSet.add(a, 'x', 'tag1');
		// b observes x (tag1) and removes it.
		let b = orSet.merge(orSet.create<string>(), a);
		b = orSet.remove(b, 'x');
		// a concurrently re-adds x with a new (unobserved) tag.
		a = orSet.add(a, 'x', 'tag2');

		const merged = orSet.merge(a, b);
		// tag1 was removed, but the concurrent tag2 survives → x is present.
		expect(orSet.has(merged, 'x')).toBe(true);
		expect(orSet.merge(b, a)).toEqual(merged);
	});

	test('merge is idempotent', () => {
		let a = orSet.create<string>();
		a = orSet.add(a, 'a', 't1');
		expect(orSet.merge(a, a)).toEqual(a);
	});
});

describe('LWW-Map', () => {
	test('set, get, delete', () => {
		let map = lwwMap.create<number>();
		map = lwwMap.set(map, 'k', 1, 'a', 1);
		expect(lwwMap.get(map, 'k')).toBe(1);
		map = lwwMap.set(map, 'k', 2, 'a', 2);
		expect(lwwMap.get(map, 'k')).toBe(2);
		map = lwwMap.delete(map, 'k', 'a', 3);
		expect(lwwMap.has(map, 'k')).toBe(false);
		expect(lwwMap.get(map, 'k')).toBeUndefined();
	});

	test('a later concurrent set beats a delete (and vice versa)', () => {
		const deleted = lwwMap.delete(
			lwwMap.set(lwwMap.create<number>(), 'k', 1, 'a', 1),
			'k',
			'a',
			3
		);
		const set4 = lwwMap.set(lwwMap.create<number>(), 'k', 9, 'b', 4);
		expect(lwwMap.get(lwwMap.merge(deleted, set4), 'k')).toBe(9);
		expect(lwwMap.get(lwwMap.merge(set4, deleted), 'k')).toBe(9);
	});

	test('replica id breaks timestamp ties deterministically', () => {
		const x = lwwMap.set(lwwMap.create<string>(), 'k', 'A', 'a', 5);
		const y = lwwMap.set(lwwMap.create<string>(), 'k', 'B', 'b', 5);
		expect(lwwMap.get(lwwMap.merge(x, y), 'k')).toBe('B');
		expect(lwwMap.get(lwwMap.merge(y, x), 'k')).toBe('B');
	});

	test('entries lists only live keys', () => {
		let map = lwwMap.create<number>();
		map = lwwMap.set(map, 'a', 1, 'r', 1);
		map = lwwMap.set(map, 'b', 2, 'r', 1);
		map = lwwMap.delete(map, 'a', 'r', 2);
		expect(lwwMap.entries(map)).toEqual([['b', 2]]);
	});
});

describe('list CRDT', () => {
	test('insert and delete by index', () => {
		const a = createList<number>('a');
		a.insert(0, [1, 2, 3]);
		expect(a.list()).toEqual([1, 2, 3]);
		a.delete(1, 1);
		expect(a.list()).toEqual([1, 3]);
	});

	test('concurrent inserts at different positions converge', () => {
		const a = createList<number>('a');
		a.insert(0, [1, 2, 3]);
		const b = createList<number>('b', a.state());
		a.insert(3, [9]); // append on a
		b.insert(0, [0]); // prepend on b
		a.merge(b.state());
		b.merge(a.state());
		expect(a.list()).toEqual(b.list());
		expect(a.list()).toContain(9);
		expect(a.list()).toContain(0);
	});

	test('delta carries only local ops and merges to converge', () => {
		const a = createList<string>('a');
		a.insert(0, ['x', 'y']);
		const b = createList<string>('b');
		b.merge(a.takeDelta());
		expect(b.list()).toEqual(['x', 'y']);
		expect(a.takeDelta().elements).toHaveLength(0);
	});
});
