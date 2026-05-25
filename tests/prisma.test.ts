import { describe, expect, test } from 'bun:test';
import {
	deriveReadTopics,
	keyTopic,
	tableTopic
} from '../src/adapters/prisma/index';

describe('topic vocabulary', () => {
	test('tableTopic is the model name', () => {
		expect(tableTopic('user')).toBe('user');
	});

	test('keyTopic joins model and key, incl. bigint', () => {
		expect(keyTopic('user', 5)).toBe('user:5');
		expect(keyTopic('session', 'abc')).toBe('session:abc');
		expect(keyTopic('user', 5n)).toBe('user:5');
	});
});

describe('deriveReadTopics', () => {
	test('no filter depends on the whole-model topic', () => {
		expect(deriveReadTopics('user')).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('an empty where also falls back to the model topic', () => {
		expect(deriveReadTopics('user', {})).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('a scalar id equality narrows to a row topic', () => {
		expect(deriveReadTopics('user', { id: 5 })).toEqual({
			topics: ['user:5'],
			rowLevel: true
		});
	});

	test('a string id narrows to a row topic', () => {
		expect(deriveReadTopics('session', { id: 'abc' })).toEqual({
			topics: ['session:abc'],
			rowLevel: true
		});
	});

	test('a bigint id narrows to a row topic', () => {
		expect(deriveReadTopics('user', { id: 5n })).toEqual({
			topics: ['user:5'],
			rowLevel: true
		});
	});

	test('the explicit { equals } form narrows to a row topic', () => {
		expect(deriveReadTopics('user', { id: { equals: 7 } })).toEqual({
			topics: ['user:7'],
			rowLevel: true
		});
	});

	test('a range operator falls back to the model topic', () => {
		expect(deriveReadTopics('user', { id: { gt: 5 } })).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('an in-list falls back to the model topic', () => {
		expect(deriveReadTopics('user', { id: { in: [1, 2, 3] } })).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('a non-key field falls back to the model topic', () => {
		expect(deriveReadTopics('user', { email: 'a@b.com' })).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('multiple fields fall back to the model topic', () => {
		expect(deriveReadTopics('user', { id: 5, name: 'x' })).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});

	test('AND/OR compound filters fall back to the model topic', () => {
		expect(
			deriveReadTopics('user', { AND: [{ id: 5 }, { name: 'x' }] })
		).toEqual({ topics: ['user'], rowLevel: false });
	});

	test('a compound-key where falls back to the model topic', () => {
		expect(
			deriveReadTopics('membership', {
				userId_teamId: { userId: 1, teamId: 2 }
			})
		).toEqual({ topics: ['membership'], rowLevel: false });
	});

	test('an explicit keyField narrows on a non-id field', () => {
		expect(
			deriveReadTopics(
				'user',
				{ email: 'a@b.com' },
				{ keyField: 'email' }
			)
		).toEqual({ topics: ['user:a@b.com'], rowLevel: true });
	});

	test('the default keyField (id) does not match other fields', () => {
		// `email` equality without keyField override stays model-level.
		expect(deriveReadTopics('user', { email: 'a@b.com' })).toEqual({
			topics: ['user'],
			rowLevel: false
		});
	});
});
