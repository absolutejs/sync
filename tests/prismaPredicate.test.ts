import { describe, expect, test } from 'bun:test';
import {
	matchesWhere,
	UnsupportedFilterError
} from '../src/adapters/prisma/predicate';

const row = {
	id: 5,
	userId: 5,
	status: 'open',
	total: 100,
	title: 'Hello World',
	archivedAt: null as Date | null
};

describe('matchesWhere', () => {
	test('scalar field equality', () => {
		expect(matchesWhere({ status: 'open' }, row)).toBe(true);
		expect(matchesWhere({ status: 'closed' }, row)).toBe(false);
	});

	test('multiple fields are ANDed', () => {
		expect(matchesWhere({ userId: 5, status: 'open' }, row)).toBe(true);
		expect(matchesWhere({ userId: 5, status: 'closed' }, row)).toBe(false);
	});

	test('equals / not (scalar)', () => {
		expect(matchesWhere({ status: { equals: 'open' } }, row)).toBe(true);
		expect(matchesWhere({ status: { not: 'archived' } }, row)).toBe(true);
		expect(matchesWhere({ status: { not: 'open' } }, row)).toBe(false);
	});

	test('not (nested condition)', () => {
		expect(matchesWhere({ total: { not: { gt: 200 } } }, row)).toBe(true);
		expect(matchesWhere({ total: { not: { gt: 50 } } }, row)).toBe(false);
	});

	test('in / notIn', () => {
		expect(matchesWhere({ status: { in: ['open', 'pending'] } }, row)).toBe(
			true
		);
		expect(matchesWhere({ status: { in: ['closed'] } }, row)).toBe(false);
		expect(matchesWhere({ status: { notIn: ['closed'] } }, row)).toBe(true);
		expect(matchesWhere({ status: { notIn: ['open'] } }, row)).toBe(false);
	});

	test('numeric comparisons', () => {
		expect(matchesWhere({ total: { gt: 50 } }, row)).toBe(true);
		expect(matchesWhere({ total: { gte: 100 } }, row)).toBe(true);
		expect(matchesWhere({ total: { lt: 100 } }, row)).toBe(false);
		expect(matchesWhere({ total: { lte: 100, gt: 0 } }, row)).toBe(true);
	});

	test('string contains / startsWith / endsWith', () => {
		expect(matchesWhere({ title: { contains: 'World' } }, row)).toBe(true);
		expect(matchesWhere({ title: { startsWith: 'Hello' } }, row)).toBe(
			true
		);
		expect(matchesWhere({ title: { endsWith: 'World' } }, row)).toBe(true);
		expect(matchesWhere({ title: { contains: 'xyz' } }, row)).toBe(false);
	});

	test('null means IS NULL', () => {
		expect(matchesWhere({ archivedAt: null }, row)).toBe(true);
		expect(matchesWhere({ archivedAt: { not: null } }, row)).toBe(false);
		const archived = { ...row, archivedAt: new Date() };
		expect(matchesWhere({ archivedAt: { not: null } }, archived)).toBe(
			true
		);
	});

	test('AND / OR / NOT', () => {
		expect(
			matchesWhere({ AND: [{ userId: 5 }, { status: 'open' }] }, row)
		).toBe(true);
		expect(
			matchesWhere({ OR: [{ status: 'closed' }, { userId: 5 }] }, row)
		).toBe(true);
		expect(
			matchesWhere({ OR: [{ status: 'closed' }, { userId: 9 }] }, row)
		).toBe(false);
		expect(matchesWhere({ NOT: [{ status: 'closed' }] }, row)).toBe(true);
		expect(matchesWhere({ NOT: [{ status: 'open' }] }, row)).toBe(false);
	});

	test('Date equality compares by time', () => {
		const at = new Date('2026-01-01T00:00:00Z');
		const r = { id: 1, at };
		expect(matchesWhere({ at: new Date('2026-01-01T00:00:00Z') }, r)).toBe(
			true
		);
		expect(matchesWhere({ at: new Date('2026-02-01T00:00:00Z') }, r)).toBe(
			false
		);
	});

	test('an unsupported operator throws UnsupportedFilterError', () => {
		expect(() =>
			matchesWhere({ title: { contains: 'x', mode: 'insensitive' } }, row)
		).toThrow(UnsupportedFilterError);
		expect(() => matchesWhere({ author: { is: { id: 1 } } }, row)).toThrow(
			UnsupportedFilterError
		);
	});
});
